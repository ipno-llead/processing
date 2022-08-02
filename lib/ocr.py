import os
from pathlib import Path
import pathlib
import json
from typing import List

os.environ["USE_TORCH"] = "1"
from tqdm import tqdm
import deba
import pandas as pd
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

import matplotlib.pyplot as plt

from doctr.io import DocumentFile
from doctr.models import ocr_predictor


def _root_dir() -> pathlib.Path:
    return pathlib.Path(os.path.realpath(__file__)).parent.parent


def detect_non_english(df: pd.DataFrame) -> pd.DataFrame:
    """Detect documents whose OCR text is not english

    This is mainly useful to detect PDF with incorrect rotation

    Args:
        df (pd.DataFrame)
            the frame already piped through lib.ocr.process_pdf

    Returns:
        unmodified frame
    """
    fileset = set()

    def detect_lang(row: pd.Series):
        try:
            lang = detect(row.text)
        except LangDetectException:
            return row
        if lang != "en" and row.fileid not in fileset:
            fileset.add(row.fileid)
            print("detected lang %s for %s" % (lang, row.filepath))
        return row

    df.loc[df.text != "", :].apply(detect_lang, axis=1)


def process_pdf(
    df: pd.DataFrame,
    dir_name: str,
    ignore_cache: bool = False,
) -> pd.DataFrame:
    """Reads and returns PDF content as text

    This function expect a dataframe, each row containing metadata
    about a PDF file. The following columns are expected:

    - filepath: path to the file relative to dir_name
    - filesha1: SHA1 hash of the file content
    - fileid: unique id of the file, usually derived from filesha1

    When the processing is finished, the dataframe will be exploded
    into 1 row per page. The following columns will be added:

    - text: the text content of the pdf page
    - pageno: the page number

    This function keeps OCRed texts in the folder data/ocr_cache to
    avoid reprocessing files that it has already processed. After
    adding new files, please run the following commands to share the
    OCR cache with others:

        scripts/dvc_add.sh
        dvc push

    Args:
        df (pd.DataFrame):
            a frame containing PDFs metadata
        dir_name (str):
            the parent directory of all PDF files
        ignore_cache (bool):
            if set to True, don't read from or write to OCR cache.
            Useful for testing new OCR configs.

    Returns:
        exploded frame with 1 row per page
    """
    texts: List[List[str]] = []
    root_dir = _root_dir()
    cache_root_dir = deba.data("ocr_cache") / os.path.relpath(dir_name, str(root_dir))
    new_file_detected = False
    predictor = ocr_predictor(pretrained=True)
    for _, row in tqdm(df.iterrows(), desc="OCR pdf", total=df.shape[0], position=0):
        pdfpath = "%s/%s" % (dir_name, row.filepath)
        doc = DocumentFile.from_pdf(pdfpath)
        result = predictor(doc)

        if not ignore_cache:
            ocr_cachefile: Path = cache_root_dir / (row.filesha1 + ".json")
            ocr_cachefile.parent.mkdir(parents=True, exist_ok=True)

            if ocr_cachefile.is_file():
                with open(ocr_cachefile, "r") as f:
                    texts.append(json.load(f))
                continue

        doc = DocumentFile.from_pdf(pdfpath)
        result = predictor(doc)

        pages = [
            "\n".join(
                [
                    " ".join([word.value for word in ln.words])
                    for blk in pg.blocks
                    for ln in blk.lines
                ]
            )
            for pg in result.pages
        ]

        if not ignore_cache:
            with open(ocr_cachefile, "w") as f:
                json.dump(pages, f)

        texts.append(pages)
        new_file_detected = True

    if new_file_detected:
        print(
            "\n".join(
                [
                    "New OCR content detected, please run the following commands to share the OCR cache with others:",
                    "",
                    "    dvc add --file ocr_cache.dvc data/ocr_cache",
                    "    dvc push",
                    "",
                ]
            )
        )

    df.loc[:, "text"] = pd.Series(texts, index=df.index)
    df = df.explode("text").reset_index(drop=True)
    df.loc[:, "pageno"] = df.groupby("fileid").cumcount() + 1
    return df
