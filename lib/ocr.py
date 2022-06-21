import os
from pathlib import Path
import pathlib
import tempfile
import json
from typing import List

from pdf2image import convert_from_path
import pytesseract
from tqdm import tqdm
import deba
import pandas as pd


def _root_dir() -> pathlib.Path:
    return pathlib.Path(os.path.realpath(__file__)).parent.parent


def process_pdf(df: pd.DataFrame, dir_name: str) -> pd.DataFrame:
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

    Returns:
        exploded frame with 1 row per page
    """
    texts: List[List[str]] = []
    root_dir = _root_dir()
    cache_root_dir = deba.data("ocr_cache") / os.path.relpath(dir_name, str(root_dir))
    new_file_detected = False
    for _, row in tqdm(df.iterrows(), desc="OCR pdf", total=df.shape[0], position=0):
        pdfpath = "%s/%s" % (dir_name, row.filepath)
        ocr_cachefile: Path = cache_root_dir / (row.filesha1 + ".json")
        ocr_cachefile.parent.mkdir(parents=True, exist_ok=True)

        if ocr_cachefile.is_file():
            with open(ocr_cachefile, "r") as f:
                texts.append(json.load(f))
            continue

        pages = []
        with tempfile.TemporaryDirectory() as path:
            images = convert_from_path(pdfpath, dpi=500, output_folder=path)
            for image in tqdm(
                images,
                desc="processing %s" % os.path.relpath(pdfpath, str(root_dir)),
                position=1,
                leave=False,
            ):
                txt = pytesseract.image_to_string(image)
                pages.append(txt)

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
                    "    scripts/dvc_add.sh",
                    "    dvc push",
                    "",
                ]
            )
        )

    df.loc[:, "text"] = pd.Series(texts, index=df.index)
    df = df.explode("text").reset_index(drop=True)
    df.loc[:, "pageno"] = df.groupby("fileid").cumcount() + 1
    return df
