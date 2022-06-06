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
    texts: List[List[str]] = []
    root_dir = _root_dir()
    cache_root_dir = deba.data("ocr_cache") / os.path.relpath(dir_name, str(root_dir))
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
            images = convert_from_path(pdfpath, dpi=300, output_folder=path)
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

    df.loc[:, "text"] = pd.Series(texts, index=df.index)
    df = df.explode("text").reset_index(drop=True)
    df.loc[:, "pageno"] = df.groupby("fileid").cumcount() + 1
    return df
