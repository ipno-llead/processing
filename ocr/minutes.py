from pathlib import Path
import tempfile
import json
from typing import List

from pdf2image import convert_from_path
import pytesseract
from tqdm import tqdm
import deba
import pandas as pd


def only_pdf(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df.filetype == "pdf"].reset_index(drop=True)


def process_pdf(df: pd.DataFrame) -> pd.DataFrame:
    texts: List[List[str]] = []
    for _, row in tqdm(df.iterrows(), desc="OCR pdf", total=df.shape[0], position=0):
        pdfpath = "raw_minutes/%s" % row.filepath
        ocr_cachefile: Path = deba.data("ocr_cache/minutes/") / (row.filesha1 + ".json")
        ocr_cachefile.parent.mkdir(parents=True, exist_ok=True)

        if ocr_cachefile.is_file():
            with open(ocr_cachefile, "r") as f:
                texts.append(json.load(f))
            continue

        pages = []
        with tempfile.TemporaryDirectory() as path:
            images = convert_from_path(deba.data(pdfpath), dpi=300, output_folder=path)
            for image in tqdm(
                images, desc="processing %s" % pdfpath, position=1, leave=False
            ):
                txt = pytesseract.image_to_string(image)
                pages.append(txt)

        with open(ocr_cachefile, "w") as f:
            json.dump(pages, f)
        texts.append(pages)

    df.loc[:, "text"] = pd.Series(texts, index=df.index)
    df = df.explode("text").reset_index(drop=True)
    print(df)
    print(df.groupby("fileid").cumcount())
    df.loc[:, "pageno"] = df.groupby("fileid").cumcount() + 1
    print(df.loc[:, ["fileid", "pageno"]])
    return df
    # df.loc[:, "pageno"] = df.loc[:, "text"].map(lambda x: list(range(1, len(x) + 1)))
    # return df.explode(["text", "pageno"])


def process_all_pdfs() -> pd.DataFrame:
    return (
        pd.read_csv(deba.data("meta/minutes_files.csv"))
        .pipe(only_pdf)
        .pipe(process_pdf)
    )


if __name__ == "__main__":
    df = process_all_pdfs()
    df.to_csv(deba.data("ocr/minutes_pdfs.csv"), index=False)
