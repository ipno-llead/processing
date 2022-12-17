import os
from pathlib import Path
import pathlib
import tempfile
import json
from typing import List, Union
import subprocess
from distutils.spawn import find_executable

from pdf2image import convert_from_path
import pytesseract
from tqdm import tqdm
import deba
import pandas as pd


def _root_dir() -> pathlib.Path:
    return pathlib.Path(os.path.realpath(__file__)).parent.parent


def process_pdf(
    df: pd.DataFrame,
    dir_name: str,
    output_images_to_tempdir: bool = True,
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
        output_images_to_tempdir (bool):
            output PDF images to a tempdir. If set to False, keep images
            in memory instead.
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
    for _, row in tqdm(df.iterrows(), desc="OCR pdf", total=df.shape[0], position=0):
        pdfpath = "%s/%s" % (dir_name, row.filepath)

        if not ignore_cache:
            ocr_cachefile: Path = cache_root_dir / (row.filesha1 + ".json")
            ocr_cachefile.parent.mkdir(parents=True, exist_ok=True)

            if ocr_cachefile.is_file():
                with open(ocr_cachefile, "r") as f:
                    texts.append(json.load(f))
                continue

        pages = []
        kwargs = {"dpi": 300}
        if output_images_to_tempdir:
            tempdir = tempfile.TemporaryDirectory()
            kwargs["output_folder"] = tempdir.name

        images = convert_from_path(pdfpath, **kwargs)
        for image in tqdm(
            images,
            desc="processing %s" % os.path.relpath(pdfpath, str(root_dir)),
            position=1,
            leave=False,
        ):
            txt = pytesseract.image_to_string(image)
            pages.append(txt)

        if output_images_to_tempdir:
            tempdir.cleanup()

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


def fetch_ocr_results(prefix: Union[str, None]) -> pd.DataFrame:
    """Fetches OCR results from k8s-ocr-jobqueue

    Args:
        prefix (str):
            fetch ocr results for files with this prefix

    Returns:
        a dataframe with the following columns:
            filepath:
                the original file path
            pageno:
                page number
            result:
                OCR result as produced by DocTR
    """
    cache_dir = pathlib.Path(__file__).parent.parent / "data/ocr_results"
    bucket = "k8s-ocr-jobqueue-results"
    src = "gs://%s" % bucket
    dst = str(cache_dir.absolute())
    if prefix is not None:
        src = "%s/%s" % (src, prefix)
        dst = os.path.abspath("%s/%s" % (dst, prefix))
    os.makedirs(dst, exist_ok=True)

    gsutil = find_executable(
        "gsutil", str(pathlib.Path.home() / "google-cloud-sdk/bin")
    )
    if gsutil is None:
        raise Exception("couldnt find gsutil in ~/google-cloud-sdk/bin")

    subprocess.run(
        [
            gsutil,
            "-m",
            "rsync",
            "-i",
            "-J",
            "-r",
            src,
            dst,
        ],
        capture_output=True,
        check=True,
    )

    records = []
    for root, _, files in tqdm(
        list(os.walk(dst)), desc="loading files from %s" % json.dumps(dst), leave=False
    ):
        filepath = os.path.relpath(os.path.abspath(root), str(cache_dir))
        for file in files:
            pageno, _ = os.path.splitext(file)
            pageno = int(pageno)

            filename = pathlib.Path(root) / file
            with filename.open("r") as f:
                result = json.load(f)

            records.append({"filepath": filepath, "pageno": pageno, "result": result})

    return pd.DataFrame.from_records(records).sort_values(["filepath", "pageno"])


def fetch_ocr_text(prefix: Union[str, None]) -> pd.DataFrame:
    """Fetches OCR text from k8s-ocr-jobqueue

    Args:
        prefix (str):
            fetch ocr results for files with this prefix

    Returns:
        a dataframe with the following columns:
            filepath:
                the original file path
            pageno:
                page number
            text:
                the combined text of the page
    """
    df = fetch_ocr_results(prefix)
    df.loc[:, "text"] = df.result.map(
        lambda pg: "\n".join(
            [
                " ".join([word["value"] for word in line["words"]])
                for blk in pg["blocks"]
                for line in blk["lines"]
            ]
        )
    )
    return df.drop(columns="result")
