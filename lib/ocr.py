import os
import timeit
from pathlib import Path
from contextlib import contextmanager
from datetime import timedelta
import pathlib
import tempfile
import json
import subprocess
from distutils.spawn import find_executable

import deba
import pandas as pd
from google.cloud.storage import Client
from google.auth import default

from lib import queue_pdf_for_ocr

SOURCE_BUCKET = "k8s-ocr-jobqueue-pdfs"
RESULT_BUCKET = "k8s-ocr-jobqueue-results"
GCLOUD_PROJECT = "excellent-zoo-300106"


def _run_gsutil(*args):
    paths = [
        str(pathlib.Path.home() / "google-cloud-sdk/bin"),
        "/usr/local/gcloud/google-cloud-sdk/bin",
    ]
    gsutil = find_executable("gsutil", *paths)
    if gsutil is None:
        raise Exception("couldnt find gsutil in %s" % (":".join(paths)))

    try:
        subprocess.run(
            [
                gsutil,
                *args,
            ],
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f'command {" ".join([gsutil, *args])} failed\n\tstdout:{e.stdout.decode("utf8")}\n\tstderr:{e.stderr.decode("utf8")}'
        )


@contextmanager
def _time_exc(desc: str):
    start = timeit.default_timer()

    yield None

    stop = timeit.default_timer()

    print(f"finished {desc} ({timedelta(seconds=stop-start)})")


def _rsync_ocr_results(df: pd.DataFrame) -> pd.DataFrame:
    """Rsync ocr results from gcs bucket and report for filesha1 in df"""
    ocr_dir = deba.data("ocr_results")
    ocr_dir.mkdir(parents=True, exist_ok=True)

    with _time_exc("downloading ocr result"):
        _run_gsutil(
            "-m",
            "rsync",
            "-i",
            "-J",
            "-r",
            "gs://%s/ocr/" % RESULT_BUCKET,
            str(ocr_dir.absolute()),
        )

    records = []

    for filesha1 in df.filesha1.values:
        filedir = ocr_dir / filesha1[:2] / (filesha1[2:] + ".pdf")
        if not filedir.exists():
            records.append(
                {
                    "filesha1": filesha1,
                    "pageno": pd.NA,
                    "ocr_status": "file not found",
                }
            )
            continue
        try:
            with (filedir / "count").open() as f:
                count = int(f.read())
        except FileNotFoundError:
            records.append(
                {
                    "filesha1": filesha1,
                    "pageno": pd.NA,
                    "ocr_status": "count file not found",
                }
            )
            continue
        for pageno in range(1, count + 1):
            try:
                with (filedir / ("%03d.json" % pageno)).open() as f:
                    data = json.loads(f.read())
                text = "\n".join(
                    [
                        " ".join([word["value"] for word in line["words"]])
                        for blk in data["blocks"]
                        for line in blk["lines"]
                    ]
                )
                records.append(
                    {
                        "filesha1": filesha1,
                        "pageno": pageno,
                        "text": text,
                        "ocr_status": "success",
                    }
                )
            except FileNotFoundError:
                records.append(
                    {
                        "filesha1": filesha1,
                        "pageno": pageno,
                        "ocr_status": "page not found",
                    }
                )
                continue

    files = pd.DataFrame.from_records(records)
    return files.merge(df, how="outer", on="filesha1")


def _check_for_processing_files(df: pd.DataFrame) -> pd.DataFrame:
    """finds files that are not yet present in the queue bucket and mark them as 'queued'"""
    credentials, _ = default()
    client = Client(GCLOUD_PROJECT, credentials=credentials)
    sha1s = set()
    for filesha1 in (
        df.loc[df.ocr_status == "queueing", "filesha1"].drop_duplicates().values
    ):
        blobs = client.list_blobs(
            SOURCE_BUCKET, 1, prefix=f"ocr/{filesha1[:2]}/{filesha1[2:]}"
        )
        if len(list(blobs)) > 0:
            sha1s.add(filesha1)
    df.loc[df.filesha1.isin(sha1s), "ocr_status"] = "processing"
    return df


def _enqueue_pdf(
    df: pd.DataFrame,
    dir_name: str,
    requeue: bool = False,
    requeue_unsuccessful: bool = False,
) -> pd.DataFrame:
    if not requeue:
        if not requeue_unsuccessful:
            df = _check_for_processing_files(df)
        if len(df.loc[df.ocr_status == "queueing"]) == 0:
            return df
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdir = Path(tmpdirname) / "ocr"
        if requeue:
            files_to_process = df
        else:
            files_to_process = df.loc[df.ocr_status == "queueing"]
        for idx, row in files_to_process.iterrows():
            file: Path = tmpdir / row.filesha1[:2] / (row.filesha1[2:] + ".pdf")
            if file.exists():
                continue
            file.parent.mkdir(parents=True, exist_ok=True)
            file.symlink_to(
                (Path(dir_name) / row.filepath).resolve(), target_is_directory=False
            )
        queue_pdf_for_ocr.run([str(tmpdir)])
    df.loc[df.ocr_status == "queueing", "ocr_status"] = "queued"
    return df


def _arg_from_env_var(arg: bool, env_name: str) -> bool:
    if not arg and os.getenv(f"OCR_{env_name}", "").lower() == "true":
        return True
    return arg


def process_pdf(
    df: pd.DataFrame,
    dir_name: str,
    requeue: bool = False,
    requeue_unsuccessful: bool = False,
    ensure_complete: bool = False,
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
    - ocr_status: ocr status of each page. Most "not found" statuses simply
      mean that the pdf page isn't completely processed and merely need
      more time. It could be one of these values:
        + "success":
            page was found and read successfully
        + "queued":
            pdf file wasn't found in the results bucket and was queued
            for processing
        + "count file not found":
            pdf file page count wasn't found in the results bucket
        + "page not found":
            a pdf page wasn't found in the results bucket

    Args:
        df (pd.DataFrame):
            a frame containing PDFs metadata
        dir_name (str):
            the parent directory of all PDF files
        requeue (bool):
            requeue pdf pages for reprocessing even if those files
            are already processed. If environment variable "OCR_REQUEUE"
            is set to "true", it also takes effect.
        requeue_unsuccessful (bool):
            requeue non-"success" pdf files for processing. This is
            also set to True if environment variable
            "OCR_REQUEUE_UNSUCCESSFUL" is set to "true".
        ensure_complete (bool):
            raise error if there are non-"success" rows. This is also
            set to True if environment variable
            "OCR_ENSURE_COMPLETE" is set to "true".

    Returns:
        exploded frame with 1 row per page
    """
    requeue = _arg_from_env_var(requeue, "REQUEUE")
    requeue_unsuccessful = _arg_from_env_var(
        requeue_unsuccessful, "REQUEUE_UNSUCCESSFUL"
    )
    ensure_complete = _arg_from_env_var(ensure_complete, "ENSURE_COMPLETE")
    df = _rsync_ocr_results(df)
    if requeue_unsuccessful:
        df.loc[df.ocr_status != "success", "ocr_status"] = "queueing"
    else:
        df.loc[df.ocr_status == "file not found", "ocr_status"] = "queueing"
    df = _enqueue_pdf(df, dir_name, requeue, requeue_unsuccessful)
    df = df.sort_values(["filesha1", "pageno"]).reset_index(drop=True)
    print(
        df.groupby("ocr_status")["ocr_status"].count(),
    )
    if ensure_complete:
        n = df.loc[df.ocr_status != "success"].shape[0]
        if n > 0:
            raise ValueError(f'there are {n} rows with non "success" ocr_status')
    return df
