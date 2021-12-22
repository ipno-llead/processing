#!/usr/bin/env python

import argparse
import csv
import re
import atexit
import os
import pathlib
import math
from urllib.parse import urlparse, unquote

import requests
from tqdm import tqdm
from PyPDF2 import PdfFileReader, PdfFileWriter
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv

selection_marks_regex = re.compile(r"\s+:(un)?selected:")


def ensure_dir(build_dir, name):
    d = build_dir / name
    if not d.exists():
        os.makedirs(d)
    return d


def filename_from_url(url):
    o = urlparse(url)
    filename = unquote(o.path).split("/")[-1].lower()
    return re.sub(r"\s+", "_", filename)


class PDFBatchReader(object):
    def __init__(self, build_dir, doc_url, batch_size=10, end_page=math.inf):
        self.build_dir = build_dir
        self.doc_url = doc_url
        self.batch_size = batch_size
        self.end_page = end_page
        self._current_batch_idx = None
        self.filename = filename_from_url(self.doc_url)
        if not self.batch_dir.exists():
            os.makedirs(self.batch_dir)
            self.download_doc(self.doc_url)
            self.split_pdf_into_batches()
        with open(self.batch_dir / "pageCount", "r") as cf:
            page_count = int(cf.read().strip())
        self.pbar = tqdm(
            total=page_count,
            desc="analyzing PDF pages {batch_size=%d}" % (self.batch_size),
        )

        atexit.register(self.cleanup)

    @property
    def progress_path(self):
        return self.build_dir / "pdf_batch_reader/progress.txt"

    @property
    def current_batch_idx(self):
        if self._current_batch_idx is None:
            prog_path = self.progress_path
            if prog_path.exists():
                with open(prog_path, "r") as f:
                    self._current_batch_idx = int(f.read().strip())
                    print("resume at batch %d" % self._current_batch_idx)
            else:
                self._current_batch_idx = 0
        return self._current_batch_idx

    @current_batch_idx.setter
    def current_batch_idx(self, value):
        self._current_batch_idx = value

    def download_doc(self, doc_url):
        docs_dir = ensure_dir(self.build_dir, "pdf_batch_reader/docs")
        filename = filename_from_url(doc_url)
        fp = docs_dir / filename
        if not fp.exists():
            r = requests.get(doc_url)
            progress_bar = tqdm(
                desc="downloading %s" % filename,
                total=int(r.headers.get("content-length", 0)),
                unit="iB",
                unit_scale=True,
            )
            with open(fp, "wb") as f:
                for data in r.iter_content(1024):
                    progress_bar.update(len(data))
                    f.write(data)
            progress_bar.close()

    @property
    def batch_dir(self):
        return self.build_dir / ("pdf_batch_reader/batches/%s" % self.filename)

    def split_pdf_into_batches(self):
        with open(self.build_dir / "pdf_batch_reader/docs" / self.filename, "rb") as f:
            reader = PdfFileReader(f)
            total = reader.getNumPages()
            with open(self.batch_dir / "pageCount", "w") as cf:
                cf.write("%d" % total)
            print(
                "splitting pdf (%d pages) into %d batches"
                % (total, int(math.ceil(float(total) / self.batch_size)))
            )
            for batch_idx, start_page in enumerate(range(0, total, self.batch_size)):
                writer = PdfFileWriter()
                for page in range(start_page, start_page + self.batch_size):
                    if page >= total or page >= self.end_page:
                        break
                    writer.addPage(reader.getPage(page))
                batch_name = "%03d.pdf" % batch_idx
                with open(self.batch_dir / batch_name, "wb") as wf:
                    writer.write(wf)

    def pdf_batches(self):
        self.pbar.update(self.current_batch_idx * self.batch_size)
        for batch_file in sorted(self.batch_dir.iterdir())[self.current_batch_idx :]:
            if batch_file.name == "pageCount":
                continue
            with open(batch_file, "rb") as f:
                yield (self.current_batch_idx + 1, f)
                self.current_batch_idx += 1
                self.pbar.update(self.batch_size)
        self.pbar.close()
        self.current_batch_idx = None

    def cleanup(self):
        if self._current_batch_idx is not None and self._current_batch_idx > 0:
            with open(self.progress_path, "w") as f:
                f.write("%d" % (self._current_batch_idx))
        elif self.progress_path.exists():
            self.progress_path.unlink()


class TableExtractor(object):
    def __init__(self, build_dir, csv_path, trim_selection_marks=False):
        self.build_dir = build_dir
        self.csv_path = csv_path
        self.trim_selection_marks = trim_selection_marks
        self._table_idx = None
        self._file_handles = dict()
        self._writers = dict()

    def get_writer(self, name, fieldnames):
        if name not in self._writers:
            fp = pathlib.Path("%s_%s.csv" % (self.csv_path.strip(), name))
            if not fp.parent.exists():
                os.makedirs(fp.parent)
            exists = fp.exists()
            self._file_handles[name] = open(fp, "w")
            self._writers[name] = csv.DictWriter(
                self._file_handles[name], fieldnames=fieldnames
            )
            if not exists:
                self._writers[name].writeheader()
        return self._writers[name]

    def save_csvs(self):
        for f in self._file_handles.values():
            f.close()

    def extract_tables_from_extracted_documents(self, batch_idx, result):
        for idx, doc in enumerate(result.documents):
            for field_name, field in doc.fields.items():
                if (
                    field.value_type != "list"
                    or field.value[0].value_type != "dictionary"
                ):
                    print(
                        "field %s in document %d is not a table: %s"
                        % (field_name, idx, field)
                    )
                    continue
                fieldnames = list(field.value[0].value.keys()) + [
                    "batch_index",
                    "table_index",
                ]
                writer = self.get_writer(field_name, fieldnames)
                writer.writerows(
                    [
                        dict(
                            [(k, v.content) for k, v in obj.value.items()]
                            + [("batch_index", batch_idx), ("table_index", idx)]
                        )
                        for obj in field.value
                    ]
                )

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.save_csvs()


def extract_tables(csv_path, start_idx, result, trim_selection_marks=False):
    for idx, tbl in enumerate(result.tables):
        tbl_idx = start_idx + idx
        fp = pathlib.Path("%s_%03d.csv" % (csv_path.strip(), tbl_idx))
        if not fp.parent.exists():
            os.makedirs(fp.parent)
        rows = [[""] * tbl.column_count for _ in range(tbl.row_count)]
        for cell in tbl.cells:
            content = cell.content.strip()
            if trim_selection_marks:
                cell.content = selection_marks_regex.sub("", content)
            rows[cell.row_index][cell.column_index] = cell.content
        with open(fp, "w") as f:
            w = csv.writer(f)
            w.writerows(rows)
    return start_idx + len(result.tables)


if __name__ == "__main__":
    load_dotenv()
    fr_endpoint = os.getenv("FORM_RECOGNIZER_ENDPOINT")
    fr_key = os.getenv("FORM_RECOGNIZER_KEY")

    parser = argparse.ArgumentParser(
        description="Extract tables from a document using Azure Form Recognizer and save as CSV files."
    )
    parser.add_argument(
        "doc_url",
        type=str,
        metavar="DOC_URL",
        help="URL of the PDF document to extract tables from",
    )
    parser.add_argument(
        "csv_path",
        type=str,
        metavar="CSV_PATH",
        help=(
            'Path pattern that will be appended with table index and ".csv" to generate the CSV save paths. '
            'I.e., if DOC_URL contains two tables and CSV_PATH is "/data/my_doc" then the results '
            'will be saved to "/data/my_doc_00.csv" and "/data/my_doc_01.csv"'
        ),
    )
    parser.add_argument(
        "--trim-selection-marks",
        action="store_true",
        help='remove ":selected:" and ":unselected:" sequences from cell values',
    )
    parser.add_argument(
        "--model-id", type=str, default="prebuilt-layout", help="ID of model to use"
    )
    parser.add_argument(
        "--build-dir",
        type=pathlib.Path,
        metavar="BUILD_DIR",
        default=pathlib.Path.cwd() / "build",
        help=(
            "Build directory which will contain temporary files that is necessary for document extraction. "
            "This script will create and store files under BUILD_DIR/docs. Defaults to {{cwd}}/build."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        metavar="BATCH_SIZE",
        default=10,
        help="PDF batch size (number of pages) that get sent to FormRecognizer in a single request.",
    )
    parser.add_argument(
        "-e",
        "--end-page",
        type=int,
        metavar="END_PAGE",
        default=math.inf,
        help="only analyze until this page (non-inclusive)",
    )
    args = parser.parse_args()

    document_analysis_client = DocumentAnalysisClient(
        endpoint=fr_endpoint, credential=AzureKeyCredential(fr_key)
    )

    with TableExtractor(args.build_dir, args.csv_path, args.trim_selection_marks) as ex:
        reader = PDFBatchReader(
            args.build_dir, args.doc_url, args.batch_size, args.end_page
        )
        for batch_idx, batch_file in reader.pdf_batches():
            poller = document_analysis_client.begin_analyze_document(
                model=args.model_id, document=batch_file
            )
            result = poller.result()
            ex.extract_tables_from_extracted_documents(batch_idx, result)
