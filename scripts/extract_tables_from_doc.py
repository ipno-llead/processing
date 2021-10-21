#!/usr/bin/env python

import argparse
import csv
import re
import os
import pathlib
import math
from urllib.parse import urlparse, unquote

from tqdm import tqdm
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from dotenv import load_dotenv
import requests
from PyPDF2 import PdfFileReader, PdfFileWriter

selection_marks_regex = re.compile(r"\s+:(un)?selected:")


def ensure_docs_dir(build_dir):
    d = build_dir / 'docs'
    if not d.exists():
        os.makedirs(d)
    return d


def get_batches_dir(build_dir, filename):
    return build_dir / 'docs/batches' / filename


def filename_from_url(url):
    o = urlparse(url)
    filename = unquote(o.path).split("/")[-1].lower()
    return re.sub(r'\s+', '_', filename)


def download_doc(build_dir, doc_url):
    docs_dir = ensure_docs_dir(build_dir)
    filename = filename_from_url(doc_url)
    fp = docs_dir / filename
    if not fp.exists():
        r = requests.get(doc_url)
        progress_bar = tqdm(
            desc='downloading %s' % filename,
            total=int(r.headers.get('content-length', 0)), unit='iB', unit_scale=True)
        with open(fp, 'wb') as f:
            for data in r.iter_content(1024):
                progress_bar.update(len(data))
                f.write(data)
        progress_bar.close()


def split_pdf_into_batches(build_dir, filename, batch_size=10):
    batches_dir = get_batches_dir(build_dir, filename)
    if not batches_dir.exists():
        os.makedirs(batches_dir)
    with open(build_dir / 'docs' / filename, 'rb') as f:
        reader = PdfFileReader(f)
        total = reader.getNumPages()
        with open(batches_dir / 'pageCount', 'w') as cf:
            cf.write('%d' % total)
        print("splitting pdf (%d pages) into %d batches" % (total, int(math.ceil(float(total) / batch_size))))
        for batch_idx, start_page in enumerate(range(0, total, batch_size)):
            writer = PdfFileWriter()
            for page in range(start_page, start_page + batch_size):
                if page >= total:
                    break
                writer.addPage(reader.getPage(page))
            batch_name = ('%03d.pdf' % batch_idx)
            with open(batches_dir / batch_name, 'wb') as wf:
                writer.write(wf)


def pdf_batches(build_dir, filename, start_batch):
    batches_dir = get_batches_dir(build_dir, filename)
    for batch_idx, batch_file in enumerate(sorted(batches_dir.iterdir())[start_batch:]):
        if batch_file.name == 'pageCount':
            continue
        with open(batch_file, 'rb') as f:
            yield (batch_idx + start_batch, f)


def get_doc_content_as_batches(build_dir, doc_url, batch_size=10, start_batch=0):
    filename = filename_from_url(doc_url)
    batches_dir = get_batches_dir(build_dir, filename)
    if not batches_dir.exists():
        download_doc(build_dir, doc_url)
        split_pdf_into_batches(build_dir, filename, batch_size)
    with open(batches_dir / 'pageCount', 'r') as cf:
        page_count = int(cf.read().strip())
    return page_count, pdf_batches(build_dir, filename, start_batch)


def extract_tables(csv_path, start_idx, result, trim_selection_marks=False):
    for idx, tbl in enumerate(result.tables):
        tbl_idx = start_idx + idx
        fp = pathlib.Path("%s_%03d.csv" % (csv_path.strip(), tbl_idx))
        if not fp.parent.exists():
            os.makedirs(fp.parent)
        rows = [[''] * tbl.column_count for _ in range(tbl.row_count)]
        for cell in tbl.cells:
            content = cell.content.strip()
            if trim_selection_marks:
                cell.content = selection_marks_regex.sub('', content)
            rows[cell.row_index][cell.column_index] = cell.content
        with open(fp, 'w') as f:
            w = csv.writer(f)
            w.writerows(rows)
    return start_idx + len(result.tables)


if __name__ == '__main__':
    load_dotenv()
    fr_endpoint = os.environ['FORM_RECOGNIZER_ENDPOINT']
    fr_key = os.environ['FORM_RECOGNIZER_KEY']

    parser = argparse.ArgumentParser(
        description='Extract tables from a document using Azure Form Recognizer and save as CSV files.')
    parser.add_argument(
        'doc_url', type=str, metavar='DOC_URL',
        help='URL of the PDF document to extract tables from',
    )
    parser.add_argument(
        'csv_path', type=str, metavar='CSV_PATH',
        help=(
            'Path pattern that will be appended with table index and ".csv" to generate the CSV save paths. '
            'I.e., if DOC_URL contains two tables and CSV_PATH is "/data/my_doc" then the results '
            'will be saved to "/data/my_doc_00.csv" and "/data/my_doc_01.csv"'
        )
    )
    parser.add_argument(
        '--trim-selection-marks', action='store_true',
        help='remove ":selected:" and ":unselected:" sequences from cell values'
    )
    parser.add_argument(
        '--build-dir', type=pathlib.Path, metavar='BUILD_DIR', default=pathlib.Path.cwd() / 'build',
        help=(
            'Build directory which will contain temporary files that is necessary for document extraction. '
            'This script will create and store files under BUILD_DIR/docs. Defaults to {{cwd}}/build.'
        )
    )
    parser.add_argument(
        '--batch-size', type=int, metavar='BATCH_SIZE', default=10,
        help='PDF batch size (number of pages) that get sent to FormRecognizer in a single request.'
    )
    args = parser.parse_args()

    document_analysis_client = DocumentAnalysisClient(
        endpoint=fr_endpoint, credential=AzureKeyCredential(fr_key)
    )

    batch_progress_fp = pathlib.Path(args.csv_path).parent / 'progress.txt'
    start_batch = 0
    if batch_progress_fp.exists():
        with open(batch_progress_fp, 'r') as f:
            start_batch = int(f.read().strip())
            print('resume at batch %d' % start_batch)

    page_count, generator = get_doc_content_as_batches(args.build_dir, args.doc_url, args.batch_size, start_batch)
    with tqdm(total=page_count, desc='analyzing PDF pages {batch_size=%d}' % (args.batch_size)) as pbar:
        tbl_idx = start_batch * args.batch_size
        pbar.update(tbl_idx)
        for batch_idx, batch_file in generator:
            poller = document_analysis_client.begin_analyze_document("prebuilt-layout", batch_file)
            result = poller.result()
            tbl_idx = extract_tables(args.csv_path, tbl_idx, result, args.trim_selection_marks)
            pbar.update(args.batch_size)
            with open(batch_progress_fp, 'w') as f:
                f.write('%d' % (batch_idx + 1))
    batch_progress_fp.unlink()
    print("extracted tables written to %s_XXX.csv" % args.csv_path)
