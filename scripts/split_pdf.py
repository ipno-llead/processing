#!/usr/bin/env python

import errno
import argparse
import os
import pathlib

from PyPDF2 import PdfFileReader, PdfFileWriter


def split_pdf(filepath, output_dir):
    with open(filepath, "rb") as f:
        reader = PdfFileReader(f)
        total = reader.getNumPages()
        for page in range(0, total):
            writer = PdfFileWriter()
            writer.addPage(reader.getPage(page))
            with open(output_dir / ("%04d.pdf" % page), "wb") as wf:
                writer.write(wf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Split given PDF_FILE into individual pages and save to OUTPUT_DIR."
    )
    parser.add_argument(
        "pdf_file",
        type=pathlib.Path,
        metavar="PDF_FILE",
        help="PDF file to extract pages from",
    )
    parser.add_argument(
        "output_dir",
        type=pathlib.Path,
        metavar="OUTPUT_DIR",
        help="Directory to output individual pages to",
    )
    args = parser.parse_args()

    if not args.pdf_file.exists():
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), args.links_file
        )
    if not args.output_dir.exists():
        os.makedirs(args.output_dir)

    split_pdf(args.pdf_file, args.output_dir)
