#!/usr/bin/env python
import argparse
import re
import shutil

from tqdm import tqdm
from PyPDF2 import PdfFileReader, PdfFileWriter


def reverse_order(filename: str, overwrite: bool):
    new_filename = re.sub(r"\.pdf$", "-re.pdf", filename)
    with open(filename, "rb") as f:
        reader = PdfFileReader(f)
        total = reader.getNumPages()
        writer = PdfFileWriter()
        for pn in tqdm(range(total - 1, -1, -1)):
            page = reader.getPage(pn)
            writer.addPage(page)
        with open(new_filename, "wb") as wf:
            writer.write(wf)
    if overwrite:
        shutil.move(new_filename, filename)
        print("overwritten %s" % filename)
    else:
        print("written to %s" % new_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reverse page order in PDF_FILE.")
    parser.add_argument(
        "pdf_file",
        type=str,
        metavar="PDF_FILE",
        help="PDF file to rotate",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite PDF_FILE, otherwise write to a new file with suffix -re",
    )
    args = parser.parse_args()

    reverse_order(args.pdf_file, args.overwrite)
