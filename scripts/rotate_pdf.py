#!/usr/bin/env python
import argparse
import re
import shutil

from tqdm import tqdm
from PyPDF2 import PdfFileReader, PdfFileWriter


def rotate(filename: str, degree: int, page_range: str, overwrite: bool):
    new_filename = re.sub(r"\.pdf$", "-ro%d.pdf" % degree, filename)
    with open(filename, "rb") as f:
        reader = PdfFileReader(f)
        total = reader.getNumPages()
        if page_range:
            parts = page_range.split(":")
            start = 0 if parts[0] == "" else int(parts[0])
            end = total - 1 if parts[1] == "" else int(parts[1])
        else:
            start, end = 0, total - 1
        writer = PdfFileWriter()
        for pn in tqdm(range(total)):
            page = reader.getPage(pn)
            if pn >= start and pn <= end:
                page.rotate_clockwise(degree)
            writer.addPage(page)
        with open(new_filename, "wb") as wf:
            writer.write(wf)
    if overwrite:
        shutil.move(new_filename, filename)
        print("overwritten %s" % filename)
    else:
        print("written to %s" % new_filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rotate each page in PDF_FILE DEGREE clockwise."
    )
    parser.add_argument(
        "pdf_file",
        type=str,
        metavar="PDF_FILE",
        help="PDF file to rotate",
    )
    parser.add_argument(
        "degree",
        type=int,
        metavar="DEGREE",
        help="Clockwise degree to rotate",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite PDF_FILE, otherwise write to a new file with suffix -roDEGREE",
    )
    parser.add_argument(
        "--range",
        type=str,
        action="store",
        help="--range START:END, only rotate pages from START to END incluse",
    )
    args = parser.parse_args()

    rotate(args.pdf_file, args.degree, args.range, args.overwrite)
