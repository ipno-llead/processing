#!/usr/bin/env python
import argparse
import re
import shutil

from tqdm import tqdm
from PyPDF2 import PdfFileReader, PdfFileWriter


def rotate(filename: str, degree: int, overwrite: bool):
    new_filename = re.sub(r"\.pdf$", "-ro%d.pdf" % degree, filename)
    with open(filename, "rb") as f:
        reader = PdfFileReader(f)
        total = reader.getNumPages()
        writer = PdfFileWriter()
        for pn in tqdm(range(total)):
            page = reader.getPage(pn)
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
    args = parser.parse_args()

    rotate(args.pdf_file, args.degree, args.overwrite)
