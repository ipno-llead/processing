#!/usr/bin/env python3

import os
import argparse
import tempfile
import subprocess
import json
from typing import List

from tqdm import tqdm
import pypdfium2 as pdfium


SOURCE_BUCKET = "k8s-ocr-jobqueue-pdfs"
KUSTOMIZE_DIR = "k8s-ocr-jobqueue"


def run(paths: List[str], pos=0) -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        if len(paths) > 1:
            paths = tqdm(paths, desc="pdf paths", position=pos, leave=False)
            pos += 1
        for path in paths:
            path = path.rstrip("/")
            head, _ = os.path.split(path)
            for root, _, files in tqdm(
                list(os.walk(path)), desc=path, position=pos, leave=False
            ):
                relroot = os.path.relpath(root, head)
                for file in tqdm(files, desc=root, position=pos + 1, leave=False):
                    if not file.endswith(".pdf"):
                        continue
                    filepath = os.path.join(root, file)
                    if os.path.islink(filepath):
                        filepath = os.readlink(filepath)
                    with pdfium.PdfDocument(filepath) as pdf:
                        pdf_dir = os.path.abspath(
                            os.path.join(tmpdirname, relroot, file)
                        )
                        os.makedirs(pdf_dir, exist_ok=True)
                        for ind, img in enumerate(
                            tqdm(
                                pdf.render_topil(scale=2),
                                desc="splitting %s" % json.dumps(file),
                                position=pos + 2,
                                leave=False,
                            )
                        ):
                            img.save(
                                os.path.join(pdf_dir, "%03d.png" % (ind + 1,)), "PNG"
                            )
                        with open(os.path.join(pdf_dir, "count"), "w") as f:
                            f.write(str(ind + 1))
        subprocess.run(
            [
                "gsutil",
                "-m",
                "rsync",
                "-c",
                "-i",
                "-J",
                "-r",
                tmpdirname,
                "gs://%s" % (SOURCE_BUCKET,),
            ],
            check=True,
        )
    subprocess.run(
        [
            "bash",
            "-c",
            f"kubectl kustomize {KUSTOMIZE_DIR} | kubectl apply -f -",
        ],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enqueue PDF files for OCR processing."
    )
    parser.add_argument(
        "paths",
        metavar="PATH",
        type=str,
        nargs="+",
        help="a path that contain PDF files to be enqueued. Files that already exist in the queue will be ignored.",
    )
    args = parser.parse_args()

    run(args.paths)
