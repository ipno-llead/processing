#!/usr/bin/env python3

import os
import argparse
import tempfile
import subprocess
import json

from tqdm import tqdm
import pypdfium2 as pdfium


SOURCE_BUCKET = "k8s-ocr-jobqueue-pdfs"
KUSTOMIZE_DIR = "k8s-ocr-jobqueue"


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

    with tempfile.TemporaryDirectory() as tmpdirname:
        paths = args.paths
        pos = 0
        if len(paths) > 1:
            paths = tqdm(paths, desc="pdf paths", position=0, leave=False)
            pos = 1
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
                    with pdfium.PdfDocument(filepath) as pdf:
                        for ind, img in enumerate(
                            tqdm(
                                pdf.render_topil(scale=2),
                                desc="splitting %s" % json.dumps(file),
                                position=pos + 2,
                                leave=False,
                            )
                        ):
                            filepath = os.path.abspath(
                                os.path.join(
                                    tmpdirname, relroot, file, "%03d.png" % (ind + 1,)
                                )
                            )
                            os.makedirs(os.path.dirname(filepath), exist_ok=True)
                            img.save(filepath, "PNG")
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
            "kustomize build %s | kubectl delete -f -; kustomize build %s | kubectl apply -f -"
            % (KUSTOMIZE_DIR, KUSTOMIZE_DIR),
        ],
    )
