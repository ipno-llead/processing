#!/usr/bin/env python

import re
import json
import argparse
import os

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


if __name__ == "__main__":
    load_dotenv()
    conn_str = os.environ["BLOB_STORAGE_CONNECTION_STRING"]
    container = os.environ["FORM_RECOGNIZER_CONTAINER"]

    parser = argparse.ArgumentParser(description="Edit FormRecognizer table labels.")
    parser.add_argument(
        "pdf_path",
        type=str,
        metavar="PDF_PATH",
        help="Path of the PDF whose labels need to be edit",
    )
    parser.add_argument(
        "table_tag", type=str, metavar="TABLE_TAG", help="Name of the table tag to edit"
    )
    parser.add_argument(
        "op",
        choices=["insertRow", "removeRow"],
        metavar="OP",
        help="Operation to perform on the table",
    )
    parser.add_argument(
        "start", type=int, metavar="START", help="index of the first row to operate on"
    )
    parser.add_argument(
        "count",
        type=int,
        metavar="COUNT",
        default=1,
        help="the number of rows to insert/remove",
    )
    args = parser.parse_args()

    if args.start < 0:
        raise ValueError("START must be >= 0")

    if args.count < 1:
        raise ValueError("COUNT must be >= 1")

    labels_path = re.sub(r"\.pdf$", r".pdf.labels.json", args.pdf_path)

    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(
        container=container, blob=labels_path
    )
    dl = blob_client.download_blob()
    labels = json.loads(dl.readall())

    pat = re.compile(r"^%s/(\d+)/(.+)$" % args.table_tag)
    labels_to_remove = []
    changes = 0
    for idx, obj in enumerate(labels["labels"]):
        m = pat.match(obj["label"])
        if m is None:
            continue
        row_idx = int(m.group(1))
        if row_idx < args.start:
            continue
        changes += 1
        if args.op == "insertRow":
            obj["label"] = "%s/%d/%s" % (
                args.table_tag,
                row_idx + args.count,
                m.group(2),
            )
        elif args.op == "removeRow":
            if row_idx < args.start + args.count:
                labels_to_remove.append(idx)
            else:
                obj["label"] = "%s/%d/%s" % (
                    args.table_tag,
                    row_idx - args.count,
                    m.group(2),
                )
    labels["labels"] = [
        obj for idx, obj in enumerate(labels["labels"]) if idx not in labels_to_remove
    ]

    blob_client.upload_blob(json.dumps(labels), overwrite=True)

    print("edit %d labels" % changes)
