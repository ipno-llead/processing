import os
from os.path import dirname, join
import sys

root_dir = dirname(dirname(__file__))
sys.path.insert(0, root_dir)

import deba
import pandas as pd

from lib.dropbox import sync_local_to_dropbox
from lib.pdf import subset_pdf


hearing_files_dir = join(root_dir, "build/hearing_files")


def process_doc(df: pd.DataFrame) -> pd.Series:
    docid = df.name
    txt_path = join(hearing_files_dir, "%s.txt" % docid)
    with open(txt_path, "w") as f:
        f.write(
            df.text.str.replace(r"(\n *){3,}", "\n\n", regex=True).str.cat(sep="\n")
        )
    pdf_path = join(hearing_files_dir, "%s.pdf" % docid)
    subset_pdf(
        join(root_dir, "data/raw_minutes", df.filepath.iloc[0]),
        pdf_path,
        df.pageno.min() - 1,
        df.pageno.max(),
    )
    page_count = len(df)
    d = {"txt_path": txt_path, "pdf_path": pdf_path, "page_count": page_count}
    return pd.Series(d, index=sorted(d.keys()))


if __name__ == "__main__":
    mins = pd.read_csv(deba.data("match/minutes_hearing_text.csv"))
    pdfs = pd.read_csv(deba.data("ocr/minutes_pdfs.csv"))

    os.makedirs(hearing_files_dir, exist_ok=True)
    local_files = (
        pdfs.merge(mins[["docid", "fileid"]], how="left", on="fileid")
        .sort_values(["fileid", "pageno"])
        .groupby("docid")
        .apply(process_doc)
    )
