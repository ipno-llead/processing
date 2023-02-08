from pathlib import Path

import deba
import pandas as pd
from vscode_spot_check import print_samples

from lib.dvc import real_dir_path
from lib.ocr import process_pdf


def only_pdf(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df.filetype == "pdf"].reset_index(drop=True)


def process_all_pdfs() -> pd.DataFrame:
    dir_name = real_dir_path("raw_louisiana_state_pd_letters_2019.dvc")
    return (
        pd.read_csv(deba.data("meta/letters_louisiana_state_pd_2019_files.csv"))
        .pipe(only_pdf)
        .pipe(process_pdf, dir_name)
    )


if __name__ == "__main__":
    df = process_all_pdfs()

    print_samples(
        df,
        resolve_source_path=lambda row: Path(__file__).parent.parent
        / deba.data("raw_louisiana_state_pd_letters_2019")
        / row.filepath,
        resolve_pageno=lambda row: row.pageno,
    )

    df.to_csv(deba.data("ocr/letters_louisiana_state_pd_2019_pdfs.csv"), index=False)
