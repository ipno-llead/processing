import deba
import pandas as pd
from lib.dvc import real_dir_path
from lib.ocr import process_pdf


def only_pdf(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df.filetype == "pdf"].reset_index(drop=True)


def process_all_pdfs() -> pd.DataFrame:
    dir_name = real_dir_path("post_ohr_advocate.dvc")
    return (
        pd.read_csv(deba.data("meta/post_officer_history_reports_advocate_files.csv"))
        .pipe(only_pdf)
        .pipe(process_pdf, dir_name)
    )


if __name__ == "__main__":
    df = process_all_pdfs()
    df.to_csv(deba.data("ocr/post_officer_history_reports_advocate_pdfs.csv"), index=False)
