import deba
import pandas as pd
from lib.dvc import real_dir_path
from lib.ocr import process_pdf


def only_pdf(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df.filetype == "pdf"].reset_index(drop=True)


def process_all_pdfs() -> pd.DataFrame:
    dir_name = real_dir_path("raw_post_officer_history_reports.dvc")
    return (
        pd.read_csv(deba.data("meta/post_officer_history_reports_files.csv"))
        .pipe(only_pdf)
        .pipe(process_pdf, dir_name)
    )


def process_pdfs_2022() -> pd.DataFrame:
    dir_name = real_dir_path("raw_post_officer_history_reports_9_16_2022.dvc")
    return (
        pd.read_csv(deba.data("meta/post_officer_history_reports_9_16_2022_files.csv"))
        .pipe(only_pdf)
        .pipe(process_pdf, dir_name)
    )


if __name__ == "__main__":
    df21 = process_all_pdfs()
    df22 = process_pdfs_2022()
    df21.to_csv(deba.data("ocr/post_officer_history_reports_pdfs.csv"), index=False)
    df22.to_csv(deba.data("ocr/post_officer_history_reports_9_16_2022_pdfs.csv"), index=False)
