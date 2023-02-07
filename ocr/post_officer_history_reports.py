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


def process_pdfs_adv() -> pd.DataFrame:
    dir_name = real_dir_path("post_ohr_advocate.dvc")
    return (
        pd.read_csv(deba.data("meta/post_officer_history_reports_advocate_files.csv"))
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


def process_pdfs_2023() -> pd.DataFrame:
    dir_name = real_dir_path("raw_post_officer_history_reports_2023.dvc")
    return (
        pd.read_csv(deba.data("meta/post_officer_history_reports_2023_files.csv"))
        .pipe(only_pdf)
        .pipe(process_pdf, dir_name)
    )


if __name__ == "__main__":
    df_21 = process_all_pdfs()
    df_22 = process_pdfs_2022()
    df_23 = process_pdfs_2023()
    df_adv = process_pdfs_adv()
    df_21.to_csv(deba.data("ocr/post_officer_history_reports_pdfs.csv"), index=False)
    df_22.to_csv(deba.data("ocr/post_officer_history_reports_9_16_2022_pdfs.csv"), index=False)
    df_23.to_csv(deba.data("ocr/post_officer_history_reports_2023_pdfs.csv"), index=False)
    df_adv.to_csv(deba.data("ocr/post_officer_history_reports_advocate_pdfs.csv"), index=False)
