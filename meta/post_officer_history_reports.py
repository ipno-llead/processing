import deba
from lib.dvc import files_meta_frame
import pandas as pd


def set_filetype(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.filepath.str.lower().str.endswith(".pdf"), "filetype"] = "pdf"
    return df


def split_filepath(df: pd.DataFrame) -> pd.DataFrame:
    filepaths = df.filepath.str.split("/")
    df.loc[:, "fn"] = filepaths.map(lambda v: v[:])
    return df


def set_file_category(df: pd.DataFrame) -> pd.DataFrame:
    column = "file_category"
    df.loc[df.fn.astype(str).str.match(r"(.+)"), column] = "post_officer_history_report"
    return df


def fetch_reports_2021() -> pd.DataFrame:
    return (
        files_meta_frame("raw_post_officer_history_reports.dvc")
        .pipe(set_filetype)
        .pipe(split_filepath)
        .pipe(set_file_category)
    )


def fetch_reports_2022() -> pd.DataFrame:
    return (
        files_meta_frame("raw_post_officer_history_reports_9_16_2022.dvc")
        .pipe(set_filetype)
        .pipe(split_filepath)
        .pipe(set_file_category)
    )

if __name__ == "__main__":
    df21 = fetch_reports_2021()
    df22 = fetch_reports_2022()
    df21.to_csv(deba.data("meta/post_officer_history_reports_files.csv"), index=False)
    df22.to_csv(deba.data("meta/post_officer_history_reports_9_16_2022_files.csv"), index=False)
