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
    df.loc[df.fn.astype(str).str.match(r"(.+)"), column] = "nopd_pib_annual_report"
    return df


def fetch_reports() -> pd.DataFrame:
    return (
        files_meta_frame("raw_nopd_pib_reports.dvc")
        .pipe(set_filetype)
        .pipe(split_filepath)
        .pipe(set_file_category)
    )


if __name__ == "__main__":
    df = fetch_reports()
    df.to_csv(deba.data("meta/nopd_pib_reports_files_2014_2019.csv"), index=False)
