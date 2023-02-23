import deba
import pandas as pd
from lib.dvc import files_meta_frame
from lib.meta import download_db_metadata


def set_filetype(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.filepath.str.lower().str.endswith(".pdf"), "filetype"] = "pdf"
    return df


def split_filepath(df: pd.DataFrame) -> pd.DataFrame:
    filepaths = df.filepath.str.split("/")
    df.loc[:, "fn"] = filepaths.map(lambda v: v[:])
    return df


def set_file_category(df: pd.DataFrame) -> pd.DataFrame:
    column = "file_category"
    df.loc[df.fn.astype(str).str.match(r"(.+)"), column] = "louisiana_state_pd_letter"
    return df


def fetch_reports() -> pd.DataFrame:
    return (
        files_meta_frame("raw_louisiana_state_pd_reports_2009_2020.dvc")
        .pipe(set_filetype)
        .pipe(split_filepath)
        .pipe(set_file_category)
    )


def db_path():
    fpath = (
        r"/IPNO/data/Louisiana State/Data Collected/Louisiana State Police Commission/"
        r"Data Collected/Letters/raw_louisiana_state_pd_reports_2009_2020"
    )
    return fpath


if __name__ == "__main__":
    df = fetch_reports()
    df.to_csv(deba.data("meta/letters_louisiana_state_pd_2009_2020_files.csv"), index=False)
    fpath = db_path()
    db_meta = download_db_metadata(fpath)
    db_meta.to_csv(
        "data/raw/louisiana_state_pd/letters_louisiana_state_pd_2009_2020_db_files.csv"
    )
