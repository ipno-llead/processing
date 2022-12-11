import deba
from lib.dvc import files_meta_frame
import pandas as pd
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
    df.loc[
        df.fn.astype(str).str.match(r"(.+)"), column
    ] = "new_orleans_pd_appeal_hearing"
    return df


def fetch_reports() -> pd.DataFrame:
    return (
        files_meta_frame("raw_new_orleans_pd_appeals.dvc")
        .pipe(set_filetype)
        .pipe(split_filepath)
        .pipe(set_file_category)
    )


def db_path():
    fpath = (
        r"/IPNO/data/Orleans/Data Collected/New Orleans Civil Service Commission/Transcripts/appeals_data/raw_new_orleans_pd_appeals"
    )
    return fpath


if __name__ == "__main__":
    df = fetch_reports()
    df.to_csv(deba.data("meta/new_orleans_pd_appeals_files.csv"), index=False)
    # fpath = db_path()
    # db_meta = download_db_metadata(fpath)
    # db_meta.to_csv("data/raw/new_orleans_pd/new_orleans_pd_appeals_db_files.csv")
