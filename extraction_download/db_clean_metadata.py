import pandas as pd
import deba
import sys

sys.path.append("../")


def path(df):
    path = df.meta_data.str.extract(r"(\, path_display(.+)\, path_lower)")
    df.loc[:, "pdf_db_path"] = (
        path[0]
        .str.replace("path_lower", "", regex=False)
        .str.replace(r"\, path_display=\'", "", regex=True)
        .str.replace(".txt", "", regex=False)
    )
    return df


def extract_names(df):
    df.loc[:, "file_name"] = (
        df.pdf_db_path.str.replace(r"^(.+)\/text\/", "", regex=True)
        .str.replace(r"^(.+)\/transcripts/\/", "", regex=True)
        .str.replace(r"^(.+)\/transcripts\/", "", regex=True)
        .str.replace(r"experimment\/", "", regex=True)
        .str.replace(r"\.pdf(.+)", ".pdf", regex=True)
    )
    return df


def content_hash(df):
    c_hash = df.meta_data.str.extract(r"(\, content(.+)\, export_info)")
    df.loc[:, "pdf_db_content_hash"] = (
        c_hash[0]
        .str.replace(" export_info", "", regex=False)
        .str.replace("content_hash=", "", regex=False)
        .str.replace(r"\,", "", regex=True)
        .str.replace(r"\'", "", regex=True)
    )
    return df


def db_id(df):
    db_id = df.meta_data.str.extract(r"(\, id(.+)\, is_downloadable)")
    df.loc[:, "pdf_db_id"] = (
        db_id[0]
        .str.replace(" is_downloadable", "", regex=False)
        .str.replace("id=", "", regex=False)
        .str.replace(r"\,", "", regex=True)
        .str.replace(r"\'", "", regex=True)
    )
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/post/metadata/post_metadata.csv"))
        .pipe(path)
        .pipe(extract_names)
        .pipe(content_hash)
        .pipe(db_id)
        .rename(columns={"meta_data": "pdf_meta_data"})
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("raw/post/metadata/clean_post_metadata.csv"), index=False)
