import deba
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import clean_dates, names_to_title_case
from lib.uid import gen_uid


def concat_text_from_all_pages(df):
    df = df[~((df.text.fillna("") == ""))]
    df.loc[:, "text"] = df.text.str.replace(r"\n", " ", regex=True)
    text = df.groupby("md5")["text"].apply(" ".join)

    df = df.drop(columns=["text"])
    df = pd.merge(df, text, on="md5", how="outer")
    df = df[df.fileid.isin(["35294a6"])]
    return df.drop_duplicates(subset=["md5"])[~((df.md5.fillna("") == ""))]


def extract_db_meta(df):
    fn = df.meta_data.str.extract(r", name=\'(.+.pdf)\', (parent_shared_folder_id)")
    pdf_db_content_hash = df.meta_data.str.extract(
        r", content_hash=\'(.+)\', (export_info)"
    )
    pdf_db_path = df.meta_data.str.extract(r", path_display=\'(.+)\', (path_lower)")
    pdf_db_id = df.meta_data.str.extract(r", id=\'(.+)\', (is_downloadable)")

    df.loc[:, "fn"] = fn[0]
    df.loc[:, "pdf_db_content_hash"] = pdf_db_content_hash[0]
    df.loc[:, "pdf_db_path"] = pdf_db_path[0]
    df.loc[:, "pdf_db_id"] = pdf_db_id[0]
    return df.drop(columns=["meta_data"])


def clean_fn(df):
    df.loc[:, "fn"] = df.fn.str.replace(r"(\[|\]|\')", "", regex=True)
    return df


def assign_dates(df):
    df.loc[((df.fileid == "62b60db")), "doc_date"] = "12/31/2021"
    df.loc[((df.fileid == "35294a6")), "doc_date"] = "12/31/2021"
    df.loc[((df.fileid == "c5882bc")), "doc_date"] = "12/31/2020"
    df.loc[((df.fileid == "96fb849")), "doc_date"] = "12/31/2021"
    return df


def assign_titles(df):
    titles = df.fn.str.extract(r"(.+)")
    df.loc[:, "title"] = (
        titles[0]
        .str.replace(r"(\..+)", "", regex=True)
        .str.replace(r"_", " ", regex=False)
        .str.replace(r"so\b", "Sheriffs Office", regex=True)
        .str.replace(r"(\[|\')", "", regex=True)
    )
    return df.pipe(names_to_title_case, ["title"])


def assign_agencies(df):
    agencies = df.fn.str.extract(r"(.+so)")
    df.loc[:, "agency"] = (
        agencies[0]
        .str.replace(r"_", "-", regex=False)
        .str.replace(r"(\[|\')", "", regex=True)
    )
    return df


def drop_rows_with_bad_text(df):
    df.loc[:, "pageno"] = df.pageno.astype(str).str.replace(
        r"(\w+)(.+)", "", regex=True
    )
    return df[~((df.pageno.fillna("") == ""))]


def budgets():
    db_meta = pd.read_csv(deba.data("raw/budgets/budgets_db_meta.csv")).pipe(
        extract_db_meta
    )
    df = (
        pd.read_csv(deba.data("ocr/budgets.csv"))
        .pipe(clean_column_names)
        .pipe(concat_text_from_all_pages)
        .pipe(assign_dates)
        .pipe(assign_titles)
        .pipe(assign_agencies)
        .pipe(clean_fn)
    )
    df = pd.merge(df, db_meta, on="fn")
    df = (
        df.rename(columns={"md5": "docid"})
        .pipe(clean_dates, ["doc_date"])
        .pipe(gen_uid, ["agency"], "matched_uid")
    )
    return df


if __name__ == "__main__":
    df = budgets()
    df.to_csv(deba.data("clean/docs_budgets.csv"), index=False)
