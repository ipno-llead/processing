from posixpath import split
import deba
import pandas as pd
from lib.clean import (
    float_to_int_str,
    names_to_title_case,
    standardize_desc_cols,
    clean_dates,
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def concat_text_from_all_pages(df):
    text = df.groupby("md5")["text"].apply(" ".join).reset_index()
    df = df.drop(columns=["text"])
    df = pd.merge(df, text, on="md5", how="outer")
    return df


def drop_rows_missing_names(df):
    df.loc[:, "accused_name"] = (
        df.accused_name.str.lower()
        .str.strip()
        .str.replace(r"appellant\,? ?", "", regex=True)
    )
    return df[~(df.accused_name.fillna("") == "")]


def extract_doc_num(df):
    docs = df.docket_number.str.lower().str.strip().str.extract(r"(\w{4})$")

    df.loc[:, "tracking_id_og"] = docs[0]
    return df.drop(columns=["docket_number"])[~(df.docket_number.fillna("") == "")]

## we could extract factual background from new appeal hearing transcripts
## and analysis

def split_name(df):
    df.loc[:, "accused_name"] = df.accused_name.str.replace(r"appellant", "", regex=False)
    names = df.accused_name.str.extract(r"(\w+) (\w+)")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns=["accused_name"])


def clean_fn(df):
    df.loc[:, "fn"] = df.fn.str.replace(r"(\[|\]|\')", "", regex=True)
    return df 


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

## fix tracking_id_og

def clean():
    dfa = (
        pd.read_csv(deba.data("ner/new_orleans_pd_appeals_pdfs.csv"))
        .pipe(clean_column_names)
        # .drop(
        #     columns=[
        #         "docket_number_1",
        #         "docket_number_2",
        #         "docket_number_3",
        #         "docket_number_4",
        #         "accused_name_1",
        #         "accused_name_2",
        #         "accused_name_3",
        #         "accused_name_4",
        #         "pageno",
        #     ]
        # )
        # .pipe(drop_rows_missing_names)
        # .pipe(concat_text_from_all_pages)
        # .pipe(extract_doc_num)
        # .pipe(split_name)
        # .pipe(clean_fn)
        # .pipe(set_values, {"agency": "new-orleans-pd"})
        # .pipe(clean_dates, ["decision_notification_date"])
        # .pipe(gen_uid, ["first_name", "last_name", "agency"])\
        # .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
    )
    # db_meta = (pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_appeals_db_files.csv"))\
    #     .pipe(clean_column_names)
    #     .pipe(extract_db_meta)
    # )
    
    # df = pd.merge(dfa, db_meta, on="fn")
    return dfa


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/app_new_orleans_pd_appeals.csv"), index=False)
