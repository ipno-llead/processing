import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import names_to_title_case, clean_dates
from lib.uid import gen_uid
from functools import reduce


def concat_text_from_all_pages(df):
    text = df.groupby("md5")["text"].apply(" ".join).reset_index()
    df = df.drop(columns=["text"])
    df = pd.merge(df, text, on="md5")
    return df


def join_multiple_extracted_entity_cols(df):
    disposition_cols = [
        "appeal_hearing_disposition",
        "appeal_hearing_disposition_1",
        "appeal_hearing_disposition_2",
        "appeal_hearing_disposition_3",
    ]

    df["appeal_disposition"] = df[disposition_cols].apply(
        lambda row: " ".join(row.values.astype(str)), axis=1
    )
    disposition_df = df[["appeal_disposition", "md5"]]
    disposition_df = (
        disposition_df.groupby("md5")["appeal_disposition"]
        .apply(" ".join)
        .reset_index()
    )
    disposition_df.loc[
        :, "appeal_disposition"
    ] = disposition_df.appeal_disposition.str.replace(
        r" ?nan ?", "", regex=True
    ).str.replace(
        r" +", r" ", regex=True
    )

    name_cols = [
        "accused_name",
        "accused_name_1",
        "accused_name_2",
        "accused_name_3",
        "accused_name_4",
    ]

    df["accused_name"] = df[name_cols].apply(
        lambda row: " ".join(row.values.astype(str)), axis=1
    )
    name_df = df[["accused_name", "md5"]]
    name_df = name_df.groupby("md5")["accused_name"].apply(" ".join).reset_index()
    name_df.loc[:, "accused_name"] = name_df.accused_name.str.replace(
        r" ?nan ?", " ", regex=True
    ).str.replace(r" +", r" ", regex=True)

    docket_cols = [
        "docket_number",
        "docket_number_1",
        "docket_number_2",
        "docket_number_3",
        "docket_number_4",
    ]

    df["docket_number"] = df[docket_cols].apply(
        lambda row: " ".join(row.values.astype(str)), axis=1
    )
    docket_df = df[["docket_number", "md5"]]
    docket_df = docket_df.groupby("md5")["docket_number"].apply(" ".join).reset_index()
    docket_df.loc[:, "docket_number"] = docket_df.docket_number.str.replace(
        r" ?nan ?", " ", regex=True
    ).str.replace(r" +", r" ", regex=True)

    date_cols = [
        "appeal_hearing_date",
        "appeal_hearing_date_1",
        "appeal_hearing_date_2",
    ]

    df["appeal_hearing_date"] = df[date_cols].apply(
        lambda row: " ".join(row.values.astype(str)), axis=1
    )
    date_df = df[["appeal_hearing_date", "md5"]]
    date_df = (
        date_df.groupby("md5")["appeal_hearing_date"].apply(" ".join).reset_index()
    )
    date_df.loc[:, "appeal_hearing_date"] = date_df.appeal_hearing_date.str.replace(
        r" ?nan ?", " ", regex=True
    ).str.replace(r" +", r" ", regex=True)

    df = df[
        [
            "appeal_closed_date",
            "md5",
            "filepath",
            "filesha1",
            "fileid",
            "filetype",
            "fn",
            "file_category",
            "text",
            "page_count",
        ]
    ]

    data_frames = [df, disposition_df, name_df, docket_df, date_df]

    df = reduce(
        lambda left, right: pd.merge(left, right, on=["md5"], how="outer"), data_frames
    )
    return df[~((df.appeal_closed_date.fillna("") == ""))]


def drop_rows_missing_names(df):
    df.loc[:, "accused_name"] = (
        df.accused_name.str.lower()
        .str.strip()
        .str.replace(r"appellant\,? ?", "", regex=True)
    )
    return df[~(df.accused_name.fillna("") == "")]


def extract_doc_num(df):
    df.loc[:, "tracking_id_og"] = (
        df.docket_number.str.lower()
        .str.strip()
        .str.replace(r"(\w+) (\w+)[;:] (\w+) (.+)", r"\3")
        .str.replace(r"\n(.+)?", "", regex=True)
        .str.replace(r"(docket)? ?n(umber|o)[:\.] ", "", regex=True)
        .str.replace(r"8848(.+)", "8848", regex=True)
    )
    return df.drop(columns=["docket_number"])


def split_name(df):
    df.loc[:, "accused_name"] = (
        df.accused_name.str.lower()
        .str.replace(r"appellant", "", regex=False)
        .str.replace(r"^berger et\b", "eric berger", regex=True)
        .str.replace(r"a ie mitchell", "ananie mitchell", regex=False)
        .str.replace(r"^ +(\w+)", r"\1", regex=True)
        .str.replace(r"gremillion et\b", "gary gremillion", regex=True)
    )
    names = df.accused_name.str.extract(r"^(\w+) ?(\w\.)? ?(?:(\w+-?\w+? ?j?s?r?\.?) )")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "middle_name"] = names[1].str.replace(r"\.", "", regex=True)
    df.loc[:, "last_name"] = (
        names[2]
        .str.replace(r"\b(\w)$", "", regex=True)
        .str.replace(r"\.", "", regex=True)
    )
    return df[~((df.last_name.fillna("") == ""))].drop(columns=["accused_name"])


def extract_disposition(df):
    dispos = (
        df.appeal_disposition.str.lower()
        .str.strip()
        .str.replace(r"\n", " ", regex=True)
        .str.extract(r"(denies in part|grants?e?d? in part|deny|denie[sd]|grants?e?d?)")
    )
    df.loc[:, "appeal_disposition"] = (
        dispos[0]
        .str.replace(r"deny?i?e?[sd]?", "denied", regex=True)
        .str.replace(r"grants?\b", "granted")
    )
    return df


def clean_hearing_dates(df):
    df.loc[:, "appeal_hearing_date"] = (
        df.appeal_hearing_date.str.lower()
        .str.strip()
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"(\w+)\, ?(\w+)", r"\1/\2", regex=True)
        .str.replace(r"december (.+)", r"12/\1", regex=True)
        .str.replace(r"november (.+)", r"11/\1", regex=True)
        .str.replace(r"[uo]ctober (.+)", r"10/\1", regex=True)
        .str.replace(r"september (.+)", r"9/\1", regex=True)
        .str.replace(r"august (.+)", r"8/\1", regex=True)
        .str.replace(r"july (.+)", r"7/\1", regex=True)
        .str.replace(r"june (.+)", r"6/\1", regex=True)
        .str.replace(r"may (.+)", r"5/\1", regex=True)
        .str.replace(r"april (.+)", r"4/\1", regex=True)
        .str.replace(r"march (.+)", r"3/\1", regex=True)
        .str.replace(r"february (.+)", r"2/\1", regex=True)
        .str.replace(r"january (.+)", r"1/\1", regex=True)
        .str.replace(r"(wednesday|tuesday|thursday)\/", "", regex=True)
        .str.replace(r"\,", "", regex=True)
        .str.replace(r"\n", "", regex=True)
        .str.replace(r"(.+)?(officer|june|appellant|violation)(.+)?", "", regex=True)
        .str.replace(r"(.+)and(.+)", r"\2", regex=True)
        .str.replace(r"^(\w)\/(\w{2})$", "", regex=True)
        .str.replace(r"(\w)\/(\w)(\w{4})", r"\1/\2/\3", regex=True)
        .str.replace(r"2/14/2013 3/7/2013", "3/7/2013", regex=False)
        .str.replace(r"8/18-19/2021", "8/19/2021", regex=False)
        .str.replace(r"10/i/2020", "10/1/2020", regex=False)
        .str.replace(r"6\/21\. 2012", "6/21/2012", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
    )
    return df


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


def generate_title(df):
    title_cols = [
        "first_name",
        "last_name",
        "appeal_closed_date",
    ]

    df["title"] = df[title_cols].apply(
        lambda row: " ".join(row.values.astype(str)), axis=1
    )
    df.loc[:, "title"] = df.title.str.replace(
        r"^(\w+)", r"Appeal closed: \1"
    ).str.replace(r"(\w+)\/(\w+)\/(\w+)", r"on \1/\2/\3", regex=True)
    return df.pipe(names_to_title_case, ["title"])


def generate_doc_table_data(df):
    date = df.appeal_closed_date.str.extract(r"(\w+)\/(\w+)\/(\w+)")

    df.loc[:, "month"] = date[0]
    df.loc[:, "day"] = date[1]
    df.loc[:, "year"] = date[2]

    df.loc[:, "accused"] = df.first_name.str.cat(df.last_name, sep=" ")
    df.loc[:, "hrg_text"] = ""
    df.loc[:, "docid"] = df.md5
    return df.pipe(names_to_title_case, ["accused"])


def clean():
    dfa = (
        pd.read_csv(deba.data("ner/new_orleans_pd_appeals_pdfs.csv"))
        .pipe(clean_column_names)
        .rename(columns={"appeal_filed_date": "appeal_closed_date", "pageno": "page_count"})
        .pipe(concat_text_from_all_pages)
        .pipe(join_multiple_extracted_entity_cols)
        .pipe(split_name)
        .pipe(extract_doc_num)
        .pipe(extract_disposition)
        .pipe(clean_hearing_dates)
        .pipe(generate_title)
        .pipe(generate_doc_table_data)
        .pipe(clean_fn)
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"], "matched_uid")
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(
            gen_uid,
            [
                "tracking_id_og",
                "appeal_hearing_date",
                "appeal_closed_date",
                "appeal_disposition",
                "uid",
            ],
        )
    )
    db_meta = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_appeals_db_files.csv"))
        .pipe(clean_column_names)
        .pipe(extract_db_meta)
    )

    df = pd.merge(dfa, db_meta, on="fn")
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/app_new_orleans_pd.csv"), index=False)
