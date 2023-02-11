import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, clean_dates, names_to_title_case
from lib.uid import gen_uid
from functools import reduce


def drop_rows_missing_dates(df):
    return df[~((df.notification_date.fillna("") == ""))]


def extract_ids_and_subject(df):
    cols = [
        "accused_name",
        "tracking_id",
        "tracking_id_1",
        "tracking_id_2",
        "tracking_id_3",
    ]
    df["data"] = df[cols].apply(lambda row: " ".join(row.values.astype(str)), axis=1)

    subjects = df.data.str.lower().str.extract(r"(re.+)")

    df.loc[:, "letter_subject"] = (
        subjects[0]
        .str.replace(r"( ?nan ?|re: | ?_ ?)", "", regex=True)
        .str.replace(r"(\w+) \((\w+)\) hour", r"\2-hour", regex=True)
        .str.replace(r"(\w+) hour", r"\1-hour", regex=True)
        .str.replace("eight", "8", regex=False)
        .str.replace(r"twenty- ", "", regex=False)
        .str.replace(r"©", "", regex=False)
    )

    df.loc[:, "title"] = df.letter_subject

    ids = (
        df.data.str.lower()
        .str.replace(r" ?nan ?", " ", regex=True)
        .str.replace(r"re.+", " ", regex=True)
        .str.extract(r"(.+)")
    )

    df.loc[:, "tracking_id"] = (
        ids[0]
        .str.replace(r" +$", "", regex=True)
        .str.replace(r" lt. marie", "", regex=True)
        .str.replace(r"014a", "014", regex=False)
        .str.replace(r"(.+) ola", r"\1; ola", regex=True)
        .str.replace(r"^[1t]a", "ia", regex=True)
        .str.replace(r"(\w+) ?# ?(\w+)", r"\1 \2", regex=True)
        .str.replace(r"n19", "19", regex=True)
    )

    return df.drop(
        columns=[
            "tracking_id_1",
            "tracking_id_2",
            "tracking_id_3",
            "data",
            "subject_of_letter",
        ]
    )


def split_names(df):
    names = (
        df.accused_name.str.lower()
        .str.strip()
        .str.replace(r"st\, clair", "stclair", regex=True)
        .str.replace(r"(“(\w+)”) ", "", regex=True)
        .str.extract(
            r"(master trooper|pilot|sergeant|lt\.|cadet|trooper|lieutenant) (\w+) (\w+)"
        )
    )

    df.loc[:, "rank_desc"] = names[0].str.replace(r"lt\.", "lieutenant", regex=True)
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns=["accused_name"])


def sanitize_letter_dates(df):
    df.loc[:, "letter_date"] = (
        df.notification_date.str.lower()
        .str.strip()
        .str.replace(r"(\w+)\, (\w+)", r"\1/\2", regex=True)
        .str.replace(r"october (.+)", r"10/\1", regex=True)
        .str.replace(r"december (.+)", r"12/\1", regex=True)
        .str.replace(r"august (.+)", r"08/\1", regex=True)
        .str.replace(r"june (.+)", r"06/\1", regex=True)
        .str.replace(r"may (.+)", r"05/\1", regex=True)
        .str.replace(r"april (.+)", r"04/\1", regex=True)
        .str.replace(r"march (.+)", r"03/\1", regex=True)
        .str.replace(r"february (.+)", r"02/\1", regex=True)
    )
    return df.drop(columns=["notification_date"])


def drop_rows_missing_letter_subjects(df):
    return df[~((df.letter_subject.fillna("") == ""))]


def join_multiple_extracted_entity_cols(df):
    allegation_cols = [
        "allegation",
        "allegation_1",
        "allegation_2",
        "allegation_3",
        "allegation_4",
        "allegation_5",
        "allegation_6",
        "allegation_7",
    ]

    df["allegation"] = df[allegation_cols].apply(
        lambda row: " ".join(row.values.astype(str)), axis=1
    )
    allegation_df = df[["allegation", "md5"]]
    allegation_df.loc[:, "allegation"] = allegation_df.allegation.str.replace(
        r" ?nan ?", "", regex=True
    )
    allegation_df = allegation_df[~((allegation_df.allegation.fillna("") == ""))]
    allegation_df = allegation_df.drop_duplicates(subset=["allegation", "md5"])

    tracking_id_df = df[["tracking_id", "md5"]]
    tracking_id_df = tracking_id_df[~((tracking_id_df.tracking_id.fillna("") == ""))]

    subject_df = df[["report_subject", "md5"]]
    subject_df = subject_df[~((subject_df.report_subject.fillna("") == ""))]

    df = df[
        [
            "report_date",
            "md5",
            "filepath",
            "filesha1",
            "fileid",
            "filetype",
            "fn",
            "file_category",
            "text",
            "pageno",
        ]
    ]

    data_frames = [df, tracking_id_df, allegation_df, subject_df]

    df = reduce(
        lambda left, right: pd.merge(left, right, on=["md5"], how="outer"), data_frames
    )
    df = df[
        [
            "report_date",
            "md5",
            "filepath",
            "filesha1",
            "fileid",
            "filetype",
            "fn",
            "file_category",
            "text",
            "pageno",
            "tracking_id",
            "allegation",
            "report_subject",
        ]
    ]
    return df[~((df.report_date.fillna("") == ""))].reset_index(drop=True)


def extract_and_split_names_2020(df):
    names = (
        df.fn.str.replace(r"(\[|\'|\.|pdf|_investigative.+)", " ", regex=True)
        .str.replace(r"_", " ", regex=True)
        .str.replace(r"( +$|^ +)", "", regex=True)
        .str.extract(r"(\w+) ?\b(\w)?\b (\w+)")
    )

    df.loc[:, "last_name"] = names[0]
    df.loc[:, "middle_name"] = names[1].fillna("")
    df.loc[:, "first_name"] = names[2]
    return df


def clean_report_subject(df):
    df.loc[:, "report_subject"] = (
        df.report_subject.str.lower()
        .str.strip()
        .str.replace(r"\n.+", "", regex=True)
        .str.replace(r"(re:|su[rb]ject:) ", "", regex=True)
        .str.replace(r"re:\n\n", "", regex=True)
    )
    return df


def extract_allegation_and_disposition(df):
    alls = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"\n", " ", regex=True)
        .str.replace(
            r"shoulder \‘weapons and rifles\, car carry mode",
            "weapons and rifles; car carry mode",
        )
        .str.extract(
            r"\,? (use of firearms|unsatisfactory performance|"
            r"conduct unbecoming an officer|false statements|conformance tu laws|"
            r"carrying and stor(age|ing) of firearms|false statements|"
            r"lawful orders|reporting for duty|"
            r"department vehicles|use of department equiqment|"
            r"|involvement in altercations|"
            r"cooperation with other agencies|weapons and rifles; car carry mode|"
            r"body worn camera & in-car camera systems - activation|courtesy|"
            r"performance of duty|body worn cameras- pre-operational procedures|"
            r"body worn cameras- responsibilities|"
            r"body worn cameras- activation)\,? (was|is) (sustained)"
        )
    )

    df.loc[:, "allegation"] = (
        alls[0]
        .str.replace(r"\btu\b", "to", regex=True)
        .str.replace(r"(\w+)- (\w+)", r"\1 - \2", regex=True)
        .str.replace(r"&", "and", regex=False)
    )
    df.loc[:, "disposition"] = alls[3]
    return df[~((df.allegation.fillna("") == ""))]


def sanitize_dates_2020(df):
    df.loc[:, "report_date"] = (
        df.report_date.str.lower()
        .str.strip()
        .str.replace(r"(\w+)\, ?(\w+)", r"\1/\2", regex=True)
        .str.replace(r"december (.+)", r"12/\1", regex=True)
        .str.replace(r"november (.+)", r"11/\1", regex=True)
        .str.replace(r"october (.+)", r"10/\1", regex=True)
        .str.replace(r"september (.+)", r"9/\1", regex=True)
        .str.replace(r"august (.+)", r"8/\1", regex=True)
        .str.replace(r"july (.+)", r"7/\1", regex=True)
        .str.replace(r"june (.+)", r"6/\1", regex=True)
        .str.replace(r"may (.+)", r"5/\1", regex=True)
        .str.replace(r"april (.+)", r"4/\1", regex=True)
        .str.replace(r"march (.+)", r"3/\1", regex=True)
        .str.replace(r"february (.+)", r"2/\1", regex=True)
        .str.replace(r"january (.+)", r"1/\1", regex=True)
        .str.replace(r"(.+)?([a-z]|\.|previous)(.+)?", "", regex=True)
    )
    return df[~((df.report_date.fillna("") == ""))]


def clean_tracking_id(df):
    df.loc[:, "tracking_id"] = (
        df.tracking_id.fillna("")
        .str.lower()
        .str.strip()
        .str.replace(r"\n", "", regex=True)
        .str.replace(r"\'", "", regex=True)
        .str.replace(r"case ?#?:? ?n?", "", regex=True)
    )
    return df


def drop_rows_missing_names(df):
    return df[~((df.last_name.fillna("") == ""))]


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


def concat_text_from_all_pages(df):
    text = df.groupby("md5")["text"].apply(" ".join).reset_index()
    df = df.drop(columns=["text"])
    df = pd.merge(df, text, on="md5", how="outer")
    return df


def generate_doc_date(df):
    df.loc[:, "doc_date"] = df.letter_date
    return df


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def format_titles_2019(df):
    df.loc[:, "title"] = (
        df.title.fillna("")
        + ":"
        + df.first_name.fillna("")
        + " "
        + df.last_name.fillna("")
        + " on "
        + df.letter_date
    )
    df.loc[:, "title"] = df.title.str.replace(
        r"(.+):(.+)", r"\1 Notice: \2", regex=True
    ).str.replace(r"\/", "-", regex=True)
    return df.pipe(names_to_title_case, ["title"])


def format_titles_2020(df):
    df.loc[:, "title"] = df.report_subject
    df.loc[:, "title"] = (
        df.title.fillna("")
        + ":"
        + df.first_name.fillna("")
        + " "
        + df.last_name.fillna("")
        + df.middle_name.fillna("")
        + " on "
        + df.report_date
    )
    df.loc[:, "title"] = df.title.str.replace(
        r"(.+):(.+)", r"\1 investigative report: \2", regex=True
    ).str.replace(r"\/", "-", regex=True)
    return df.pipe(names_to_title_case, ["title"])


def clean_letters_2019():
    db_meta = pd.read_csv(
        deba.data("raw/louisiana_state_pd/letters_louisiana_state_pd_2019_db_files.csv")
    ).pipe(extract_db_meta)

    df = (
        pd.read_csv(deba.data("ner/letters_louisiana_state_pd_2019.csv")).pipe(
            clean_column_names
        )
        # .pipe(concat_text_from_all_pages)
        # .pipe(drop_rows_missing_dates)
        # .pipe(extract_ids_and_subject)
        # .pipe(split_names)
        # .pipe(sanitize_letter_dates)
        # .pipe(drop_rows_missing_letter_subjects)
        # .pipe(clean_names, ["first_name", "last_name"])
        # .pipe(set_values, {"agency": "louisiana-state-pd"})
        # .pipe(gen_uid, ["first_name", "last_name", "agency"])
        # .pipe(gen_uid, ["letter_subject", "uid", "letter_date"], "allegation_uid")
        # .pipe(clean_fn)
    )
    # df = pd.merge(df, db_meta, on="fn", how="outer")
    # df = (
    #     df.rename(columns={"md5": "docid"})
    #     .pipe(generate_doc_date)
    #     .pipe(clean_dates, ["doc_date"])
    # )
    return df


def clean_reports_2020():
    df = (
        pd.read_csv(deba.data("ner/reports_louisiana_state_pd_2020.csv")).pipe(
            clean_column_names
        )
        # .drop(
        #     columns=[
        #         "previous_discipline",
        #         "previous_discipline_1",
        #         "previous_discipline_2",
        #         "previous_discipline_3",
        #         "previous_discipline_4",
        #         "previous_discipline_5",
        #         "previous_discipline_6",
        #     ]
        # )
        # .pipe(join_multiple_extracted_entity_cols)
        # .pipe(sanitize_dates_2020)
        # .pipe(clean_dates, ["report_date"])
        # .pipe(extract_and_split_names_2020)
        # .pipe(clean_report_subject)
        # .pipe(extract_allegation_and_disposition)
        # .pipe(clean_tracking_id)
        # .pipe(clean_names, ["first_name", "last_name"])
        # .pipe(set_values, {"agency": "louisiana-state-pd"})
        # .pipe(gen_uid, ["first_name", "last_name", "agency"])
        # .pipe(
        #     gen_uid,
        #     ["allegation", "uid", "report_year", "report_month", "report_day"],
        #     "allegation_uid",
        # )
        # .drop_duplicates(subset=["allegation_uid"])
        # .pipe(drop_rows_missing_names)
        # .pipe(create_tracking_id_og_col)
        # .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


if __name__ == "__main__":
    df19 = clean_letters_2019()
    df20 = clean_reports_2020()
    df19.to_csv(deba.data("clean/cprr_louisiana_state_pd_2019.csv"), index=False)
    df20.to_csv(deba.data("clean/cprr_louisiana_state_pd_2020.csv"), index=False)
