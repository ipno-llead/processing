import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names
from lib.uid import gen_uid
from functools import reduce


def drop_rows_missing_dates(df):
    return df[~((df.notification_date.fillna("") == ""))]


def extract_ids_and_subject(df):
    cols = [
        "accused_name_2",
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
    )

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
            "accused_name_2",
            "tracking_id_1",
            "tracking_id_2",
            "tracking_id_3",
            "data",
            "subject_of_letter",
        ]
    )


def split_names(df):
    names = (
        df.accused_name_1.str.lower()
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
    return df.drop(columns=["accused_name_1"])


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
    df = df[~((df.report_date.fillna("") == ""))]
    return df


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
        .str.replace(r"october (.+)", r"10/\1", regex=True)
        .str.replace(r"september (.+)", r"10/\1", regex=True)
        .str.replace(r"august (.+)", r"08/\1", regex=True)
        .str.replace(r"june (.+)", r"06/\1", regex=True)
        .str.replace(r"may (.+)", r"05/\1", regex=True)
        .str.replace(r"april (.+)", r"04/\1", regex=True)
        .str.replace(r"march (.+)", r"03/\1", regex=True)
        .str.replace(r"february (.+)", r"02/\1", regex=True)
        .str.replace(r"january (.+)", r"02/\1", regex=True)
    )
    return df


def clean_letters_2019():
    df = (
        pd.read_csv(deba.data("ner/letters_louisiana_state_pd_2019.csv"))
        .pipe(clean_column_names)
        .pipe(drop_rows_missing_dates)
        .pipe(extract_ids_and_subject)
        .pipe(split_names)
        .pipe(sanitize_letter_dates)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["letter_subject", "uid", "letter_date"], "allegation_uid")
    )
    return df


def clean_reports_2020():
    df = (
        pd.read_csv(deba.data("ner/reports_louisiana_state_pd_2020.csv"))
        .pipe(clean_column_names)
        .drop(
            columns=[
                "previous_discipline",
                "previous_discipline_1",
                "previous_discipline_2",
                "previous_discipline_3",
                "previous_discipline_4",
                "previous_discipline_5",
                "previous_discipline_6",
            ]
        )
        .pipe(join_multiple_extracted_entity_cols)
        .pipe(extract_and_split_names_2020)
        .pipe(clean_report_subject)
        .pipe(extract_allegation_and_disposition)
        .pipe(sanitize_dates_2020)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "uid", "report_date"], "allegation_uid")
        .drop_duplicates(subset=["allegation_uid"])
    )
    return df


if __name__ == "__main__":
    df19 = clean_letters_2019()
    df20 = clean_reports_2020()
    df19.to_csv(deba.data("clean/cprr_louisiana_state_pd_2019.csv"), index=False)
    df20.to_csv(deba.data("clean/cprr_louisiana_state_pd_2020.csv"), index=False)
