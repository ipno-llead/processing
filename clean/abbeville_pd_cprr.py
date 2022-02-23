import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates
from lib.uid import gen_uid
import re
from lib.rows import duplicate_row


def clean_tracking_number(df):
    df.loc[:, "tracking_number"] = (
        df.ia_case_number.str.lower()
        .str.strip()
        .str.replace("ia case number", "", regex=False)
    )
    return df.drop(columns="ia_case_number")


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace("allegation", "", regex=False)
        .str.replace(
            r"gen\. order 121 - unit usage", "general order 121: unit usage", regex=True
        )
        .str.replace(
            "107 - misuse of equipment",
            "general order 107: misuse of equipment",
            regex=False,
        )
        .str.replace(r"^unauth\.", "unauthorized", regex=True)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"non\- ?sustained", "not sustained", regex=True)
        .str.replace("disposition", "", regex=False)
        .str.replace(r"\?", "unknown", regex=True)
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = (
        df.discipline.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("none", "", regex=False)
        .str.replace("discipline", "", regex=False)
        .str.replace(r"(\w{1}) days?", r"\1-day", regex=True)
        .str.replace(r"^3-day$", "3-day suspension", regex=True)
        .str.replace(r"^\?$", "unknown", regex=True)
        .str.replace(r"^unk$", "unknown", regex=True)
    )
    return df.drop(columns="discipline")


def clean_allegation_desc(df):
    df.loc[:, "allegation_desc"] = (
        df.notes.str.lower()
        .str.strip()
        .str.replace(r"gen\.", "general", regex=True)
        .str.replace("notes", "", regex=False)
    )
    return df.drop(columns="notes")


def split_rows_with_multiple_officers(df):
    df.loc[:, "officer_name"] = (
        df.officer_name.str.lower().str.strip().str.replace(r" \/ ", "/", regex=True)
    )
    i = 0
    for idx in df[df.officer_name.str.contains("/")].index:
        s = df.loc[idx + i, "officer_name"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer_name"] = name
        i += len(parts) - 1
    return df


def split_and_clean_names(df):
    names = df.officer_name.str.replace("officer name", "", regex=False).str.extract(
        r"(\w+) ?(\w+)?\.? (\w+)$"
    )

    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "middle_name"] = names[1].fillna("")
    df.loc[:, "last_name"] = names[2].fillna("")
    return df.drop(columns="officer_name")


def remove_q_marks_from_dates(df):
    df.loc[:, "receive_date"] = df.receive_date.str.replace(r"\? ", "", regex=True)

    df.loc[
        :, "investigation_complete_date"
    ] = df.investigation_complete_date.str.replace(r"\? ", "", regex=True)
    return df


def remove_uid_for_unknown_officers(df):
    df.loc[((df.first_name == "") & (df.last_name == "")), "uid"] = ""
    return df[~((df.uid == ""))]


def clean_receive_and_complete_dates(df):
    df.loc[:, "receive_date"] = (
        df.date_received.str.lower()
        .str.strip()
        .str.replace("date received", "", regex=False)
    )

    df.loc[:, "investigation_complete_date"] = (
        df.date_completed.str.lower()
        .str.strip()
        .str.replace("date completed", "", regex=False)
    )
    return df.drop(columns=["date_received", "date_completed"])


def clean21():
    df = (
        pd.read_csv(deba.data("raw/abbeville_pd/abbeville_pd_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .pipe(clean_tracking_number)
        .pipe(clean_allegation)
        .pipe(clean_disposition)
        .pipe(clean_action)
        .pipe(clean_allegation_desc)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_and_clean_names)
        .pipe(clean_receive_and_complete_dates)
        .pipe(clean_dates, ["receive_date", "investigation_complete_date"])
        .pipe(set_values, {"agency": "Abbeville PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            [
                "uid",
                "allegation",
                "disposition",
                "action",
                "receive_year",
                "receive_day",
                "receive_month",
            ],
            "allegation_uid",
        )
        .pipe(remove_uid_for_unknown_officers)
    )
    return df


def clean18():
    df = (
        pd.read_csv(deba.data("raw/abbeville_pd/abbeville_pd_2015_2018.csv"))
        .pipe(clean_column_names)
        .drop(columns=["ia_case_number"])
        .rename(
            columns={
                "date_received": "receive_date",
                "date_completed": "investigation_complete_date",
            }
        )
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_and_clean_names)
        .pipe(clean_allegation)
        .pipe(clean_allegation_desc)
        .pipe(clean_disposition)
        .pipe(clean_action)
        .pipe(remove_q_marks_from_dates)
        .pipe(clean_dates, ["receive_date", "investigation_complete_date"])
        .pipe(set_values, {"agency": "Abbeville PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            [
                "uid",
                "allegation",
                "disposition",
                "action",
                "receive_year",
                "receive_day",
                "receive_month",
            ],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df21 = clean21()
    df18 = clean18()
    df21.to_csv(deba.data("clean/cprr_abbeville_pd_2019_2021.csv"), index=False)
    df18.to_csv(deba.data("clean/cprr_abbeville_pd_2015_2018.csv"), index=False)
