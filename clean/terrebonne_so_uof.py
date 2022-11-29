import pandas as pd
import deba
from lib.clean import (
    clean_datetimes,
    standardize_desc_cols,
    clean_races,
    clean_sexes,
    clean_dates,
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
import re
from lib.rows import duplicate_row


def split_datetime_col(df):
    data = df.occurred_datetime.str.extract(r"(\w{1,2}\/\w{1,2}\/\w{4})\b ?(.+)?")
    df.loc[:, "occurred_date"] = data[0]
    df.loc[:, "occurred_time"] = data[1]
    return df.drop(columns=["occurred_datetime"])


def clean_call_type(df):
    df.loc[:, "call_type"] = (
        df.call_type.str.lower()
        .str.strip()
        .str.replace(r"w\/(\w+)", r"with \1", regex=True)
        .str.replace(r"(\w+):(\w+)", r"\1: \2", regex=True)
        .str.replace(r"poss", "possession", regex=False)
        .str.replace(r"att\.", "attempt", regex=True)
        .str.replace(r"(\w)cts", r"\1 counts", regex=True)
        .str.replace(r"\,", r"/", regex=True)
        .str.replace(r"(\w+) ?\/ ?(\w+)", r"\1/\2", regex=True)
        .str.replace(r"^(\w+) (\w+):", r"\1\2:", regex=True)
    )

    return df


def clean_uof_type(df):
    df.loc[:, "use_of_force_type"] = (
        df.use_of_force_type.str.lower()
        .str.strip()
        .str.replace(r"\, ", "/", regex=True)
    )
    return df


def clean_uof_description(df):
    df.loc[:, "use_of_force_result"] = (
        df.injury_s.str.lower()
        .str.strip()
        .str.replace(r"yes \(crash related\)", "crashed related injury", regex=True)
        .str.replace(r"none", "", regex=False)
    )
    return df.drop(columns=["injury_s"])


def join_officer_rows(df):
    df.loc[:, "officer_name"] = df.officer_s_involved.fillna("").str.cat(
        df.officer_name.fillna(""), sep="/"
    )
    return df


def split_rows_with_multiple_officers(df):
    df.loc[:, "officer_name"] = (
        df.officer_name.str.lower().str.strip().str.replace(r"\, ", "/", regex=True)
    )
    i = 0
    for idx in df[df.officer_name.fillna("").str.contains("/")].index:
        s = df.loc[idx + i, "officer_name"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer_name"] = name
        i += len(parts) - 1
    return df


def split_names(df):
    names = df.officer_name.str.extract(r"(dy|lt|cpl|agent)?\.? ?(\w+) (.+)")
    df.loc[:, "rank_desc"] = (
        names[0]
        .str.replace(r"dy", "deputy", regex=False)
        .str.replace(r"lt", "lieutenant", regex=False)
        .str.replace(r"cpl", "corporal", regex=False)
    )
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns=["officer_name", "officer_s_involved"])[
        ~((df.last_name.fillna("") == ""))
    ]


def split_rows_with_multiple_citizens(df):
    df.loc[:, "citizen_race"] = (
        df.suspect_race.str.lower().str.strip().str.replace(r"\, ", "/", regex=True)
    )
    i = 0
    for idx in df[df.citizen_race.fillna("").str.contains("/")].index:
        s = df.loc[idx + i, "citizen_race"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "citizen_race"] = name
        i += len(parts) - 1

    df.loc[:, "citizen_sex"] = (
        df.suspect_gender.str.lower().str.strip().str.replace(r"\, ", "/", regex=True)
    )
    i = 0
    for idx in df[df.citizen_sex.fillna("").str.contains("/")].index:
        s = df.loc[idx + i, "citizen_sex"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "citizen_sex"] = name
        i += len(parts) - 1

    df.loc[:, "citizen_age"] = (
        df.suspect_age.str.lower().str.strip().str.replace(r"\, ", "/", regex=True)
    )
    i = 0
    for idx in df[df.citizen_age.fillna("").str.contains("/")].index:
        s = df.loc[idx + i, "citizen_age"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "citizen_age"] = name
        i += len(parts) - 1
    return (
        df.drop(columns=["suspect_race", "suspect_gender", "suspect_age"])
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_races, ["citizen_race"])
    )


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/terrebonne_so/terrebonne_so_uof_2021.csv"))
        .pipe(clean_column_names)
        .drop(
            columns=[
                "suspect_name",
                "officer_race",
                "officer_gender",
                "officer_age",
                "officer_length_of_service",
            ]
        )
        .rename(
            columns={
                "date_time_of_incident": "occurred_datetime",
                "location_of_incident": "incident_location",
                "case_number": "tracking_id",
                "case_status": "status",
            }
        )
        .pipe(split_datetime_col)
        .pipe(clean_call_type)
        .pipe(clean_uof_type)
        .pipe(clean_uof_description)
        .pipe(join_officer_rows)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_rows_with_multiple_citizens)
        .pipe(split_names)
        .pipe(standardize_desc_cols, ["incident_location", "tracking_id", "status"])
        .pipe(set_values, {"agency": "terrebonne-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "incident_location",
                "tracking_id",
                "call_type",
                "status",
                "use_of_force_type",
                "occurred_date",
                "use_of_force_result",
            ],
            "uof_uid",
        )
        .pipe(
            gen_uid,
            ["citizen_race", "citizen_sex", "citizen_age"],
            "uof_citizen_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    df = df.drop_duplicates(subset=["uid", "uof_uid"])
    dfa = df[
        [
            "incident_location",
            "tracking_id",
            "status",
            "call_type",
            "use_of_force_type",
            "occurred_date",
            "occurred_time",
            "use_of_force_result",
            "rank_desc",
            "first_name",
            "last_name",
            "agency",
            "uid",
            "uof_uid",
        ]
    ]
    dfb = df[["uof_uid", "citizen_age", "citizen_sex", "citizen_race"]]
    return dfa, dfb


if __name__ == "__main__":
    uof, citizen_uof = clean()
    uof.to_csv(deba.data("clean/uof_terrebonne_so_2021.csv"), index=False)
    citizen_uof.to_csv(
        deba.data("clean/uof_citizens_terrebonne_so_2021.csv"), index=False
    )
