import pandas as pd
import deba
from lib.clean import (
    clean_dates,
    clean_races,
    clean_sexes,
    standardize_desc_cols,
    strip_leading_comma,
)
from lib.columns import set_values
from lib.uid import gen_uid


def extract_date(df):
    df.loc[:, "occurred_date"] = (
        df["date"]
        .str.strip()
        .str.extract(r"(\d{1,2}/\d{1,2}/\d{4})\s*$")[0]
    )
    df.loc[:, "occurred_time"] = (
        df["time"]
        .str.strip()
        .str.extract(r"(\d{1,2}:\d{2})")[0]
        .str.zfill(5)
    )
    return df.drop(columns=["date", "time"])


def clean_call_type(df):
    df.loc[:, "call_type"] = (
        df.call_type.str.lower()
        .str.strip()
        .str.replace(r"w\/", "with ", regex=True)
        .str.replace(r"\.", "", regex=True)
    )
    return df


def clean_uof_type(df):
    df.loc[:, "use_of_force_type"] = (
        df.use_of_force_type.str.lower()
        .str.strip()
        .str.replace(r"\s*-\s*", "-", regex=True)
    )
    return df


def clean_injuries(df):
    df.loc[:, "use_of_force_result"] = (
        df.injuries.str.lower()
        .str.strip()
        .str.replace(r"^none$", "", regex=True)
        .str.replace(r"ofc injured", "officer injured", regex=False)
        .str.replace(r"^yes$", "injured", regex=True)
    )
    return df.drop(columns=["injuries"])


def clean_disciplinary(df):
    df.loc[:, "disciplinary_actions"] = (
        df.disciplinary_actions.str.lower()
        .str.strip()
        .str.replace(r"^none necessary$", "", regex=True)
    )
    return df


def clean_officer_name(df):
    df.loc[:, "last_name"] = df["last_name"].str.lower().str.strip()
    df.loc[:, "first_name"] = ""
    return df


def parse_subject_name(df):
    parts = df["subject_involved"].str.strip().str.extract(
        r"^([A-Za-z])\.\s*(.+)$"
    )
    df.loc[:, "citizen_first_name"] = parts[0].str.lower().str.strip()
    df.loc[:, "citizen_last_name"] = parts[1].str.lower().str.strip()
    return df.drop(columns=["subject_involved"])


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean():
    df = pd.read_csv(
        deba.data("raw/covington_pd/covington_pd_uof_22_25.csv"), header=0
    )
    df.columns = [
        "date", "time", "location", "last_name", "badge_no", "rank_desc",
        "officer_race", "officer_gender", "officer_age_range", "years_of_service",
        "item_number", "status", "call_type", "subject_involved",
        "citizen_age", "citizen_sex", "citizen_race",
        "use_of_force_type", "injuries", "medical_assistance", "disciplinary_actions",
        "empty",
    ]
    df = df.drop(columns=["empty"])
    df = (
        df.pipe(strip_leading_comma)
        .rename(columns={"item_number": "tracking_id", "location": "incident_location"})
        .drop(columns=["medical_assistance", "officer_race", "officer_gender"])
        .pipe(extract_date)
        .pipe(clean_dates, ["occurred_date"])
        .pipe(clean_call_type)
        .pipe(clean_uof_type)
        .pipe(clean_injuries)
        .pipe(clean_disciplinary)
        .pipe(clean_officer_name)
        .pipe(parse_subject_name)
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(standardize_desc_cols, ["incident_location", "tracking_id", "status", "rank_desc", "badge_no", "officer_age_range", "years_of_service"])
        .pipe(set_values, {"agency": "covington-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "tracking_id", "use_of_force_type", "occurred_year", "occurred_month", "occurred_day", "call_type"],
            "uof_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )

    citizen_df = df[
        ["uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"]
    ].copy().pipe(
        gen_uid,
        ["citizen_age", "citizen_sex", "citizen_race", "agency"],
        "citizen_uid",
    )

    uof_df = df[
        [
            "tracking_id",
            "tracking_id_og",
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "occurred_time",
            "incident_location",
            "status",
            "call_type",
            "use_of_force_type",
            "use_of_force_result",
            "disciplinary_actions",
            "rank_desc",
            "badge_no",
            "officer_age_range",
            "years_of_service",
            "first_name",
            "last_name",
            "agency",
            "uid",
            "uof_uid",
        ]
    ].drop_duplicates(subset=["uid", "uof_uid"])

    return uof_df, citizen_df


if __name__ == "__main__":
    uof, citizen_uof = clean()
    uof.to_csv(deba.data("clean/uof_covington_pd_2022_2025.csv"), index=False)
    citizen_uof.to_csv(deba.data("clean/uof_cit_covington_pd_2022_2025.csv"), index=False)
