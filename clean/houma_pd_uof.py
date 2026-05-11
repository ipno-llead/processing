import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid


def strip_leading_apostrophes(df):
    df.columns = [c.lstrip("'").strip() for c in df.columns]
    for col in df.columns:
        if df[col].dtype == object:
            df.loc[:, col] = df[col].str.lstrip("'").str.strip()
    return df


def split_officer_name(df):
    names = df.primary_officer.str.extract(r"(.+),\s*(.+)")
    df.loc[:, "last_name"] = names[0].str.strip()
    df.loc[:, "first_name"] = names[1].str.strip()
    return df.drop(columns=["primary_officer"])


def split_incident_datetime(df):
    parts = df.incident_date.str.extract(r"(\d{1,2}/\d{1,2}/\d{2})\s+(\d{2}:\d{2})")
    df.loc[:, "occurred_date"] = parts[0]
    df.loc[:, "occurred_time"] = parts[1]
    return df.drop(columns=["incident_date"])


def clean_incident_code(df):
    df.loc[:, "use_of_force_description"] = df.incident_code.str.lower().str.strip()
    return df.drop(columns=["incident_code"])


def clean_tracking_id(df):
    df = df.rename(columns={"case": "tracking_id"})
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/houma_pd/houma_pd_uof_2020.csv"))
        .pipe(strip_leading_apostrophes)
        .pipe(clean_column_names)
        .drop_duplicates()
        .pipe(clean_tracking_id)
        .pipe(split_officer_name)
        .pipe(split_incident_datetime)
        .pipe(clean_incident_code)
        .rename(columns={"location": "use_of_force_location"})
        .drop(columns=["investigator"])
        .pipe(standardize_desc_cols, ["tracking_id", "use_of_force_location", "first_name", "last_name"])
        .pipe(set_values, {"agency": "houma-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"], "uid")
        .pipe(
            gen_uid,
            ["uid", "tracking_id", "use_of_force_description", "occurred_date"],
            "uof_uid",
        )
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


if __name__ == "__main__":
    uof = clean()
    uof.to_csv(deba.data("clean/uof_houma_pd_2020.csv"), index=False)
