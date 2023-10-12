import pandas as pd
import deba
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values
from lib.clean import (
    strip_leading_comma,
    clean_dates,
    clean_races,
    clean_sexes,
    standardize_desc_cols,
)


def clean_medical_treatment(df):
    df.loc[:, "medical_treatment"] = (
        df.medical_treatment.str.lower()
        .str.strip()
        .str.replace(r"none(.+)?", "no", regex=True)
        .str.replace(r"^dpy\. ", "yes", regex=True)
        .str.replace(r"n\/a", "", regex=True)
        .str.replace(r"yesyes", "yes", regex=False)
    )
    return df


def clean_use_of_force_description(df):
    df.loc[:, "use_of_force_description"] = (
        df.type_of_force.str.lower()
        .str.strip()
        .str.replace(r"(\w+) \/ (\w+)", r"\1/\2", regex=True)
    )
    return df.drop(columns=["type_of_force"])


def clean_uof_reason(df):
    df.loc[:, "use_of_force_reason"] = (
        df.incident_type.str.lower()
        .str.strip()
        .str.replace(
            r"^dist\. ?w ?\/ ?weapons$", "disturbance with weapons", regex=True
        )
        .str.replace(r"domestic dist\.$", "domestic disturbance", regex=True)
        .str.replace(r"invest\.$", "investigation", regex=True)
    )
    return df.drop(columns=["incident_type"])


def clean_tracking_id_og(df):
    df.loc[:, "tracking_id_og"] = (
        df.uof.str.lower()
        .str.strip()
        .str.replace(r"^uu?o?f-? ?(.+)", r"\1", regex=True)
    )
    return df.drop(columns=["uof"])


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.justified.str.lower()
        .str.strip()
        .str.replace(r"yes", "use of force justified", regex=False)
        .str.replace(r"no", "use of force not justified", regex=False)
    )
    return df.drop(columns=["justified"])


def split_rows_with_multiple_officers(df):
    df = (
        df.drop("name_of_deputy", axis=1)
        .join(
            df["name_of_deputy"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("name_of_deputy"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_names(df):
    names = df.name_of_deputy.str.lower().str.strip().str.extract(r"^(\w+) (.+)")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns=["name_of_deputy"])


def clean_uof():
    df = (
        pd.read_csv(deba.data("raw/lafayette_so/lafayette_so_uof_2015_2019.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
        .drop(columns=["injury_deputy_suspect"])
        .rename(
            columns={
                "date": "occur_date",
                "race": "citizen_race",
                "gender": "citizen_sex",
                "age": "citizen_age",
            }
        )
        .pipe(clean_dates, ["occur_date"])
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(clean_tracking_id_og)
        .pipe(clean_medical_treatment)
        .pipe(clean_uof_reason)
        .pipe(clean_use_of_force_description)
        .pipe(clean_disposition)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_names)
        .pipe(
            standardize_desc_cols,
            ["use_of_force_description", "use_of_force_reason", "location", "district"],
        )
        .pipe(set_values, {"agency": "lafayette-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["agency", "tracking_id_og"], "tracking_id")
        .pipe(
            gen_uid,
            [
                "use_of_force_reason",
                "use_of_force_description",
                "disposition",
                "location",
                "occur_year",
                "occur_month",
                "uid",
                "tracking_id",
            ],
            "uof_uid",
        )
        .pipe(
            gen_uid,
            [
                "citizen_race",
                "citizen_sex",
                "citizen_age",
                "use_of_force_reason",
                "use_of_force_reason",
                "tracking_id",
            ],
            "citizen_uid",
        )
    )
    return df


def extract_citizen(uof):
    citizen_df = uof.loc[
        :,
        [
            "citizen_race",
            "citizen_sex",
            "citizen_age",
            "citizen_uid",
            "agency",
        ],
    ]
    uof = uof.drop(
        columns=[
            "citizen_race",
            "citizen_sex",
            "citizen_age",
            "citizen_uid",
        ]
    )
    return citizen_df, uof


if __name__ == "__main__":
    uof = clean_uof()
    citizen_df, uof = extract_citizen(uof)
    citizen_df.to_csv(
        deba.data("clean/uof_citizens_lafayette_so_2015_2019.csv"), index=False
    )
    uof.to_csv(deba.data("clean/uof_lafayette_so_2015_2019.csv"), index=False)
