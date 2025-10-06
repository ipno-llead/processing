import pandas as pd
import deba
from lib.clean import (
    clean_datetimes,
    standardize_desc_cols,
    clean_races,
    clean_sexes,
    clean_dates,
    strip_leading_comma, 
    clean_names
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
import re
from lib.rows import duplicate_row


def split_occurred_date(df):
    dates = df.occurred_date.str.extract(r"(\d+)/(\d+)/(\d+)")
    df.loc[:, "occurred_month"] = dates[0]
    df.loc[:, "occurred_day"] = dates[1]
    df.loc[:, "occurred_year"] = dates[2]
    df.drop(columns=["occurred_date"], inplace=True)
    return df


def clean_citizen_armed_with(df):
    df.loc[:, "citizen_armed_with"] = (
        df.citizen_armed_with.str.lower()
        .str.strip()
        .fillna("")
        .replace(["other", "unknown"], "")
    )
    return df


def clean_use_of_force_result(df):
    df.loc[:, "use_of_force_result"] = (
        df.use_of_force_result.str.lower()
        .replace({"yes": "arrested", "no": "not arrested"})
    )

    # Merge arrested_for charges into use_of_force_result
    arrest_charges = df.arrested_for.fillna("").str.strip()
    arrested_mask = df.use_of_force_result == "arrested"
    has_charges = arrest_charges != ""

    df.loc[arrested_mask & has_charges, "use_of_force_result"] = (
        "arrested; " + arrest_charges[arrested_mask & has_charges].str.lower()
    )
    df.drop(columns=["arrested_for"], inplace=True)
    return df


def clean_citizen_injured(df):
    df.loc[:, "citizen_injured"] = (
        df.citizen_injured.str.lower()
        .replace({"yes": "injured", "no": "not injured"})
    )
    return df


def split_officers(df):
    # Split officer names by semicolon
    df.loc[:, "officer_names"] = df.officer_names.fillna("").str.strip()

    # Expand rows for multiple officers
    df.loc[:, "officer_names"] = df.officer_names.str.split(";")
    df = df.explode("officer_names").reset_index(drop=True)

    df.loc[:, "officer_names"] = df.officer_names.str.strip()

    name_parts = df.officer_names.str.extract(r'^([^,]+),\s*(.+)$')

    df.loc[:, "last_name"] = name_parts[0].str.strip()

    first_middle = name_parts[1].str.strip().fillna("")
    first_middle_split = first_middle.str.split(r'\s+', n=1, expand=True)

    df.loc[:, "first_name"] = first_middle_split[0].fillna("")
    df.loc[:, "middle_name"] = first_middle_split[1].fillna("") if 1 in first_middle_split.columns else ""

    df = df.drop(columns=["officer_names"])

    return df


def calculate_citizen_age(df):
    # Convert occurred date to datetime
    occurred_date = pd.to_datetime(
        df.occurred_year + "-" + df.occurred_month + "-" + df.occurred_day,
        errors="coerce"
    )

    # Parse citizen_dob, treating "Juvenile" as invalid
    dob = pd.to_datetime(df.citizen_dob.replace("Juvenile", pd.NA), errors="coerce")

    # Calculate age
    age = (occurred_date - dob).dt.days / 365.25

    # Create citizen_age column as string
    df.loc[:, "citizen_age"] = age.round(0).astype(str)

    # Replace 'nan' with empty string
    df.loc[:, "citizen_age"] = df.citizen_age.replace("nan", "")

    df = df.drop(columns=["citizen_dob"])

    return df


def extract_justification(df):
    notes = df.notes.fillna("").str.lower()

    # Check for justified language
    justified_mask = (
        notes.str.contains("justified", na=False) |
        notes.str.contains("proper", na=False) |
        notes.str.contains("within policy", na=False) |
        notes.str.contains("within the scope", na=False) |
        notes.str.contains("appropriate", na=False) |
        notes.str.contains("lawful", na=False) |
        notes.str.contains("reasonable", na=False) |
        notes.str.contains("according to policy", na=False)
    )

    df.loc[:, "use_of_force_justified"] = ""
    df.loc[justified_mask, "use_of_force_justified"] = "justified"

    df = df.drop(columns=["notes"])

    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/slidell_pd/slidell_pd_uof_2021_2025.csv"))
        .pipe(clean_column_names)
        .drop(
            columns=[
                "page_number",
                "time",
            ]
        )
        .rename(
            columns={
                "case_number": "tracking_id_og",
                "type_situation": "call_type",
                "reason_force_used": "use_of_force_reason",
                "final_disposition": "disposition",
                "date": "occurred_date",
                "danger_factors": "risk_factors",
                "camera": "camera_type",
                "subject_dob": "citizen_dob", 
                "subject_gender": "citizen_sex",
                "subject_race": "citizen_race", 
                "subject_armed_with": "citizen_armed_with",
                "force_used": "use_of_force_type",
                "arrested": "use_of_force_result",
                "injured": "citizen_injured",
                "officer_actions": "use_of_force_description", 
            }
        )
        .pipe(split_occurred_date)
        .pipe(standardize_desc_cols, ["call_type", "use_of_force_description", "level_of_resistance", "use_of_force_reason", "disposition", "use_of_force_type", "recorded_on_camera", "camera_type", "risk_factors" ])
        .pipe(clean_citizen_armed_with)
        .pipe(clean_use_of_force_result)
        .pipe(clean_citizen_injured)
        .pipe(split_officers)
        .pipe(calculate_citizen_age)
        .pipe(extract_justification)
        .pipe(clean_races, ["citizen_race"])
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "slidell-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id_og",
                "call_type",
                "use_of_force_type",
                "occurred_year",
                "occurred_month",
                "occurred_day",
                "use_of_force_result",
            ],
            "uof_uid",
        )
        .pipe(
            gen_uid,
            ["citizen_race", "citizen_sex", "citizen_age"],
            "uof_citizen_uid",
        )
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
    )

    uof = df[[
        "tracking_id",
        "tracking_id_og",
        "call_type",
        "use_of_force_type",
        "use_of_force_reason",
        "use_of_force_description",
        "use_of_force_justified",
        "disposition",
        "occurred_year",
        "occurred_month",
        "occurred_day",
        "recorded_on_camera",
        "camera_type",
        "risk_factors",
        "level_of_resistance",
        "first_name",
        "middle_name",
        "last_name",
        "agency",
        "uid",
        "uof_uid",
    ]].drop_duplicates(subset=["uid", "uof_uid"])

    citizen_uof = df[[
        "uof_uid",
        "citizen_race",
        "citizen_sex",
        "citizen_age",
        "citizen_injured",
        "citizen_armed_with",
        "use_of_force_result",
        "agency",
        "uof_citizen_uid",
    ]].drop_duplicates(subset=["uof_citizen_uid"])

    return uof, citizen_uof


if __name__ == "__main__":
    uof, citizen_uof = clean()
    uof.to_csv(deba.data("clean/uof_slidell_pd_2021_2025.csv"), index=False)
    citizen_uof.to_csv(deba.data("clean/uof_cit_slidell_pd_2021_2025.csv"), index=False)
