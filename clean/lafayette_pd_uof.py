import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_races, clean_sexes, clean_names
from lib.uid import gen_uid


def strip_leading_apostrophes(df):
    df.columns = [c.lstrip("'").strip() for c in df.columns]
    for col in df.columns:
        if df[col].dtype == object:
            df.loc[:, col] = df[col].str.lstrip("'").str.strip()
    return df


def split_overview_datetime(df):
    parts = df.event_date_time.str.extract(r"(\d{1,2}/\d{1,2}/\d{4})\s*(.*)")
    df.loc[:, "occurred_date"] = parts[0]
    df.loc[:, "occurred_time"] = parts[1]
    return df.drop(columns=["event_date_time"])


def clean_video_available(df):
    df.loc[:, "video_available"] = (
        df.video_available.str.lower()
        .str.strip()
        .str.replace(r"not_selected\s*", "", regex=True)
        .str.strip()
    )
    return df


def clean_firearm_involved(df):
    filled = df.firearm_involved.fillna("").str.strip()
    df.loc[:, "use_of_force_description"] = filled.apply(
        lambda x: "firearm involved" if x != "" else "firearm not involved"
    )
    return df.drop(columns=["firearm_involved"])


def clean_injuries_badge(df):
    """Handle rows where columns shifted: badge has 'badge first_name',
    first_name has last_name, last_name has officer_injured."""
    badge_has_name = df.badge.str.contains(r"\d+\s+\w+", na=False)
    parts = df.loc[badge_has_name, "badge"].str.extract(r"(\d+)\s+(.*)")
    df.loc[badge_has_name, "officer_injured"] = df.loc[
        badge_has_name, "last_name"
    ]
    df.loc[badge_has_name, "last_name"] = df.loc[badge_has_name, "first_name"]
    df.loc[badge_has_name, "first_name"] = parts[1]
    df.loc[badge_has_name, "badge"] = parts[0]
    return df


def strip_event_suffix(df):
    """Remove the -1 suffix from event numbers in the injuries file."""
    df.loc[:, "event_number"] = df.event_number.str.replace(
        r"-\d+$", "", regex=True
    )
    return df


def clean_officer_injured(df):
    df.loc[:, "officer_injured"] = (
        df.officer_injured.str.lower()
        .str.strip()
        .str.replace(r"^yes.*", "yes", regex=True)
        .str.replace("unknown", "", regex=False)
    )
    return df


def split_subject_datetime(df):
    parts = df.event_date_time.str.extract(r"(\d{1,2}/\d{1,2}/\d{4})\s*(.*)")
    df.loc[:, "occurred_date"] = parts[0]
    df.loc[:, "occurred_time"] = parts[1]
    return df.drop(columns=["event_date_time"])


def clean_citizen_armed_with(df):
    df.loc[:, "citizen_armed_with"] = (
        df.armed_with.fillna("")
        .str.lower()
        .str.strip()
        .replace({"other": "", "unknown": ""})
    )
    return df.drop(columns=["armed_with"])


def fix_overview_shifted_rows(df):
    """Some rows are missing the firearm_involved field, causing columns to shift left.
    Rows with 'YES' in firearm_involved are correct; rows with numeric values
    actually have subjects_involved shifted into firearm_involved."""
    shifted = df.firearm_involved.str.isnumeric().fillna(False)
    df.loc[shifted, "subjects_arrested"] = df.loc[shifted, "subjects_injured"]
    df.loc[shifted, "subjects_injured"] = df.loc[shifted, "officers_injured"]
    df.loc[shifted, "officers_injured"] = df.loc[shifted, "officers_involved"]
    df.loc[shifted, "officers_involved"] = df.loc[shifted, "subjects_involved"]
    df.loc[shifted, "subjects_involved"] = df.loc[shifted, "firearm_involved"]
    df.loc[shifted, "firearm_involved"] = ""
    return df


def clean_overview():
    df = (
        pd.read_csv(
            deba.data("raw/lafayette_pd/lafayette_pd_uof_24_26_overview.csv")
        )
        .pipe(strip_leading_apostrophes)
        .pipe(clean_column_names)
        .drop_duplicates()
        .pipe(fix_overview_shifted_rows)
        .rename(
            columns={
                "event_number": "tracking_id_og",
                "address": "use_of_force_location",
            }
        )
        .pipe(split_overview_datetime)
        .pipe(clean_video_available)
        .pipe(clean_firearm_involved)
        .pipe(
            standardize_desc_cols,
            ["use_of_force_location"],
        )
        .pipe(set_values, {"agency": "lafayette-pd"})
    )
    return df


def clean_injuries():
    df = (
        pd.read_csv(
            deba.data("raw/lafayette_pd/lafayette_pd_uof_24_26_injuries.csv")
        )
        .pipe(strip_leading_apostrophes)
        .pipe(clean_column_names)
        .drop_duplicates()
        .pipe(strip_event_suffix)
        .pipe(clean_injuries_badge)
        .drop(columns=["event_date_time"])
        .pipe(clean_officer_injured)
        .rename(columns={"event_number": "tracking_id_og", "badge": "badge_no"})
        .pipe(clean_names, ["first_name", "last_name"])
    )
    return df


def clean_subjects():
    df = (
        pd.read_csv(
            deba.data("raw/lafayette_pd/lafayette_pd_uof_24_26_subjects.csv")
        )
        .pipe(clean_column_names)
        .drop_duplicates()
        .rename(
            columns={
                "event_number": "tracking_id_og",
                "subject_last_name": "citizen_last_name",
                "subject_first_name": "citizen_first_name",
                "age_at_incident": "citizen_age",
                "race": "citizen_race",
                "sex": "citizen_sex",
                "subject_dob": "citizen_dob",
            }
        )
        .pipe(split_subject_datetime)
        .pipe(clean_citizen_armed_with)
        .drop(columns=["address", "occurred_date", "occurred_time", "person_number"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_names, ["citizen_last_name", "citizen_first_name"])
        .pipe(set_values, {"agency": "lafayette-pd"})
    )
    return df


def build_uof(overview, injuries):
    """Merge overview with injuries to create officer-level UOF records."""
    uof = overview.merge(injuries, on="tracking_id_og", how="left")

    # only generate uid for rows that have officer names
    has_officer = uof.first_name.notna() & uof.last_name.notna()
    officer_rows = uof.loc[has_officer].copy().pipe(
        gen_uid, ["first_name", "last_name", "agency"], "uid"
    )
    uof.loc[has_officer, "uid"] = officer_rows["uid"]

    uof = (
        uof.pipe(
            gen_uid,
            [
                "tracking_id_og",
                "occurred_date",
                "use_of_force_location",
                "first_name",
                "last_name",
            ],
            "uof_uid",
        )
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
    )
    return uof


def build_citizens(subjects, uof):
    """Build citizen-level UOF records linked by uof_uid."""
    # get uof_uid from the built UOF table via tracking_id_og
    link = uof[["tracking_id_og", "uof_uid"]].drop_duplicates()
    citizens = subjects.merge(link, on="tracking_id_og", how="inner")
    citizens = (
        citizens.pipe(
            gen_uid,
            [
                "citizen_last_name",
                "citizen_first_name",
                "citizen_race",
                "citizen_sex",
                "citizen_age",
                "uof_uid",
            ],
            "citizen_uid",
        )
        .drop(columns=["tracking_id_og", "citizen_dob"])
    )
    return citizens


if __name__ == "__main__":
    overview = clean_overview()
    injuries = clean_injuries()
    subjects = clean_subjects()

    uof = build_uof(overview, injuries)
    citizens = build_citizens(subjects, uof)

    uof.to_csv(deba.data("clean/uof_lafayette_pd_2024_2026.csv"), index=False)
    citizens.to_csv(
        deba.data("clean/uof_cit_lafayette_pd_2024_2026.csv"), index=False
    )
