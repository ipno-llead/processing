import pandas as pd
import deba
from lib.clean import standardize_desc_cols
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def split_officer_id(df):
    parts = df.case_reporting_officer_id.str.extract(
        r"(\d+)\s*-\s*(.+)"
    )
    df.loc[:, "badge_number"] = parts[0].str.strip()
    df.loc[:, "last_name"] = parts[1].str.strip()
    return df.drop(columns=["case_reporting_officer_id"])


def split_datetime_col(df):
    data = df.case_reported_date_and_time.str.extract(
        r"(\d{1,2}/\d{1,2}/\d{4})\s*(.*)"
    )
    df.loc[:, "occurred_date"] = data[0]
    df.loc[:, "occurred_time"] = data[1]
    return df.drop(columns=["case_reported_date_and_time"])


def clean_subject_injury(df):
    df.loc[:, "use_of_force_result"] = (
        df.case_subject_injury_type.str.lower()
        .str.strip()
    )
    return df.drop(columns=["case_subject_injury_type"])


def clean_offense(df):
    df.loc[:, "use_of_force_description"] = (
        df.case_offense_statute_abbreviation.str.lower()
        .str.strip()
    )
    return df.drop(columns=["case_offense_statute", "case_offense_statute_abbreviation"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/gonzales_pd/gonzales_pd_uof_23_26.csv"))
        .pipe(clean_column_names)
        .drop_duplicates()
        .rename(columns={"case_number": "tracking_id"})
        .pipe(split_officer_id)
        .pipe(split_datetime_col)
        .pipe(clean_subject_injury)
        .pipe(clean_offense)
        .pipe(standardize_desc_cols, ["tracking_id", "last_name"])
        .pipe(set_values, {"agency": "gonzales-pd"})
        .pipe(gen_uid, ["last_name", "agency"], "uid")
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "use_of_force_description",
                "occurred_date",
                "use_of_force_result",
            ],
            "uof_uid",
        )
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


if __name__ == "__main__":
    uof = clean()
    uof.to_csv(deba.data("clean/uof_gonzales_pd_2023_2026.csv"), index=False)
