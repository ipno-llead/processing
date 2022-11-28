import pandas as pd
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols
from lib.rows import duplicate_row
import re
import deba


def clean_incident_location(df):
    df.loc[:, "incident_location"] = (
        df.occurrence_location.str.lower()
        .str.strip()
        .str.replace(r"^n\/a$", "", regex=True)
    )
    return df.drop(columns=["occurrence_location"])


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.bias_complaint_response.str.lower()
        .str.strip()
        .str.replace(r"(^no |yes )", "", regex=True)
        .str.replace("sustalned", "sustained", regex=False)
    )
    return df.drop(columns=["bias_complaint_response"])


def split_rows_with_multiple_officers(df):
    i = 0
    for idx in df[df.employee_involved.str.contains("/")].index:
        s = df.loc[idx + i, "employee_involved"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "employee_involved"] = name
        i += len(parts) - 1
    return df


def split_officer_names(df):
    names = (
        df.employee_involved.str.lower()
        .str.strip()
        .str.replace(r"^1lt ", "", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"^(?:(\w+\'?\w+?) )? ?(\w+)")
    )

    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "last_name"] = names[1].fillna("")
    return df.drop(columns=["employee_involved"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/ascension_so/ascension_so_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "tracking": "tracking_id",
                "date": "receive_date",
                "occurrence_date": "occur_date",
            }
        )
        .drop(columns=["file_location_db_assoclated", "al_associated"])
        .pipe(clean_dates, ["receive_date", "occur_date"])
        .pipe(clean_incident_location)
        .pipe(clean_disposition)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_officer_names)
        .pipe(standardize_desc_cols, ["incident_location"])
        .pipe(set_values, {"agency": "ascension-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "disposition", "tracking_id"], "allegation_uid")
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_ascension_so_2019_2021.csv"), index=False)
