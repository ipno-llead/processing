import pandas as pd
import bolo
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, clean_dates
from lib.uid import gen_uid


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("and where ", "", regex=False)
        .str.replace(r"^fleet crash\/incident", "fleet crash incident", regex=True)
        .str.replace(r"&", "and", regex=True)
        .str.replace(r"\, ", "/", regex=True)
    )
    return df.drop(columns="complaint")


def split_rows_with_allegations(df):
    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def clean_investigation_status(df):
    df.loc[:, "investigation_status"] = (
        df.complete.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("x", "complete", regex=False)
    )
    return df.drop(columns="complete")


def clean_action(df):
    df.loc[:, "action"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"hrs\.", "hours.", regex=True)
        .str.replace(r"suspended for (\w+) (\w+)\.", r"\1-\2 suspension", regex=True)
        .str.replace(r"loss of unit for (\w+) (\w+)", r"\1-\2 loss of unit", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace("terminated", "termination", regex=False)
    )
    return df.drop(columns="disposition")


def split_names(df):
    names = df.name.str.lower().str.strip().fillna("").str.extract(r"(\w+)\, (\w+)")
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    return df.drop(columns="name")


def clean():
    df = (
        pd.read_csv(bolo.data("raw/st_landry_so/st_landry_so_cprr_2019_2020.csv"))
        .pipe(clean_column_names)
        .rename(columns={"case": "tracking_number", "date": "receive_date"})
        .pipe(clean_dates, ["receive_date"])
        .pipe(clean_allegation)
        .pipe(split_rows_with_allegations)
        .pipe(clean_investigation_status)
        .pipe(clean_action)
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "St. Landry SO"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "tracking_number", "allegation", "action"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(bolo.data("clean/cprr_st_landry_so_2020.csv"), index=False)
