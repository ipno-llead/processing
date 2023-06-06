import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_names,
    clean_dates,
    names_to_title_case,
    standardize_desc_cols,
)
from lib.uid import gen_uid
from functools import reduce


def split_rows_with_multiple_allegations(df):
    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split(";", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = df.action.str.replace(
        r"suspensino", "suspension", regex=False
    )
    return df


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/louisiana_state_pd/cprr_louisiana_state_pd_2019_2020.csv")
        )
        .pipe(clean_column_names)
        .rename(columns={"tracking_id": "tracking_id_og"})
        .pipe(split_rows_with_multiple_allegations)
        .pipe(clean_action)
        .pipe(standardize_desc_cols, ["allegation", "action", "disposition"])
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "action", "uid"], "allegation_uid")
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_louisiana_state_pd_2019_2020.csv"), index=False)
