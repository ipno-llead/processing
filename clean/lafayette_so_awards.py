from lib.clean import convert_dates

import pandas as pd
import deba
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values
from lib.clean import strip_leading_comma


def split_officer_name(df):
    names = (
        df.deputy_name.str.lower()
        .str.strip()
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\w+) (\w+)")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns=["deputy_name"])


def clean_17():
    df = (
        pd.read_csv(deba.data("raw/lafayette_so/lafayette_so_awards_2017.csv"))
        .pipe(clean_column_names)
        .rename(columns={"month": "receive_month", "division": "department_desc"})
        .pipe(split_officer_name)
        .pipe(convert_dates, ["receive_month"])
        .pipe(
            set_values,
            {"agency": "lafayette-so", "receive_day": "1", "receive_year": "2017"},
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "department_desc", "receive_month", "receive_day", "receive_year"],
            "award_uid",
        )
    )
    return df

def clean_18():
    df = (
        pd.read_csv(deba.data("raw/lafayette_so/lafayette_so_awards_2018.csv"))
        .pipe(clean_column_names)
        .rename(columns={"month": "receive_month", "division": "department_desc"})
        .pipe(strip_leading_comma)
        .pipe(split_officer_name)
        .pipe(convert_dates, ["receive_month"])
        .pipe(
            set_values,
            {"agency": "lafayette-so", "receive_day": "1", "receive_year": "2018"},
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "department_desc", "receive_month", "receive_day", "receive_year"],
            "award_uid",
        )
    )
    return df

def clean_19():
    df = (
        pd.read_csv(deba.data("raw/lafayette_so/lafayette_so_awards_2019.csv"))
        .pipe(clean_column_names)
        .rename(columns={"month": "receive_month", "division": "department_desc"})
        .pipe(strip_leading_comma)
        .pipe(split_officer_name)
        .pipe(convert_dates, ["receive_month"])
        .pipe(
            set_values,
            {"agency": "lafayette-so", "receive_day": "1", "receive_year": "2019"},
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "department_desc", "receive_month", "receive_day", "receive_year"],
            "award_uid",
        )
    )
    return df

def clean_20():
    df = (
        pd.read_csv(deba.data("raw/lafayette_so/lafayette_so_awards_2020.csv"))
        .pipe(clean_column_names)
        .rename(columns={"month": "receive_month", "division": "department_desc"})
        .pipe(strip_leading_comma)
        .pipe(split_officer_name)
        .pipe(convert_dates, ["receive_month"])
        .pipe(
            set_values,
            {"agency": "lafayette-so", "receive_day": "1", "receive_year": "2020"},
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "department_desc", "receive_month", "receive_day", "receive_year"],
            "award_uid",
        )
    )
    return df

def clean_21():
    df = (
        pd.read_csv(deba.data("raw/lafayette_so/lafayette_so_awards_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"month": "receive_month", "division": "department_desc"})
        .pipe(strip_leading_comma)
        .pipe(split_officer_name)
        .pipe(convert_dates, ["receive_month"])
        .pipe(
            set_values,
            {"agency": "lafayette-so", "receive_day": "1", "receive_year": "2021"},
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "department_desc", "receive_month", "receive_day", "receive_year"],
            "award_uid",
        )
    )
    return df


if __name__ == "__main__":
    df17 = clean_17()
    df18 = clean_18()
    df19 = clean_19()
    df20 = clean_20()
    df21 = clean_21()
    df = pd.concat([df17, df18, df19, df20, df21], ignore_index=True)
    df.to_csv(deba.data("clean/award_lafayette_so_2017_2021.csv"), index=False)
