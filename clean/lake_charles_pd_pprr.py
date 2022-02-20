from lib.uid import gen_uid

import pandas as pd
import bolo
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, standardize_desc_cols, clean_salaries
from lib import salary


def clean_f_name(df):
    df.loc[:, "first_name"] = (
        df.first_name.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"(^jr |^ii? |^r |^os )", "", regex=True)
        .str.replace(r" totals$", "", regex=True)
        .str.replace(r"^en ", "", regex=True)
        .str.replace(r"^i willie$", "willie", regex=True)
    )
    return df


def extract_suffix(df):
    suffix = df.first_name.str.lower().str.strip().str.extract(r"(^jr|^ii)")

    df.loc[:, "suffix"] = suffix[0].fillna("")
    return df


def clean_l_name(df):
    df.loc[:, "last_name"] = (
        df.last_name.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^etienne-gre$", "etienne-green", regex=True)
        .str.replace(r"\.$", "", regex=True)
        .str.replace(r" depart ment$", "", regex=True)
        .str.replace(r"^newingham$", "newingham jr", regex=True)
    )

    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns="suffix")


def clean_employee_id(df):
    df.loc[:, "employee_id"] = (
        df.emp_no.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r" \-$", "", regex=True)
        .str.replace(r"^2 (\w+)", r"\1", regex=True)
        .str.replace(r"^5 2 (\w+)", r"\1", regex=True)
        .str.replace(r"^1 (\w+)", r"\1", regex=True)
        .str.replace(r"^5 45 (\w+)", r"\1", regex=True)
        .str.replace(r"^(\w+) 11$", r"\1", regex=True)
        .str.replace(r" á$", "", regex=True)
    )
    return df.drop(columns="emp_no")


def extract_department_desc(df):
    departments = (
        df.title.str.lower()
        .str.strip()
        .str.extract(
            r"(general maintence|administrative (.+)|communications|computer (.+)|records (.+))"
        )
    )

    df.loc[:, "department_desc"] = (
        departments[0].fillna("").str.replace(r" a$", "", regex=True)
    )
    return df


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.title.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"(\,|\/|\.)(.+)", "", regex=True)
        .str.replace(r"^o ", "", regex=True)
        .str.replace(" of police", "", regex=False)
        .str.replace("to police chief", "", regex=False)
        .str.replace(r"^police$", "", regex=True)
    )
    return df.drop(columns="title")


def clean_annual_salary(df):
    df.loc[:, "salary"] = df.annual_salary.str.replace(
        r"^(\w+)\.(\w+) ", "", regex=True
    ).str.replace(r"(\w+)\,(\w+)\,(\w+)$", r"\1,\2.\3", regex=True)
    return df


def clean():
    df = (
        pd.read_csv(bolo.data("raw/lake_charles_pd/pprr_lake_charles_pd_2017_2021.csv"))
        .pipe(clean_column_names)
        .drop(columns=["current_hourly_rate"])
        .pipe(clean_employee_id)
        .pipe(extract_suffix)
        .pipe(clean_f_name)
        .pipe(clean_l_name)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(extract_department_desc)
        .pipe(clean_rank_desc)
        .pipe(standardize_desc_cols, ["rank_desc", "department_desc"])
        .pipe(clean_annual_salary)
        .pipe(clean_salaries, ["salary"])
        .pipe(set_values, {"salary_freq": salary.YEARLY, "agency": "Lake Charles PD"})
        .pipe(gen_uid, ["employee_id", "agency", "first_name", "last_name"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(bolo.data("clean/pprr_lake_charles_pd_2017_2021.csv"), index=False)
