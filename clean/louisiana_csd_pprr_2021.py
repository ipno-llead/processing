from lib.columns import clean_column_names, set_values
import deba
from lib.clean import (
    clean_dates,
    clean_races,
    clean_sexes,
    parse_dates_with_known_format,
    clean_salaries,
    standardize_desc_cols,
    clean_names,
)
from lib.uid import gen_uid
from lib import salary
import pandas as pd
import numpy as np


def split_names(df):
    names = df.employee_name.str.strip().str.extract(r"^([^,]+),([^ ]+)(?: (\w+))?$")
    df.loc[:, "last_name"] = names.loc[:, 0]
    df.loc[:, "first_name"] = names.loc[:, 1]
    df.loc[:, "middle_name"] = names.loc[:, 2]
    df.loc[:, "middle_initial"] = df.middle_name.fillna("").map(lambda x: x[:1])
    return df.drop(columns=["employee_name"])


def clean_hire_date(df):
    df.loc[:, "hire_date"] = df.hire_date.astype(str).replace({"0": np.NaN})
    return df


def clean_demo():
    return (
        pd.read_csv(
            deba.data("raw/louisiana_csd/louisiana_csd_pprr_demographics_2021.csv")
        )
        .pipe(clean_column_names)
        .drop(columns=["agency_name", "classified_unclassified"])
        .rename(
            columns={
                "organizational_unit": "department_desc",
                "job_title": "rank_desc",
                "annual_rate_of_pay": "salary",
                "gender": "sex",
                "data_date": "salary_date",
            }
        )
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "data_production_year": 2021,
                "agency": "Louisiana State PD",
            },
        )
        .pipe(clean_salaries, ["salary"])
        .pipe(split_names)
        .pipe(clean_hire_date)
        .pipe(parse_dates_with_known_format, ["hire_date"], "%Y%m%d")
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(standardize_desc_cols, ["rank_desc", "department_desc"])
        .pipe(clean_names, ["first_name", "last_name", "middle_initial", "middle_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name", "race"])
        .sort_values(["uid", "salary_date", "salary"], na_position="first")
        .drop_duplicates(["uid", "salary_date"], keep="last")
    )


def clean_term():
    return (
        pd.read_csv(
            deba.data("raw/louisiana_csd/louisiana_csd_pprr_terminations_2021.csv")
        )
        .pipe(clean_column_names)
        .drop(columns=["agency_name", "action_type"])
        .rename(
            columns={
                "organization_unit": "department_desc",
                "job_title": "rank_desc",
                "action_reason": "left_reason",
                "action_effective_date": "left_date",
            }
        )
        .pipe(standardize_desc_cols, ["rank_desc", "department_desc"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_dates, ["left_date"])
        .pipe(
            set_values,
            {
                "data_production_year": 2021,
                "agency": "Louisiana State PD",
            },
        )
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


if __name__ == "__main__":
    demo = clean_demo()
    term = clean_term()
    demo.to_csv(deba.data("clean/pprr_demo_louisiana_csd_2021.csv"), index=False)
    term.to_csv(deba.data("clean/pprr_term_louisiana_csd_2021.csv"), index=False)
