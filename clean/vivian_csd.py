import pandas as pd

from lib.columns import clean_column_names
import deba
from lib.clean import clean_dates, clean_names, float_to_int_str, standardize_desc_cols
from lib.uid import gen_uid
from lib import salary


def split_salary_col(df):
    df.loc[:, "salary"] = df.salary.fillna("").str.lower().str.strip()
    df.loc[df.salary.str.match(r".+\/week$"), "salary_freq"] = salary.WEEKLY
    df.loc[df.salary.str.match(r".+\/hr$"), "salary_freq"] = salary.HOURLY
    df.loc[:, "salary"] = df.salary.str.replace(r"\/week$", "", regex=True).str.replace(
        r"\/hr$", "", regex=True
    )
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "Vivian PD"
    df.loc[:, "data_production_year"] = 2021
    return df

def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = df.job_title.str.lower().str.strip()\
        .str.replace(r" ?o?f? ?police", "", regex=True)
    return df 


def clean():
    return (
        pd.read_csv(deba.data("raw/vivian_csd/vivian_csd_pprr_2021.csv"))
        .pipe(clean_column_names)
        .drop(columns=["length_of_service"])
        .rename(
            columns={
                "employee_type": "employment_status",
                "last_promotion": "rank_year",
                "date_of_hire": "hire_date",
            }
        )
        .pipe(clean_rank_desc)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(split_salary_col)
        .pipe(float_to_int_str, ["rank_year"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["rank_desc", "employment_status"])
        .pipe(assign_agency)
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


if __name__ == "__main__":
    df = clean()

    df.to_csv(deba.data("clean/pprr_vivian_pd_2021.csv"), index=False)
