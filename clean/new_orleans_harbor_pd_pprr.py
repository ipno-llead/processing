import pandas as pd
from lib.clean import (
    clean_dates, clean_salary, clean_name, clean_names, standardize_desc, standardize_desc_cols,
    clean_salaries
)
from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.uid import gen_uid
import sys
sys.path.append("../")


def assign_agency(df, year):
    df.loc[:, 'agency'] = 'New Orleans Harbor PD'
    df.loc[:, 'data_production_year'] = year
    return df


def clean_personnel_2008():
    df = pd.read_csv(data_file_path(
        "new_orleans_harbor_pd/new_orleans_harbor_pd_pprr_1991-2008.csv"
    ))
    df = clean_column_names(df)
    df = df.dropna(axis=1, how="all")
    df = df.rename(columns={
        "mi": "middle_initial",
        "date_hired": "hire_date",
        "position_rank": "rank_desc",
        "hourly_pay_rate": "hourly_salary",
        "effective_date": "pay_effective_date"
    })
    return df\
        .pipe(clean_dates, ["hire_date", "term_date", "pay_effective_date"])\
        .pipe(clean_names, ["first_name", "last_name", "middle_initial"])\
        .pipe(standardize_desc_cols, ["rank_desc"])\
        .pipe(clean_salaries, ["hourly_salary"])\
        .pipe(assign_agency, 2008)\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name', 'middle_initial'])


def clean_personnel_2020():
    df = pd.read_csv(data_file_path(
        "new_orleans_harbor_pd/new_orleans_harbor_pd_pprr_2020.csv"))
    df = clean_column_names(df)
    df = df.dropna(how="all")
    df2 = df[["first_name", "last_name", "position_rank", "date_hired",
              "term_date", "hourly_pay", "mi", "pay_effective_date"]]
    df2.rename(columns={
        "date_hired": "hire_date",
        "position_rank": "rank_desc",
        "hourly_pay": "hourly_salary",
        "mi": "middle_initial"
    }, inplace=True)
    for col in ["last_name", "middle_initial", "first_name"]:
        df2.loc[:, col] = clean_name(df2[col])
    df2 = clean_dates(
        df2, ["term_date", "hire_date", "pay_effective_date"])
    df2.loc[:, "hourly_salary"] = clean_salary(df2.hourly_salary)
    df2.loc[:, "rank_desc"] = standardize_desc(df2.rank_desc)
    df2 = gen_uid(df2, ["first_name", "last_name",
                        "middle_initial", "hire_year", "hire_month", "hire_day"])
    df2.loc[:, "agency"] = "New Orleans Harbor PD"
    df2.loc[:, "data_production_year"] = 2020
    return df2


if __name__ == "__main__":
    df20 = clean_personnel_2020()
    df08 = clean_personnel_2008()
    ensure_data_dir("clean")
    df20.to_csv(
        data_file_path("clean/pprr_new_orleans_harbor_pd_2020.csv"),
        index=False)
    df08.to_csv(
        data_file_path("clean/pprr_new_orleans_harbor_pd_1991_2008.csv"),
        index=False)
