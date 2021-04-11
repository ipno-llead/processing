from lib.columns import clean_column_names
from lib.clean import (
    clean_names, parse_dates_with_known_format, clean_salaries, standardize_desc_cols,
    clean_employment_status
)
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append("../")


def assign_agency(df):
    df.loc[:, "agency"] = "Baton Rouge CSD"
    return df


def assign_rank_year_and_pay_year(df):
    df = df.sort_values(['uid', 'year'])
    uid = None
    salary = None
    rank = None
    dept = None
    for idx, row in df.iterrows():
        if row.uid != uid:
            uid = row.uid
        else:
            if row.annual_salary != salary:
                df.loc[idx, 'pay_effective_year'] = row.year
            if row.rank_desc != rank:
                df.loc[idx, 'rank_year'] = row.year
            if row.department_desc != dept:
                df.loc[idx, 'dept_year'] = row.year
        salary = row.annual_salary
        rank = row.rank_desc
        dept = row.department_desc
    return df


def clean_17():
    df = pd.read_csv(data_file_path(
        "baton_rouge_csd/baton_rouge_csd_pprr_2017.csv"))
    df = clean_column_names(df)
    df = df[[
        'year', 'employee_num', 'last_name', 'first_name', 'middle_init', 'division_num', 'division_name', 'job_code',
        'job_title', 'current_hire_date', 'employment_end_date', 'employment_status', 'gross_pay', 'total_hourly_rate'
    ]]
    df = df.rename(columns={
        "employee_num": "employee_id",
        "middle_init": "middle_initial",
        "division_num": "department_code",
        "division_name": "department_desc",
        "job_code": "rank_code",
        "job_title": "rank_desc",
        "current_hire_date": "hire_date",
        "employment_end_date": "resign_date",
        "gross_pay": "annual_salary",
        "total_hourly_rate": "hourly_salary"
    })
    df.loc[:, "data_production_year"] = "2017"
    df = df\
        .pipe(clean_names, ["first_name", "last_name", "middle_initial"])\
        .pipe(clean_salaries, ["annual_salary"])\
        .pipe(standardize_desc_cols, ["department_desc", "rank_desc"])\
        .pipe(parse_dates_with_known_format, ["hire_date", "resign_date"], "%m/%d/%Y")\
        .pipe(assign_agency)\
        .pipe(gen_uid, [
            "agency", "data_production_year", "first_name", "last_name", "middle_initial"])\
        .pipe(assign_rank_year_and_pay_year)\
        .pipe(gen_uid, ['uid', 'year', 'pay_effective_year', 'rank_year', 'dept_year'], 'perhist_uid')
    return df


def clean_19():
    df = pd.read_csv(data_file_path(
        "baton_rouge_csd/baton_rouge_csd_pprr_2019.csv"))
    df = clean_column_names(df)
    df = df[['year', 'last_name', 'first_name', 'middle_init',
             'pay_location_code', 'pay_location_description',
             'job_code', 'job_title', 'current_hire_date', 'employment_end_date',
             'employment_status', 'gross_pay', 'uniqueid']]
    df = df.rename(columns={
        # "year": "data_production_year",
        "middle_init": "middle_initial",
        "pay_location_code": "department_code",
        "pay_location_description": "department_desc",
        "job_code": "rank_code",
        "job_title": "rank_desc",
        "current_hire_date": "hire_date",
        "employment_end_date": "resign_date",
        "gross_pay": "annual_salary",
        "uniqueid": "employee_id"
    })
    df.loc[:, "data_production_year"] = "2019"
    df = df.drop_duplicates()
    df = df\
        .pipe(clean_names, ["first_name", "last_name", "middle_initial"])\
        .pipe(clean_salaries, ["annual_salary"])\
        .pipe(standardize_desc_cols, ["department_desc", "rank_desc"])\
        .pipe(parse_dates_with_known_format, ["hire_date", "resign_date"], "%m/%d/%Y")\
        .pipe(clean_employment_status, ["employment_status"])\
        .pipe(assign_agency)\
        .pipe(gen_uid, ["agency", "data_production_year", "employee_id"])\
        .pipe(assign_rank_year_and_pay_year)\
        .pipe(gen_uid, ['uid', 'year', 'pay_effective_year', 'rank_year', 'dept_year'], 'perhist_uid')
    return df


if __name__ == "__main__":
    df17 = clean_17()
    df19 = clean_19()
    ensure_data_dir("clean")
    df17.to_csv(
        data_file_path("clean/pprr_baton_rouge_csd_2017.csv"),
        index=False)
    df19.to_csv(
        data_file_path("clean/pprr_baton_rouge_csd_2019.csv"),
        index=False)
