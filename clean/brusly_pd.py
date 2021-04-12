from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.clean import clean_names, clean_dates, standardize_desc_cols, clean_salaries
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append("../")


def split_cprr_name(df):
    names = df.officer_name.str.split(" ", expand=True)
    df.loc[:, "last_name"] = names.loc[:, 1].fillna("")
    df.loc[:, "first_name"] = names.loc[:, 0].fillna("")
    df = df.drop(columns="officer_name")
    return df


def split_supervisor(df):
    sup = df.supervisor.str.replace(
        r"^(.+) (\w+ \w+)$", r"\1@@\2").str.split("@@", expand=True)
    df.loc[:, "supervisor_rank"] = sup.loc[:, 0].str.lower()\
        .str.replace(r"asst\.", "assistant").str.replace(r"lt\.", "lieutenant")
    names = sup.loc[:, 1].str.split(" ", expand=True)
    df.loc[:, "supervisor_first_name"] = names.loc[:, 0].str.lower()
    df.loc[:, "supervisor_last_name"] = names.loc[:, 1].str.lower()
    df = df.drop(columns=["supervisor"])
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "Brusly PD"
    df.loc[:, "data_production_year"] = "2020"
    return df


def clean_cprr():
    df = pd.read_csv(data_file_path("brusly_pd/brusly_pd_cprr_2020.csv"))
    df = clean_column_names(df)
    df.columns = ['receive_date', 'occur_date', 'officer_name', 'supervisor',
                  'charges', 'action', 'suspension_start_date', 'suspension_end_date']
    df = df\
        .pipe(split_cprr_name)\
        .pipe(clean_names, ["last_name", "first_name"])\
        .pipe(split_supervisor)\
        .pipe(clean_dates, ['receive_date', 'occur_date', 'suspension_start_date', 'suspension_end_date'])\
        .pipe(standardize_desc_cols, ['charges', 'action'])\
        .pipe(assign_agency)
    return df


def split_pprr_name(df):
    names = df.employee_name.str.split(", ", expand=True)
    df.loc[:, "last_name"] = names.loc[:, 0].fillna("")
    names = names.loc[:, 1].fillna("").str.split(" ", expand=True)
    df.loc[:, "first_name"] = names.loc[:, 0].fillna("")
    df.loc[:, "middle_initial"] = names.loc[:, 1].fillna("")
    df = df.drop(columns="employee_name")
    return df


def clean_pprr():
    df = pd.read_csv(data_file_path("brusly_pd/brusly_pd_pprr_2020.csv"))
    df = clean_column_names(df)
    df.columns = [
        'employee_name', 'birth_date', 'hire_date', 'term_date',
        'company_name', 'department_name', 'rank_desc', 'annual_salary']
    df = df.drop(df[df.employee_name.isna()].index).reset_index(drop=True)
    df = df.drop(columns=['company_name', 'department_name'])
    df = df\
        .pipe(split_pprr_name)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(clean_dates, ['birth_date', 'hire_date', 'term_date'])\
        .pipe(standardize_desc_cols, ["rank_desc"])\
        .pipe(clean_salaries, ["annual_salary"])\
        .pipe(assign_agency)\
        .pipe(gen_uid, [
            "agency", "last_name", "first_name", "middle_initial", "birth_year", "birth_month", "birth_day"
        ])\
        .pipe(gen_uid, ["uid", "hire_year", "hire_month", "hire_day"], "perhist_uid")
    return df


if __name__ == "__main__":
    cprr = clean_cprr()
    pprr = clean_pprr()
    ensure_data_dir("clean")
    cprr.to_csv(data_file_path("clean/cprr_brusly_pd_2020.csv"), index=False)
    pprr.to_csv(data_file_path("clean/pprr_brusly_pd_2020.csv"), index=False)
