from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, clean_dates, standardize_desc_cols, clean_salaries, clean_races, clean_sexes
from lib.uid import gen_uid
from lib import salary
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
    sup = df.supervisor.str.replace(r"^(.+) (\w+ \w+)$", r"\1@@\2", regex=True)\
        .str.split("@@", expand=True)
    df.loc[:, "supervisor_rank"] = sup.loc[:, 0].str.lower()\
        .str.replace(r"asst\.", "assistant", regex=True)\
        .str.replace(r"lt\.", "lieutenant", regex=True)
    names = sup.loc[:, 1].str.split(" ", expand=True)
    df.loc[:, "supervisor_first_name"] = names.loc[:, 0].str.lower()
    df.loc[:, "supervisor_last_name"] = names.loc[:, 1].str.lower()
    df = df.drop(columns=["supervisor"])
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
        .pipe(set_values, {
            'data_production_year': 2020,
            'agency': 'Brusly PD'
        })
    return df


def split_pprr_name(df):
    names = df.full_nme.str.split(", ", expand=True)
    df.loc[:, "last_name"] = names.loc[:, 0].fillna("")
    names = names.loc[:, 1].fillna("").str.split(" ", expand=True)
    df.loc[:, "first_name"] = names.loc[:, 0].fillna("")
    df.loc[:, "middle_initial"] = names.loc[:, 1].fillna("")
    df = df.drop(columns="full_nme")
    return df


def clean_position(df):
    df.loc[:, 'rank_desc'] = df.position.str.lower().str.strip()\
        .str.replace('police ', '', regex=False)
    return df.drop(columns='position')


def clean_pprr():
    df = pd.read_csv(data_file_path("brusly_pd/brusly_pd_pprr_2020.csv"))\
        .pipe(clean_column_names)\
        .drop(columns=['company_name', 'department_name'])\
        .rename(columns={'annual_salary': 'salary'})\
        .pipe(clean_salaries, ['salary'])\
        .pipe(set_values, {'salary_freq': salary.YEARLY})\
        .pipe(split_pprr_name)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(clean_dates, ['hire_date', 'birth_date'])\
        .pipe(clean_races, ['race'])\
        .pipe(clean_sexes, ['sex'])\
        .pipe(clean_position)\
        .pipe(set_values, {
            'data_production_year': 2020,
            'agency': 'Brusly PD'
        })\
        .pipe(gen_uid, [
            "agency", "last_name", "first_name", "middle_initial", "birth_year", "birth_month", "birth_day"
        ])
    return df


def clean_award():
    return pd.read_csv(data_file_path('brusly_pd/brusly_pd_awards_2015-2020_byhand.csv'))\
        .pipe(clean_column_names)\
        .rename(columns={
            'l_name': "last_name",
            'f_name': 'first_name',
            'comments': 'award_comments'
        })\
        .pipe(set_values, {
            'data_production_year': 2021,
            'agency': 'Brusly PD'
        })\
        .pipe(gen_uid, ['agency', 'last_name', 'first_name'])


if __name__ == "__main__":
    cprr = clean_cprr()
    pprr = clean_pprr()
    award = clean_award()
    ensure_data_dir("clean")
    cprr.to_csv(data_file_path("clean/cprr_brusly_pd_2020.csv"), index=False)
    pprr.to_csv(data_file_path("clean/pprr_brusly_pd_2020.csv"), index=False)
    award.to_csv(data_file_path("clean/award_brusly_pd_2021.csv"), index=False)
