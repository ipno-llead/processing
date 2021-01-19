import pandas as pd
from lib.clean import (
    clean_date, clean_salary, clean_name, standardize_desc
)
from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.match_records import gen_uid
import sys
sys.path.append("../")


def read_input():
    df = pd.read_csv(data_file_path(
        "new_orleans_harbor_pd/new_orleans_harbor_pd_pprr_2020.csv"))
    df = clean_column_names(df)
    return df.dropna(how="all")


def clean_personel():
    df = read_input()
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
    for col in ["term_date", "hire_date", "pay_effective_date"]:
        df2.loc[:, col] = clean_date(df2[col])
    df2.loc[:, "hourly_salary"] = clean_salary(df2.hourly_salary)
    df2.loc[:, "rank_desc"] = standardize_desc(df2.rank_desc)
    df2 = gen_uid(df2, ["first_name", "last_name",
                        "middle_initial", "term_date", "hire_date"])
    return df2


if __name__ == "__main__":
    df = clean_personel()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/pprr_new_orleans_harbor_pd_2020.csv"),
        index=False)
