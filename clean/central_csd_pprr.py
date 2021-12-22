import sys

sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import clean_salaries
from lib import salary


def clean_employment_status(df):
    df.loc[:, "employment_status"] = (
        df.ft_or_pt_status.str.lower()
        .str.strip()
        .str.replace("ft", "full time", regex=False)
        .str.replace("pt", "part time", regex=False)
    )
    return df.drop(columns="ft_or_pt_status")


def clean_rank(df):
    df.loc[:, "rank_desc"] = (
        df.ranks.str.lower()
        .str.strip()
        .str.replace(r"(assistant)? ?po[jl]?i?i[co]e ?", "", regex=True)
        .str.replace("sergeant/dispatcher", "sergeant", regex=False)
        .str.replace(r"(\w+)/officer", r"officer", regex=True)
    )
    return df.drop(columns="ranks")


def split_name(df):
    df.loc[:, "name_of_officer"] = (
        df.name_of_officer.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("doug browning", "browning, doug", regex=False)
    )
    names = df.name_of_officer.str.extract(r"^(?:(\w+)) ?(jr|sr)?,? (\w+) ?(.+)?")
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "suffix"] = names[1].fillna("")
    df.loc[:, "first_name"] = names[2]
    df.loc[:, "middle_initial"] = names[3].str.replace(r"\.", "", regex=True).fillna("")
    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["name_of_officer", "suffix"])


def assign_agency(df):
    df.loc[:, "agency"] = "Central PD"
    return df


def clean():
    df = (
        pd.read_csv(data_file_path("raw/central_csd/central_csd_pprr_2014_2019.csv"))
        .pipe(clean_column_names)
        .drop(columns=["sadge_number", "hourty_rate"])
        .rename(columns={"date_of_hire": "hire_date"})
        .pipe(clean_employment_status)
        .pipe(clean_rank)
        .pipe(split_name)
        .pipe(assign_agency)
        .pipe(clean_salaries, ["salary"])
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(gen_uid, ["last_name", "first_name", "middle_initial", "agency"])
        .drop_duplicates(subset=["hire_date", "uid"], keep="first")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path("clean/pprr_central_csd_2014_2019.csv"), index=False)
