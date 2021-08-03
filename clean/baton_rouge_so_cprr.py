from lib.columns import clean_column_names
from lib.uid import gen_uid
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, standardize_desc_cols, clean_dates, clean_sexes, clean_races, clean_datetimes
)
import pandas as pd
import sys
sys.path.append("../")


def split_name(df):
    names1 = df.name.str.strip().str.replace(
        r"^(\w+(?: \w\.)?) (\w+(?:, \w{2}\.)?)$", r"\1@@\2", regex=False).str.split("@@", expand=True)
    names2 = names1.iloc[:, 0].str.split(" ", expand=True)
    df.loc[:, "first_name"] = names2.iloc[:, 0]
    df.loc[:, "middle_initial"] = names2.iloc[:, 1]
    df.loc[:, "last_name"] = names1.iloc[:, 0]
    df = df.drop(columns=["name"])
    return df


def clean_action(df):
    df.loc[:, "action"] = df.action.str.lower().str.strip()\
        .str.replace(r"(\. |, | and )", " | ", regex=False).str.replace(r"\.$", "", regex=False)\
        .str.replace("privliges", "privileges", regex=False)\
        .str.replace("priviledges", "privileges", regex=False)\
        .str.replace(
            "demotion to cpl to deputy and suspended for 7 days",
            "7-day suspension | demotion - cpl. to dep.", regex=False)\
        .str.replace("capt.", "captain", regex=False)\
        .str.replace(
            "suspended five days and the loss of take home vehicle privileges for 60 days.",
            "5-day suspension | loss of take home vehicle privileges for 60 days", regex=False)\
        .str.replace("admin.", "administration", regex=False)\
        .str.replace("none.", "none", regex=False)\
        .str.replace("resignation", "resigned", regex=False)\
        .str.replace("correctrions", "corrections", regex=False)\
        .str.replace("prev.", "privileges", regex=False)\
        .str.replace("Verbally reprimanded", "verbal reprimand", regex=False)\
        .str.replace("one day suspension", "1-day suspension", regex=False)\
        .str.replace("one-day suspension", "1-day suspension", regex=False)\
        .str.replace("suspended two days", "2-day suspension", regex=False)\
        .str.replace("suspended without pay for two days", "2-day suspension without pay", regex=False)\
        .str.replace("seven day suspension.", "7-day suspension", regex=False)\
        .str.replace("3 day suspension", "3-day suspension", regex=False)\
        .str.replace("10 day suspension", "10-day suspension", regex=False)\
        .str.replace("30 day suspension", "30-day suspension", regex=False)\
        .str.replace("7 day suspension", "7-day suspension", regex=False)\
        .str.replace("5 day suspension", "5-day suspension", regex=False)\
        .str.replace("2 week suspension", "2-week suspension", regex=False)\
        .str.replace(
            "suspended one day and loss of take home vehicle privileges for 30 days",
            "1-day suspension | loss of take home vehicle privileges for 30 days", regex=False)\
        .str.replace(
            "10 day suspension 30 day loss of extra duty detail privelages",
            "10-day suspension | 30-day loss of extra duty detail privileges", regex=False)\
        .str.replace(
            "loss of take home vehicle privileges for 60 days", "60-day loss of take home vehicle privileges",
            regex=False)\
        .str.replace(
            "loss of take of home vehicle privileges for 20 days",
            "20-day loss of take of home vehicle privileges", regex=False)
    return df


def clean_complainant(df):
    df.loc[:, "complainant_type"] = df.complainant_type.str.lower().str.strip()\
        .str.replace("brpd detective", "baton rouge police department detective", regex=False)\
        .str.replace("deer park texas pd", 'deer park texas police department', regex=False)\
        .str.replace("ebrso administration and brpd", "administration and baton rouge police department", regex=False)\
        .str.replace("administration (see also 17-19)", "administration", regex=False)
    return df


def split_infraction(df):
    infractions = df.infraction.str.extract(r"^([A-Za-z ,]+)(\d.+)?$")
    df.loc[:, 'charges'] = infractions.iloc[:, 1].fillna('').str.strip()\
        .str.replace(r"-(\d+)$", r".\1", regex=False)\
        .str.cat(
            infractions.iloc[:, 0].str.strip(),
            sep=' - ')\
        .str.replace(r'^ - ', '', regex=False)
    df = df.drop(columns=["infraction"])
    return df


def clean_charges(df):
    df.loc[:, "charges"] = df.charges.str.lower().str.strip() \
        .str.replace(r'^([^\s]*)\s+', "", regex=False) \
        .str.replace("1-d1.05", "01-01.05", regex=False)\
        .str.replace("1- 01.14 ", "01-01.14", regex=False)\
        .str.replace("01-1.06", "01-01.06", regex=False)\
        .str.replace("1-01.14", "01-01.14", regex=False)\
        .str.replace("performanceb", "performance", regex=False)\
        .str.replace("use offorce", "use of force", regex=False)\
        .str.replace("performanced", "performance", regex=False)\
        .str.replace(" informationid", "information", regex=False)\
        .str.replace("courtesye", "courtesy", regex=False)\
        .str.replace("001-01.14 - unsatisfactory performance", "01-01.14 unsatisfactory performance", regex=False)\
        .str.replace("1-01.1 - dissemination ofinformation", "01-01.18 - dissemination of information", regex=False) \
        .str.replace("01-01.0 - courtesy", "01-01.05 - courtesy", regex=False)\
        .str.replace("001-01.14- unsatisfactory performance", "01-01.14 - unsatisfactory performance", regex=False)\
        .str.replace("1-01 - unsatisfactory  performance", "01-01.14 - unsatisfactory performance", regex=False)\
        .str.replace("1-01.1 - unsatisfactory performance", "01-01.14 - unsatisfactory performance", regex=False)
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "Baton Rouge SO"
    return df


def assign_prod_year(df, year):
    df.loc[:, "data_production_year"] = year
    return df


def clean18():
    df = pd.read_csv(data_file_path(
        "baton_rouge_so/baton_rouge_so_cprr_2018.csv"))
    df = clean_column_names(df)
    df.columns = ['name', 'badge_no', 'rank_desc', 'rank_date', 'race', 'sex',
                  'birth_year', 'infraction',
                  'occur_datetime',
                  'complainant_type',
                  'disposition', 'action', 'department_desc']
    df = df\
        .pipe(split_name)\
        .pipe(split_infraction)\
        .pipe(
            standardize_desc_cols,
            ["rank_desc", "disposition", "complainant_type", "department_desc", "charges"])\
        .pipe(clean_dates, ["rank_date"])\
        .pipe(clean_races, ["race"])\
        .pipe(clean_sexes, ["sex"])\
        .pipe(clean_datetimes, ["occur_datetime"])\
        .pipe(clean_action)\
        .pipe(assign_agency)\
        .pipe(assign_prod_year, '2018')\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(gen_uid, ["agency", "first_name", "last_name", "birth_year", "badge_no"])\
        .pipe(gen_uid, ['agency', 'uid', 'occur_year', 'occur_month', 'occur_day'], 'complaint_uid')
    return df


def clean20():
    df = pd.read_csv(data_file_path(
        "baton_rouge_so/baton_rouge_so_cprr_2016-2020.csv"))
    df = clean_column_names(df)
    df.columns = ['tracking_number', 'name', 'badge_no', 'rank_desc', 'rank_date', 'race', 'sex',
                  'birth_year', "department_desc", 'infraction',
                  'occur_datetime',
                  'complainant_type',
                  'disposition', 'action']
    df = df\
        .pipe(split_name)\
        .pipe(split_infraction)\
        .pipe(
            standardize_desc_cols,
            ["rank_desc", "disposition", "complainant_type", "department_desc", "charges"])\
        .pipe(clean_dates, ["rank_date"])\
        .pipe(clean_races, ["race"])\
        .pipe(clean_sexes, ["sex"])\
        .pipe(clean_datetimes, ["occur_datetime"])\
        .pipe(clean_action)\
        .pipe(clean_charges)\
        .pipe(assign_agency)\
        .pipe(clean_complainant)\
        .pipe(assign_prod_year, '2020')\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(gen_uid, ["agency", "first_name", "last_name", "birth_year", "badge_no"])\
        .pipe(gen_uid, ['agency', 'uid', 'occur_year', 'occur_month', 'occur_day'], 'complaint_uid')
    return df


if __name__ == "__main__":
    df18 = clean18()
    df20 = clean20()
    ensure_data_dir("clean")
    df18.to_csv(
        data_file_path("clean/cprr_baton_rouge_so_2018.csv"),
        index=False)
    df20.to_csv(
        data_file_path("clean/cprr_baton_rouge_so_2016-2020.csv"),
        index=False)
