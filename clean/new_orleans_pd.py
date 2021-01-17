from lib.columns import clean_column_names
from lib.match_records import gen_uid, concat_dfs
from lib.path import data_file_path
import pandas as pd
import re
import os
import sys
sys.path.append("../")


def clean_yearly_pprr(orig_df):
    df = orig_df[[
        "Birthdate - Personal Dta", "Department ID - Job Dta", "Empl ID",
        "Badge Nbr", "Job Code - Job Dta", "Description - Job Code Info"
    ]]
    name_df = orig_df["Name - Personal Dta"].str.split(",", expand=True)
    df.loc[:, "Last Name"] = name_df.iloc[:, 0].str.replace(r" +", " ")
    name_df = name_df.iloc[:, 1].str.split(" ", expand=True)
    df.loc[:, "First Name"] = name_df.iloc[:, 0]
    df.loc[:, "Middle Name"] = name_df.iloc[:, 1].fillna("")
    df.rename(columns={
        "Birthdate - Personal Dta": "Birth Year",
        "Department ID - Job Dta": "Department #",
        "Badge Nbr": "Badge #",
        "Job Code - Job Dta": "Rank Number #",
        "Description - Job Code Info": "Rank",
        "Empl ID": "Employee ID #"
    }, inplace=True)
    df.loc[:, "Employee ID #"] = df["Employee ID #"].astype(
        "str").str.rjust(6, "0")
    df.loc[:, "Badge #"] = df["Badge #"].fillna("0").astype("str")\
        .str.replace(r"\.0+$", "").str.strip().str.replace(r"^$", "0")\
        .str.rjust(5, "0").str.replace("00000", "")
    df.loc[:, "Birth Year"] = df["Birth Year"].astype("str")
    df.loc[:, "Rank"] = df["Rank"].str.title()
    return df


def clean_2018_pprr():
    pprr18 = pd.read_csv(
        "../data/New Orleans PD/Administrative Data/New Orleans_PD_PPRR_2018.csv")
    pprr18.columns = [v.strip() for v in pprr18.columns]
    name_df = pprr18["Name"].str.split(", ", expand=True)
    pprr18 = pprr18[["Birthdate - Personal Dta", "Description", "Badge No."]]
    pprr18.rename(columns={
        "Birthdate - Personal Dta": "Birth Year",
        "Badge No.": "Badge #",
        "Description": "Rank",
    }, inplace=True)
    pprr18.loc[:, "Last Name"] = name_df.iloc[:, 0]
    name_df = name_df.iloc[:, 1].str.split(" ", expand=True)
    pprr18.loc[:, "First Name"] = name_df.iloc[:, 0].str.replace(r"^,", "")\
        .str.replace(r"\W", "").str.strip().str.replace(r"^l", "I")
    pprr18.loc[:, "Middle Name"] = name_df.iloc[:, 1].str.title().fillna("")
    pprr18.loc[:, "Birth Year"] = pprr18["Birth Year"].str.replace(
        "/", "", regex=False).str.title().str.strip().str.rjust(4, "1").fillna("")
    pprr18.loc[:, "Rank"] = pprr18["Rank"].str.title()
    pprr18.loc[:, "Badge #"] = pprr18["Badge #"].str.rjust(5, "0").fillna("")
    pprr18.drop_duplicates(inplace=True, ignore_index=True)
    id_cols = ["First Name", "Middle Name", "Last Name", "Birth Year"]
    pprr18 = gen_uid(pprr18, id_cols)

    personel_18 = pprr18[["UID", "First Name", "Last Name", "Middle Name",
                          "Birth Year"]].drop_duplicates(ignore_index=True)

    history_18 = pprr18[["UID", "Rank", "Badge #"]
                        ].drop_duplicates(ignore_index=True)
    history_18.loc[:, "Year"] = "2018"
    history_18.loc[:, "Police Department"] = "New Orleans PD"

    return personel_18, history_18


def read_csv_files():
    prefix = data_file_path("new_orleans_pd")
    pat = re.compile(r".+_\d{4}\.csv$")
    csv_files = [name for name in os.listdir(
        prefix) if pat.match(name) is not None]
    input_dfs = {name: pd.read_csv(os.path.join(prefix, name))
                 for name in csv_files}
    for k, df in input_dfs.items():
        input_dfs[k] = clean_column_names(df)
    return input_dfs


def clean_csvs(input_dfs):
    personel_dfs = []
    hist_dfs = []
    id_cols = ["First Name", "Middle Name", "Last Name", "Birth Year"]
    hist_cols = ["UID", "Department #", "Badge #", "Rank Number #", "Rank"]
    year_pat = re.compile(r"^.+_(\d{4})\.xls")
    for name, df1 in input_dfs.items():
        df2 = clean_yearly_pprr(df1)
        df2 = gen_uid(df2, id_cols)
        hist_df = df2[hist_cols]
        hist_df.loc[:, "Year"] = year_pat.match(name).group(1)
        hist_df.loc[:, "Police Department"] = "New Orleans PD"
        hist_dfs.append(hist_df)
        personel_dfs.append(
            df2[["UID", "Last Name", "Middle Name", "First Name", "Birth Year", "Employee ID #"]])

    history = pd.concat(hist_dfs)
    history.drop_duplicates(inplace=True, ignore_index=True)
    personel = pd.concat(personel_dfs)
    personel.drop_duplicates(inplace=True, ignore_index=True)
    return history, personel


def clean_personel():
    input_dfs = read_csv_files()
    history, personel = clean_csvs(input_dfs)
    personel_18, history_18 = clean_2018_pprr()
    history = concat_dfs([history, history_18])
    personel = personel.merge(personel_18, on=[
                              "UID", "Last Name", "Middle Name", "First Name", "Birth Year"], how="outer")
    return history, personel
