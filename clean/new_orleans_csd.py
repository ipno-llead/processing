import datetime
from lib.columns import (
    rearrange_personel_columns, rearrange_personel_history_columns
)
from lib.match_records import gen_uid
from lib.excel import remove_unnamed_cols
from lib.date import to_datetime_series
from lib.name import clean_name
import pandas as pd
import sys
sys.path.insert(0, "../")


def clean_personel_2014():
    df1 = pd.read_excel(
        "../data/New Orleans Civil Service Department/Administrative Data/New Orleans_CSD_PPRR_2014.xls")
    df2 = df1[["ORGN Desc", "Last Name", "First Name", "Birth Date", "Hire Date",
               "Title Code", "Title Desc", "Termination Date", "Pay Prog Start Date", "Annual Salary"]]
    df2.rename(columns={
        "ORGN Desc": "Department",
        "Title Code": "Rank Number #",
        "Title Desc": "Rank",
    }, inplace=True)
    for col in ["Last Name", "First Name"]:
        df2.loc[:, col] = df2[col].map(clean_name, na_action="ignore")
    for col in ["Birth Date", "Hire Date", "Termination Date", "Pay Prog Start Date"]:
        df2.loc[:, col] = to_datetime_series(df2[col])
    df2.loc[:, "Department"] = df2["Department"].str.replace(r"^POL ", "")
    # df2 = gen_uid(df2, ["Last Name", "First Name", "Birth Date"])
    # personel = rearrange_personel_columns(df2)
    # history = rearrange_personel_history_columns(df2)
    # return history, personel
    return df2


def clean_personel_2009():

    def convert_date(s):
        if len(s) > 0:
            m, d, y = s.split("/")
            return datetime.date(int(y), int(m), int(d))
        return s

    df1 = pd.read_csv(
        "../data/New Orleans Civil Service Department/Administrative Data/New-Orleans_CSD_PPRR_2009.csv")
    df1 = remove_unnamed_cols(df1)
    df1.loc[:, "TERM DATE"] = df1["TERM DATE"].fillna("").map(convert_date)
    df1.loc[:, "PAY PROG START DATE"] = df1["PAY PROG START DATE"].fillna(
        "").map(convert_date)
    df1.rename(columns={name: name.title()
                        for name in df1.columns}, inplace=True)
    df2 = df1[["Orgn Desc", "First Name", "Last Name", "Titl E Code",
               "Title Desc", "Term Date", "Pay Prog Start Date", "Annual Salary"]]
    df2.rename(columns={
        "Term Date": "Termination Date",
        "Orgn Desc": "Department",
        "Titl E Code": "Rank Number #",
        "Title Desc": "Rank",
    }, inplace=True)
    for col in ["Last Name", "First Name"]:
        df2.loc[:, col] = df2[col].map(clean_name, na_action="ignore")
    df2.loc[:, "Annual Salary"] = df2["Annual Salary"].str.strip()\
        .str.replace(r"\$|,", "").astype("float64").map('{:.2f}'.format)
    # df2 = gen_uid(df2, ["Last Name", "First Name"])
    # personel = rearrange_personel_columns(df2)
    # history = rearrange_personel_history_columns(df2)
    # return history, personel
    return df2
