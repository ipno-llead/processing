from lib.match_records import gen_uid
from lib.columns import (
    rearrange_personel_columns, rearrange_personel_history_columns
)
from lib.excel import remove_unnamed_cols
from lib.date import to_datetime_series
from lib.name import clean_name
import pandas as pd
import sys
sys.path.append("../")


def read_excel_file():
    df = pd.read_excel(
        "../data/New Orleans Harbor PD/Administrative Data/New Orleans_Harbor PD_PPRR_2020.xlsx")
    df = remove_unnamed_cols(df)
    df.rename(columns={name: " ".join(name.title().split())
                       for name in df.columns}, inplace=True)
    return df.dropna(how="all")


def clean_personel():
    df = read_excel_file()
    df2 = df[["First Name", "Last Name", "Position/Rank", "Date Hired",
              "Term Date", "Hourly Pay", "Mi", "Pay Effective Date"]]
    df2.rename(columns={
        "Term Date": "Termination Date",
        "Date Hired": "Hire Date",
        "Position/Rank": "Rank",
        "Hourly Pay": "Hourly Salary",
        "Mi": "Middle Name"
    }, inplace=True)
    for col in ["Last Name", "Middle Name", "First Name"]:
        df2.loc[:, col] = df2[col].map(clean_name, na_action="ignore")
    for col in ["Termination Date", "Hire Date"]:
        df2.loc[:, col] = to_datetime_series(df2[col])
    df2 = gen_uid(df2, ["First Name", "Last Name",
                        "Middle Name", "Termination Date", "Hire Date"])
    personel = rearrange_personel_columns(df2)
    history = rearrange_personel_history_columns(df2)
    return history, personel
