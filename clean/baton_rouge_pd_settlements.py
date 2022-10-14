import deba
import pandas as pd
from lib.columns import clean_column_names

def extract_case_disposition(df):
    case_disp = df.reason_explanation.str.lower().str.strip()\
        .str.extract(r"reason: (\w+) explanation: ")
    
    df.loc[:, "case_disposition"] = case_disp[0].str.replace(r"judgment", "", regex=True)
    return df 

def clean_reason(df):
    df.loc[:, "case_desc"] = df.reason_explanation.str.lower().str.strip()\
        .str.replace(r"reason\: (\w+) explanation\: ", "", regex=True)
    return df.drop(columns=["reason_explanation"])

def split_attorney_date_col(df):
    data = df.attorney_petition_rec_incident_date.str.lower().str.strip()\
        .str.extract(r"attorney: (.+) petition received: (.+) incident date: (.+)")
    
    df.loc[:, "attorney_name"] = data[0]
    df.loc[:, "petition_receive_date"] = data[1]
    df.loc[:, "incident_date"] = data[2]
    return df

def split_attorney_name(df):
    names = df.attorney_name.str.replace(r"\.", "", regex=True).str.extract(r"(\w+) ?(?:(\w) )?(\w+)?$")

    df.loc[:, "attorney_first_name"] = names[0]
    df.loc[:, "attorney_middle_name"] = names[1]
    df.loc[:, "attorney_last_name"] = names[2]
    return df.drop(columns=["attorney_name"])


def clean_petition_receive_date(df):
    df.loc[:, "petition_receive_date"] = df.petition_receive_date\
        .str.replace(r"(\w+) (\w+)\, (\w+)", r"\1/\2/\3", regex=True)\
        .str.replace(r"mar", "3", regex=False).str.replace(r"nov", "11", regex=False)\
        .str.replace("may", "5", regex=False).str.replace(r"apr", "4", regex=False)\
        .str.replace(r"aug", "8", regex=False).str.replace(r"sep", "9", regex=False)\
        .str.replace(r"jun","6", regex=False).str.replace(r"jul", "7", regex=False)\
        .str.replace(r"oct", "10", regex=False).str.replace(r"dec", "12", regex=False)
    return df


def clean_incident_date(df):
    df.loc[:, "incident_date"] = df.incident_date\
        .str.replace(r"(\w+) (\w+)\, (\w+)", r"\1/\2/\3", regex=True)\
        .str.replace(r"mar", "3", regex=False).str.replace(r"nov", "11", regex=False)\
        .str.replace("may", "5", regex=False).str.replace(r"apr", "4", regex=False)\
        .str.replace(r"aug", "8", regex=False).str.replace(r"sep", "9", regex=False)\
        .str.replace(r"jun","6", regex=False).str.replace(r"jul", "7", regex=False)\
        .str.replace(r"oct", "10", regex=False).str.replace(r"dec", "12", regex=False)\
        .str.replace(r"jan", "1", regex=False)
    return df.drop(columns=["attorney_petition_rec_incident_date"])
    

def split_suit_no_defendant_risk_col(df):
    col = df.suit_no_plaintiff_defendant_risk.str.lower().str.strip()\
        .str.extract(r"suit no\. (.+) plaintiff: (.+) police (.+)")
    df.loc[:, "case_number"] = col[0]
    df.loc[:, "case_name"] = col[1]
    return df


def clean():
    df = pd.read_csv(deba.data("raw/baton_rouge_pd/baton_rouge_pd_settlements_2020.csv"))\
        .pipe(clean_column_names)\
        .pipe(extract_case_disposition)\
        .pipe(clean_reason)\
        .pipe(split_attorney_date_col)\
        .pipe(split_attorney_name)\
        .pipe(clean_petition_receive_date)\
        .pipe(clean_incident_date)\
        .pipe(split_suit_no_defendant_risk_col)
    return df