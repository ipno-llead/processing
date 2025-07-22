import deba
import pandas as pd
import re
import csv
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, standardize_desc_cols, clean_dates
from lib.uid import gen_uid
from datetime import datetime


def extract_case_disposition(df):
    case_disp = (
        df.reason_explanation.str.lower()
        .str.strip()
        .str.extract(r"reason: (\w+) explanation: ")
    )

    df.loc[:, "case_disposition"] = case_disp[0].str.replace(
        r"judgment", "", regex=True
    )
    return df


def extract_officer_name(df):
    names = (
        df.reason_explanation.str.lower()
        .str.strip()
        .str.replace(r"brpd (officer|employee)", "brpd", regex=True)
        .str.replace(r"police officer\,?", "brpd", regex=True)
        .str.replace(r"brpd vehicle operated by", "brpd", regex=False)
        .str.replace(r"when police officer\,", "brpd", regex=True)
        .str.replace(r"ofc\.?", "brpd", regex=True)
        .str.extract(r"brpd (\w+) (\w+) ?\((\w+)?\)?")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    df.loc[:, "badge_number"] = names[2]
    return df.drop(columns=["reason_explanation"])


def split_attorney_date_col(df):
    data = (
        df.attorney_petition_rec_incident_date.str.lower()
        .str.strip()
        .str.extract(r"attorney: (.+) petition received: (.+) incident date: (.+)")
    )

    df.loc[:, "attorney_name"] = data[0]
    df.loc[:, "claim_receive_date"] = data[1]
    df.loc[:, "claim_occur_date"] = data[2]
    return df


def split_attorney_name(df):
    names = df.attorney_name.str.replace(r"\.", "", regex=True).str.extract(
        r"(\w+) ?(?:(\w) )?(\w+)?$"
    )

    df.loc[:, "attorney_first_name"] = names[0]
    df.loc[:, "attorney_middle_name"] = names[1]
    df.loc[:, "attorney_last_name"] = names[2]
    return df.drop(columns=["attorney_name"])


def clean_claim_receive_date(df):
    df.loc[:, "claim_receive_date"] = (
        df.claim_receive_date.str.replace(
            r"(\w+) (\w+)\, (\w+)", r"\1/\2/\3", regex=True
        )
        .str.replace(r"mar", "3", regex=False)
        .str.replace(r"nov", "11", regex=False)
        .str.replace("may", "5", regex=False)
        .str.replace(r"apr", "4", regex=False)
        .str.replace(r"aug", "8", regex=False)
        .str.replace(r"sep", "9", regex=False)
        .str.replace(r"jun", "6", regex=False)
        .str.replace(r"jul", "7", regex=False)
        .str.replace(r"oct", "10", regex=False)
        .str.replace(r"dec", "12", regex=False)
    )
    return df


def clean_claim_occur_date(df):
    df.loc[:, "claim_occur_date"] = (
        df.claim_occur_date.str.replace(r"(\w+) (\w+)\, (\w+)", r"\1/\2/\3", regex=True)
        .str.replace(r"mar", "3", regex=False)
        .str.replace(r"nov", "11", regex=False)
        .str.replace(r"may", "5", regex=False)
        .str.replace(r"apr", "4", regex=False)
        .str.replace(r"aug", "8", regex=False)
        .str.replace(r"sep", "9", regex=False)
        .str.replace(r"jun", "6", regex=False)
        .str.replace(r"jul", "7", regex=False)
        .str.replace(r"oct", "10", regex=False)
        .str.replace(r"dec", "12", regex=False)
        .str.replace(r"jan", "1", regex=False)
    )
    return df.drop(columns=["attorney_petition_rec_incident_date"])


def split_suit_no_defendant_risk_col(df):
    col = (
        df.suit_no_plaintiff_defendant_risk.str.lower()
        .str.strip()
        .str.extract(r"suit no\. (.+) plaintiff: (.+) police (.+)")
    )
    df.loc[:, "case_number"] = col[0]
    df.loc[:, "case_name"] = col[1].str.replace(r":$", "", regex=True)
    df.loc[:, "risk_number"] = (
        col[2]
        .str.replace(r"(.+):(.+)", r"\2", regex=True)
        .str.replace(r"(n\/a|none)", "", regex=True)
        .str.replace(r"risk #:", "", regex=False)
    )
    return df.drop(columns=["suit_no_plaintiff_defendant_risk"])


def split_date_payment_col(df):
    col = (
        df.closed_date_amount_awarded_description_of_payment.str.lower()
        .str.strip()
        .str.extract(r"closed date:(.+) amount awarded:(.+)description(.+)")
    )
    df.loc[:, "claim_close_date"] = (
        col[0]
        .str.replace(r"^ ", "", regex=True)
        .str.replace(r"^(\w+)\, ", "", regex=True)
        .str.replace(r"(\w+) (\w+)\, (\w+)", r"\1/\2/\3", regex=True)
        .str.replace(r"mar(\w+)", "3", regex=True)
        .str.replace(r"nov(\w+)", "11", regex=True)
        .str.replace(r"may", "5", regex=True)
        .str.replace(r"apr(\w+)", "4", regex=True)
        .str.replace(r"aug(\w+)", "8", regex=True)
        .str.replace(r"sep(\w+)", "9", regex=True)
        .str.replace(r"jun(\w+)", "6", regex=True)
        .str.replace(r"jul(\w+)", "7", regex=True)
        .str.replace(r"oct(\w+)", "10", regex=True)
        .str.replace(r"dec(\w+)", "12", regex=True)
        .str.replace(r"jan(\w+)", "1", regex=True)
        .str.replace(r"feb(\w+)", "1", regex=True)
    )
    df.loc[:, "settlement_amount"] = col[1]
    df.loc[:, "settlement_amount_desc"] = col[2].str.replace(
        r"of payment:", "", regex=False
    )
    return df.drop(columns=["closed_date_amount_awarded_description_of_payment"])


# def clean():
#     df = (
#         pd.read_csv(deba.data("raw/baton_rouge_pd/baton_rouge_pd_settlements_2020.csv"))
#         .pipe(clean_column_names)
#         .pipe(extract_officer_name)
#         .pipe(extract_case_disposition)
#         .pipe(split_attorney_date_col)
#         .pipe(split_attorney_name)
#         .pipe(clean_claim_receive_date)
#         .pipe(clean_claim_occur_date)
#         .pipe(split_suit_no_defendant_risk_col)
#         .pipe(split_date_payment_col)
#         .pipe(set_values, {"agency": "baton-rouge-pd"})
#         .pipe(gen_uid, ["first_name", "last_name", "agency"])
#         .pipe(
#             gen_uid,
#             ["case_name", "case_number", "risk_number", "settlement_amount", "uid"],
#             "settlement_uid",
#         )
#     )
#     return df

import re
import pandas as pd

def extract_fields(df: pd.DataFrame) -> pd.DataFrame:
    case_numbers = []
    case_names = []
    risk_numbers = []
    first_names = []
    last_names = []

    col = df["suit_no_plaintiff_defendant_risk"]

    for entry in col:
        entry = str(entry).strip().strip("'")

        case_number_match = re.search(r"Suit No\.?\s*(.*?)\s+Plaintiff:", entry)
        case_number = case_number_match.group(1) if case_number_match else None
        case_numbers.append(case_number)

        case_name_match = re.search(r"Plaintiff:.*?Defendant", entry)
        case_name = case_name_match.group(0).strip() if case_name_match else None
        case_names.append(case_name)

        risk_number_match = re.search(r"Risk\s+#:\s*(\S+)", entry)
        raw_risk_number = risk_number_match.group(1) if risk_number_match else None
        risk_number = None if raw_risk_number == "N/A" else raw_risk_number
        risk_numbers.append(risk_number)

        defendant_match = re.search(r"VS\s+(.*?)(?:, et al| Defendant)", entry)
        if defendant_match:
            full_defendant = defendant_match.group(1).strip()
            name_parts = full_defendant.split(",")
            if len(name_parts) == 2:
                last_name = name_parts[0].strip()
                first_name = name_parts[1].strip()
            else:
                first_name = last_name = None
        else:
            first_name = last_name = None

        first_names.append(first_name)
        last_names.append(last_name)

    df = df.copy()
    df["case_number"] = case_numbers
    df["case_name"] = case_names
    df["first_name"] = first_names
    df["last_name"] = last_names
    df["risk_number"] = risk_numbers

    return df.drop(columns=["suit_no_plaintiff_defendant_risk"])

def extract_attorney_and_dates(df: pd.DataFrame) -> pd.DataFrame:
    first_names = []
    middle_names = []
    last_names = []
    receive_dates = []
    occur_dates = []

    col = df["attorney_petition_rec_incident_date"]

    for entry in col:
        entry = str(entry).strip().strip("'")

        if "Attorney/Petition Rec." in entry:
            first_names.append(None)
            middle_names.append(None)
            last_names.append(None)
            receive_dates.append(None)
            occur_dates.append(None)
            continue

        attorney_match = re.search(
            r"Attorney:\s+([\w'\-]+)(?:\s+([\w'\-\.]+))?\s+([\w'\-]+)", entry
        )
        if attorney_match:
            first_name = attorney_match.group(1).strip()
            middle_name = attorney_match.group(2).strip() if attorney_match.group(2) else ""
            last_name = attorney_match.group(3).strip()
        else:
            first_name = middle_name = last_name = None

        receive_match = re.search(r"Petition Received:\s+([A-Za-z]{3} \d{2}, \d{4})", entry)
        if receive_match:
            try:
                receive_date = datetime.strptime(receive_match.group(1), "%b %d, %Y").strftime("%m/%d/%Y")
            except ValueError:
                receive_date = None
        else:
            receive_date = None

        occur_match = re.search(r"Incident Date:\s+([A-Za-z]{3} \d{2}, \d{4})", entry)
        if occur_match:
            try:
                occur_date = datetime.strptime(occur_match.group(1), "%b %d, %Y").strftime("%m/%d/%Y")
            except ValueError:
                occur_date = None
        else:
            occur_date = None

        first_names.append(first_name)
        middle_names.append(middle_name)
        last_names.append(last_name)
        receive_dates.append(receive_date)
        occur_dates.append(occur_date)

    df = df.copy()
    df["attorney_first_name"] = first_names
    df["attorney_middle_name"] = middle_names
    df["attorney_last_name"] = last_names
    df["claim_receive_date"] = receive_dates
    df["claim_occur_date"] = occur_dates

    return df.drop(columns=["attorney_petition_rec_incident_date"])

def extract_settlement_fields(df: pd.DataFrame) -> pd.DataFrame:
    close_dates = []
    settlement_amounts = []
    settlement_descriptions = []

    col = df["closed_date_amount_awarded_description_of_payment"]

    for entry in col:
        entry = str(entry).strip().strip("'")

        close_match = re.search(r"Closed Date:\s+\w+,\s+([A-Za-z]+ \d{2}, \d{4})", entry)
        if close_match:
            try:
                close_date = datetime.strptime(close_match.group(1), "%B %d, %Y").strftime("%m/%d/%Y")
            except ValueError:
                close_date = ''
        else:
            close_date = ''
        close_dates.append(close_date)

        amount_match = re.search(r"Amount Awarded:\s+\$([0-9,]+\.\d{2})", entry)
        amount = amount_match.group(1) if amount_match else ''
        settlement_amounts.append(amount)

        desc_match = re.search(r"Description of Payment:\s+(.*)", entry)
        desc = desc_match.group(1).strip() if desc_match else ''
        settlement_descriptions.append(desc)

    df = df.copy()
    df["claim_close_date"] = close_dates
    df["settlement_amount"] = settlement_amounts
    df["settlement_amount_desc"] = settlement_descriptions

    return df.drop(columns=["closed_date_amount_awarded_description_of_payment"])

def extract_case_disposition(df: pd.DataFrame) -> pd.DataFrame:
    dispositions = []
    descriptions = []

    col = df["reason_explanation"]

    for entry in col:
        entry = str(entry).strip().strip("'")

        reason_match = re.search(r"Reason:\s*(\w+)", entry)
        disposition = reason_match.group(1) if reason_match else None
        dispositions.append(disposition)

        explanation_match = re.search(r"Explanation:\s*(.*)", entry)
        description = explanation_match.group(1).strip() if explanation_match else None
        descriptions.append(description)

    df = df.copy()
    df["case_disposition"] = dispositions
    df["disposition_desc"] = descriptions

    return df.drop(columns=["reason_explanation"])

def fix_first_name(df: pd.DataFrame) -> pd.DataFrame:
    def fix(name):
        name = str(name).strip().lower()

        if name == "officer nathan etal":
            return "nathan"
        elif name == "ltd":
            return ""
        elif name == "c urtis":
            return "curtis"
        elif name == "officer isaac":
            return "isaac"
        else:
            return name.title() if name else ""

    df = df.copy()
    df["first_name"] = df["first_name"].apply(fix)
    return df

def strip_double_quotes(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: x.strip('"') if isinstance(x, str) else x)

def remove_commas_from_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: x.replace(",", "") if isinstance(x, str) else x)


def clean_16():
    df16 = (
        pd.read_csv(deba.data("raw/baton_rouge_pd/baton_rouge_pd_settlements_2016_2023.csv"))
        .pipe(clean_column_names)
        .pipe(extract_fields)
        .pipe(extract_attorney_and_dates)
        .pipe(extract_settlement_fields)
        .pipe(extract_case_disposition)
        .pipe(fix_first_name)
        .pipe(clean_names, ["first_name", "last_name", "attorney_first_name", "attorney_middle_name", "attorney_last_name"])
        .pipe(standardize_desc_cols, ["case_name", "case_number", "risk_number", "settlement_amount", "settlement_amount_desc", "case_disposition", "disposition_desc"])
        .pipe(clean_dates, ["claim_receive_date", "claim_occur_date", "claim_close_date"])
        .pipe(set_values, {"agency": "baton-rouge-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["case_name", "case_number", "risk_number", "settlement_amount", "uid"],
            "settlement_uid",
        )

        .pipe(remove_commas_from_strings)
        .pipe(strip_double_quotes)
    )
    return df16


if __name__ == "__main__":
    # df = clean()
    df16 = clean_16()
    # df.to_csv(deba.data("clean/settlements_baton_rouge_pd_2020.csv"), index=False)
    df16.to_csv(deba.data("clean/settlements_baton_rouge_pd_2016_2023.csv"), index=False, quoting=csv.QUOTE_MINIMAL)

