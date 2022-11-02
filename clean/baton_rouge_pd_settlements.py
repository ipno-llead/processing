import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


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


def clean():
    df = (
        pd.read_csv(deba.data("raw/baton_rouge_pd/baton_rouge_pd_settlements_2020.csv"))
        .pipe(clean_column_names)
        .pipe(extract_case_disposition)
        .pipe(extract_officer_name)
        .pipe(split_attorney_date_col)
        .pipe(split_attorney_name)
        .pipe(clean_claim_receive_date)
        .pipe(clean_claim_occur_date)
        .pipe(split_suit_no_defendant_risk_col)
        .pipe(split_date_payment_col)
        .pipe(set_values, {"agency": "baton-rouge-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["case_name", "case_number", "risk_number", "settlement_amount", "uid"],
            "settlement_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/settlements_baton_rouge_pd_2020.csv"), index=False)
