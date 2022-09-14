import pandas as pd
import deba
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols


def drop_rows_missing_names(df):
    df.loc[:, "police_officer"] = df.police_officer.fillna("")
    return df[~((df.police_officer == ""))]


def clean_injury(df):
    df.loc[:, "cause_of_injury"] = (
        df.cause_of_injury.str.lower()
        .str.strip()
        .str.replace(r"(\w+)- ?(\w+)", r"\1 \2", regex=True)
        .str.replace(r" w\/ ?(\w+)", r" with \1", regex=True)
        .str.replace(
            r"^struck - motor vehicle$",
            "vehicle collision with another vehicle",
            regex=True,
        )
    )
    return df


def clean_accident_desc(df):
    df.loc[:, "accident_desc"] = (
        df.accident_desc.str.lower()
        .str.strip()
        .str.replace(r"\bunit\b", "vehicle", regex=True)
        .str.replace(
            r"(\bi\/?v\b|\binsured vehicle\b)",
            "insured/new orleans police department vehicle",
            regex=True,
        )
        .str.replace(r"\bcv\b", "citizen's vehicle", regex=True)
    )
    return df


def split_names(df):
    names = (
        df.police_officer.str.lower()
        .str.strip()
        .str.replace(r"^n\/a.+", "", regex=True)
        .str.replace(r"^unknown(.+)", "", regex=True)
        .str.extract(r"(\w+) (\w+)\,? ?(\w+)?\.?")
    )

    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "last_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")
    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["police_officer", "suffix"])[
        ~((df.first_name == "") & (df.last_name == " "))
    ]


def clean_bad_dates(df):
    for col in df.columns:
        if col.endswith("date"):
            df[col] = df[col].str.replace(r"^0$", "", regex=True)
    return df


def clean21():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_pclaims_2021.csv"))
        .pipe(clean_column_names)
        .drop(
            columns=[
                "mailaddressline1",
                "mailcity",
                "mailstate",
                "mailpostalcode",
                "namefirst",
                "namelast",
                "chd1",
            ]
        )
        .pipe(drop_rows_missing_names)
        .rename(
            columns={
                "claim_id": "tracking_id",
                "claimtypeid": "claim_type",
                "dateoccurrence": "claim_occur_date",
                "dateclaimmade": "claim_made_date",
                "datereceived": "claim_receive_date",
                "dateclosed": "claim_close_date",
                "claimstatus": "claim_status",
                "clientname": "client_name",
                "tcurrperiod": "total_current_period",
                "tpaid": "total_paid",
                "topen": "total_open",
                "tsub": "total_sub",
                "tincurred": "total_incurred",
            }
        )
        .pipe(clean_injury)
        .pipe(clean_accident_desc)
        .pipe(split_names)
        .pipe(
            standardize_desc_cols,
            [
                "claim_status",
                "client_name",
                "accident_desc",
                "jurisdiction",
                "accident_location",
                "claim_type",
            ],
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["tracking_id", "agency"],
            "property_claims_uid",
        )
    )
    return df


def clean20():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_auto_2020.csv"))
        .pipe(clean_column_names)
        .drop(
            columns=[
                "mailaddressline1",
                "mailcity",
                "mailstate",
                "mailpostalcode",
                "chd1",
            ]
        )
        .pipe(drop_rows_missing_names)
        .rename(
            columns={
                "claim_id": "tracking_id",
                "claimtypeid": "claim_type",
                "dateoccurrence": "claim_occur_date",
                "dateclaimmade": "claim_made_date",
                "datereceived": "claim_receive_date",
                "dateclosed": "claim_close_date",
                "claimstatus": "claim_status",
                "clientname": "client_name",
                "tcurrperiod": "total_current_period",
                "tpaid": "total_paid",
                "topen": "total_open",
                "tsub": "total_sub",
                "tincurred": "total_incurred",
            }
        )
        .pipe(
            clean_dates,
            [
                "claim_occur_date",
                "claim_made_date",
                "claim_receive_date",
                "claim_close_date",
            ],
        )
        .pipe(clean_injury)
        .pipe(clean_accident_desc)
        .pipe(split_names)
        .pipe(
            standardize_desc_cols,
            [
                "claim_status",
                "client_name",
                "accident_desc",
                "jurisdiction",
                "accident_location",
                "claim_type",
            ],
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["tracking_id", "agency"],
            "property_claims_uid",
        )
    )
    return df


if __name__ == "__main__":
    df20 = clean20()
    df21 = clean21()
    df20.to_csv(deba.data("clean/pclaims_new_orleans_pd_2020.csv"), index=False)
    df21.to_csv(deba.data("clean/pclaims_new_orleans_pd_2021.csv"), index=False)
