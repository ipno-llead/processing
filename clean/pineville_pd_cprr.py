import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, clean_dates
from lib.uid import gen_uid
from lib.rows import duplicate_row
import re


def sanitize_dates(df):
    df.loc[:, "incident_date"] = (
        df.date_of_incident.astype(str)
        .str.replace(r"^\'", "", regex=True)
        .str.replace(r"\(|\)", "", regex=True)
    )
    return df.drop(columns=["date_of_incident"])


def clean_action(df):
    df.loc[:, "action"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"\'", "", regex=True)
        .str.replace(r"^(\w{1,2}) days?", r"\1-day", regex=True)
    )
    return df.drop(columns=["disposition"])


def split_rows_with_multiple_officers(df):
    i = 0
    for idx in df[df.officer_name.str.contains("/")].index:
        s = df.loc[idx + i, "officer_name"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer_name"] = name
        i += len(parts) - 1
    return df


def clean_names(df):
    names = (
        df.officer_name.str.lower()
        .str.strip()
        .str.replace(r"\'", "", regex=True)
        .str.extract(r"(^co\b|pfc\.|ptn\.|sgt\.)? ?(?:(\w+) )? ?(\w+)")
    )

    df.loc[:, "rank_desc"] = (
        names[0]
        .fillna("")
        .str.replace(r"^co$", "communications office", regex=True)
        .str.replace(r"^sgt\.$", "sergeant", regex=True)
        .str.replace(r"^pfc\.$", "patrolman first class", regex=True)
        .str.replace(r"^ptn\.$", "patrolman", regex=True)
    )
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns=["officer_name"])


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"\'", "", regex=True)
        .str.replace(r" \(report\)", "", regex=True)
        .str.replace(r"^automobileaccident$", "automobile accident", regex=True)
        .str.replace(r"^off duty", "off-duty", regex=True)
        .str.replace(r"^misconduct \(off-duty\)$", "off-duty misconduct", regex=True)
        .str.replace(r"^officer ", "", regex=True)
    )
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/pineville_pd/pineville_pd_cprr_2015_2019.csv"))
        .pipe(clean_column_names)
        .rename(columns={"case_number": "tracking_id"})
        .pipe(sanitize_dates)
        .pipe(clean_dates, ["incident_date"])
        .pipe(clean_action)
        .pipe(split_rows_with_multiple_officers)
        .pipe(clean_names)
        .pipe(clean_allegation)
        .pipe(set_values, {"agency": "pineville-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "tracking_id", "allegation"], "allegation_uid")
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_pineville_pd_2015_2019.csv"), index=False)
