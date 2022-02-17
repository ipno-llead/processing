from lib.clean import standardize_desc_cols
import pandas as pd
import dirk
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates
import re
from lib.rows import duplicate_row
from lib.uid import gen_uid


def clean_tracking_number(df):
    df.loc[:, "tracking_number"] = df.file.str.replace(r"-$", "", regex=True)
    return df.drop(columns="file")


def clean_incident_dates(df):
    df.loc[:, "incident_date"] = (
        df.date_of_incident.str.lower()
        .str.strip()
        .str.replace("juune 13, 2018", "06/13/2018", regex=False)
        .str.replace("january-august 2017", "", regex=False)
    )
    return df.drop(columns="date_of_incident")


def clean_allegation(df):
    df.loc[:, "allegation_desc"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace("dspd", "denham springs police department", regex=False)
    )
    return df.drop(columns="allegation")


def split_rows_with_multiple_dispositions(df):
    df.loc[:, "disposition"] = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace("unsistained", "unsustained", regex=False)
        .str.replace("unsustained dspd", "unsustained/dspd")
        .str.replace("sustained dspd", "sustained/dspd", regex=False)
        .str.replace("none dspd", "none/dspd", regex=False)
    )

    i = 0
    for idx in df[df.disposition.str.contains("/")].index:
        s = df.loc[idx + i, "disposition"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "disposition"] = name
        i += len(parts) - 1
    return df.drop(columns="outcome")


def extract_allegation_from_disposition(df):
    df.loc[:, "allegation"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"\-? ?(sustained|unsustained|none)", "", regex=True)
        .str.replace(r"\-", "", regex=True)
        .str.replace("cleared of any wrongdoing involing the incident", "", regex=False)
        .str.replace(r"no\.?", "", regex=True)
        .str.replace(r"gee?ne?r?al", "general", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace("dsps", "dspd", regex=False)
        .str.replace("dspd", "denham springs police department", regex=False)
        .str.replace(r"(\w+)  +(\w+)$", r"\1 \2", regex=True)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"(.+) ?\- ?(.+)", r"\2", regex=True)
        .str.replace("007", "", regex=False)
        .str.replace("none", "", regex=False)
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = (
        df.disciplinary_outcome.str.lower()
        .str.strip()
        .str.replace(r"^none$", "", regex=True)
        .str.replace("none, resigned", "resigned", regex=False)
        .str.replace("one", "1", regex=False)
        .str.replace(r"(for | calendar)", "", regex=True)
        .str.replace(r"suspended (\w+) days?", r"\1-day suspension", regex=True)
        .str.replace(" and", ";", regex=False)
        .str.replace(r"\,", ";", regex=True)
        .str.replace("dspd policy order no. 320-sustained", "", regex=False)
        .str.replace(r"^n1; ", "", regex=True)
        .str.replace("; 24 hour", "; 24-hour", regex=False)
        .str.replace(
            r"loss of (\w+)\.(\w+) hours ?(of)? ?pay",
            r"\1.\2-hours loss of pay",
            regex=True,
        )
        .str.replace(
            r"loss of (\w+) hours ?(of)? ?pay", r"\1-hours loss of pay", regex=True
        )
        .str.replace(
            r"loss of (\w+) days? ?(of)? ?seniority",
            r"\1-day loss of seniority",
            regex=True,
        )
        .str.replace("loss of 1-day seniority", "1-day loss of seniority", regex=False)
    )
    return df.drop(columns="disciplinary_outcome")


def clean_names(df):
    df.loc[:, "officer_name"] = (
        df.officer_name.fillna("")
        .str.lower()
        .str.strip()
        .str.replace(r"sgt\.?", "sergeant", regex=True)
        .str.replace(r"lt\.?", "lieutenant", regex=True)
    )

    names = df.officer_name.str.extract(
        r"(officer|lieutenant|sergeant|reserve officer) (\w+)\.? (\w+)"
    )

    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns="officer_name")


def clean():
    df = (
        pd.read_csv(
            dirk.data("raw/denham_springs_pd/denham_springs_pd_cprr_2016_2021.csv")
        )
        .pipe(clean_column_names)
        .pipe(clean_tracking_number)
        .pipe(clean_incident_dates)
        .pipe(clean_allegation)
        .pipe(clean_names)
        .pipe(split_rows_with_multiple_dispositions)
        .pipe(extract_allegation_from_disposition)
        .pipe(clean_disposition)
        .pipe(clean_action)
        .pipe(clean_dates, ["incident_date"])
        .pipe(standardize_desc_cols, ["tracking_number"])
        .pipe(set_values, {"agency": "Denham Springs PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "allegation_desc", "disposition", "action"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(dirk.data("clean/cprr_denham_springs_pd_2016_2021.csv"), index=False)
