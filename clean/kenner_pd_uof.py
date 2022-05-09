import pandas as pd
import deba
from lib.columns import clean_column_names
from lib.clean import clean_dates
from lib.uid import gen_uid


def split_name(df):
    df.loc[:, "officer"] = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"(\w+), (\w{2}).,", r"\1 \2,", regex=True)
        .str.replace(r"\.", "", regex=True)
    )
    names = df.officer.str.extract(r"(?:(\w+)) ?(?:(jr|iii|sr))?, (?:(\w+)) ?(.+)?")
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "suffix"] = names[1]
    df.loc[:, "first_name"] = names[2]
    df.loc[:, "middle_name"] = names[3]
    df.loc[:, "last_name"] = df.last_name.fillna("") + " " + df.suffix.fillna("")
    return df.drop(columns=["suffix", "officer"])


def assign_action(df):
    actions = (
        df.disposition.str.lower()
        .str.strip()
        .str.extract(r"(suspended|verbal counseling)")
    )
    df.loc[:, "action"] = actions[0].fillna("")
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace("non-sustained", "not sustained", regex=False)
        .str.replace(r"(\w+)-not", r"\1/not", regex=True)
        .str.replace("suspended", "", regex=False)
        .str.replace("verbal counseling", "", regex=False)
    )
    return df


def clean_charges(df):
    df.loc[:, "charges"] = (
        df.alleged_violation.str.lower()
        .str.strip()
        .str.replace(", ", "/", regex=False)
        .str.replace("alleged ", "", regex=False)
        .str.replace(
            r"^(dr)? ?(20-2)? ?(art)? ?(32.1)? ?(-)? ?inappropriate use of ?(phys)? force$",
            "dr 20-2 art 32.1 - inappropriate use of physical force",
            regex=True,
        )
        .str.replace("proced", "procedure", regex=False)
        .str.replace(r"(\w+) (\w+)/use of force", r"use of force/\1 \2", regex=True)
        .str.replace(r"(\w+)/use of force/(\w+)", r"use of force/\1/\2", regex=True)
        .str.replace(r"(\w+) (\w+)/excess force", r"excessive force/\1 \2", regex=True)
        .str.replace(
            r"^(fsop)? ?(7-3)? ?-? ?use of force continuum$",
            "fsop 7-3 (b) - use of force continuum",
            regex=True,
        )
        .str.replace("general: ", "", regex=False)
        .str.replace(r"^\(|\)$", "", regex=True)
    )
    return df.drop(columns="alleged_violation")


def assign_agency(df):
    df.loc[:, "agency"] = "Kenner PD"
    return df


def clean_uof():
    df = (
        pd.read_csv(deba.data("raw/kenner_pd/kenner_pd_uof_2005_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "incident_date", "address": "location"})
        .pipe(clean_dates, ["incident_date"])
        .pipe(assign_action)
        .pipe(clean_disposition)
        .pipe(clean_charges)
        .pipe(assign_agency)
        .pipe(
            gen_uid,
            [
                "disposition",
                "action",
                "charges",
                "location",
                "incident_year",
                "incident_month",
                "incident_day",
            ],
            "uof_uid",
        )
    )
    return df


def extract_officer(uof):
    df = (
        uof.loc[
            :,
            [
                "officer",
                "agency",
                "uof_uid",
            ],
        ]
        .pipe(split_name)
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
    )
    uof = uof.drop(columns=["officer"])
    return df, uof


if __name__ == "__main__":
    uof = clean_uof()
    uof_officer, uof = extract_officer(uof)
    uof.to_csv(deba.data("clean/uof_kenner_pd_2005_2021.csv"), index=False)
    uof_officer.to_csv(
        deba.data("clean/uof_officers_kenner_pd_2005_2021.csv"), index=False
    )
