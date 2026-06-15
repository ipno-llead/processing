import pandas as pd
import deba
from lib.clean import clean_dates, clean_sexes, clean_races, standardize_desc_cols
from lib.columns import set_values
from lib.uid import gen_uid
import re


def split_officers(df):
    """Split officers_involved into individual rows.

    Format: '1881 Billiot, Devin (SJPSO) 1647 - Anthony, Stevie (SJPSO)'
    Pattern: badge [- ] Last, First [suffix] (SJPSO)
    """
    rows = []
    for idx, row in df.iterrows():
        raw = row["officers_involved"]
        if pd.isna(raw) or str(raw).strip() == "":
            rows.append({**row, "badge_no": "", "last_name": "", "first_name": ""})
            continue
        # each officer matches: badge [-] Last, First [Jr/Jr./Sr/etc] (SJPSO)
        matches = re.findall(
            r"(\d+)\s*-?\s*([A-Za-z'-]+),\s*([A-Za-z.\s]+?)\s*\(SJPSO\)",
            raw,
        )
        if not matches:
            rows.append({**row, "badge_no": "", "last_name": "", "first_name": ""})
            continue
        for badge, last, first in matches:
            rows.append({
                **row,
                "badge_no": badge.strip(),
                "last_name": last.strip().lower(),
                "first_name": first.strip().lower(),
            })
    result = pd.DataFrame(rows).drop(columns=["officers_involved"])
    return result.reset_index(drop=True)


def clean_datetime(df):
    df.loc[:, "occurred_date"] = df.incident_date
    df.loc[:, "occurred_time"] = (
        df.incident_time.fillna("")
        .astype(str)
        .str.strip()
        .str.zfill(5)
        .where(df.incident_time.notna(), "")
    )
    return df.drop(columns=["incident_date", "incident_time"])


def clean_subject_age(df):
    df.loc[:, "citizen_age"] = (
        df.subject_age
        .fillna("")
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )
    return df.drop(columns=["subject_age"])


def clean_subject_name(df):
    parts = df.subject_name.fillna("").str.extract(r"^([^,]+),\s*(.+)$")
    df.loc[:, "citizen_name"] = (
        parts[1].fillna("").str.strip() + " " + parts[0].fillna("").str.strip()
    ).str.strip().str.lower()
    return df.drop(columns=["subject_name"])


def clean_force_type(df):
    df.loc[:, "use_of_force_type"] = (
        df.force_type.fillna("").str.lower().str.strip()
    )
    return df.drop(columns=["force_type"])


def clean_injury(df):
    df.loc[:, "use_of_force_result"] = (
        df.injury.fillna("").str.lower().str.strip()
    )
    df.loc[df.use_of_force_result == "none", "use_of_force_result"] = ""
    return df.drop(columns=["injury"])


def clean_tracking_id(df):
    df.loc[:, "tracking_id_og"] = df.case_number.str.strip()
    return df


def clean():
    df = pd.read_csv(
        deba.data("raw/st_john_so/sjpso_uof_2023_2026.csv"), header=0
    )
    df.columns = [
        "incident_date", "incident_time", "address", "officers_involved",
        "case_number", "code", "subject_name", "force_type",
        "subject_age", "subject_sex", "subject_race", "injury",
    ]

    df = (
        df.pipe(split_officers)
        .pipe(clean_datetime)
        .pipe(clean_dates, ["occurred_date"])
        .pipe(clean_subject_age)
        .pipe(clean_subject_name)
        .pipe(clean_force_type)
        .pipe(clean_injury)
        .pipe(clean_tracking_id)
        .rename(columns={
            "subject_sex": "citizen_sex",
            "subject_race": "citizen_race",
            "address": "incident_location",
        })
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(
            standardize_desc_cols,
            ["incident_location", "code", "badge_no"],
        )
        .pipe(set_values, {"agency": "st-john-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id_og",
                "use_of_force_type",
                "occurred_year",
                "occurred_month",
                "occurred_day",
            ],
            "uof_uid",
        )
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
    )

    citizen_df = (
        df[["uof_uid", "citizen_age", "citizen_sex", "citizen_race",
            "citizen_name", "use_of_force_result", "agency"]]
        .copy()
        .pipe(
            gen_uid,
            ["citizen_age", "citizen_sex", "citizen_race", "citizen_name", "agency"],
            "citizen_uid",
        )
        .drop_duplicates(subset=["citizen_uid", "uof_uid"])
    )

    uof_df = df[
        [
            "tracking_id",
            "tracking_id_og",
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "occurred_time",
            "incident_location",
            "code",
            "use_of_force_type",
            "use_of_force_result",
            "badge_no",
            "first_name",
            "last_name",
            "agency",
            "uid",
            "uof_uid",
        ]
    ].drop_duplicates(subset=["uid", "uof_uid"])

    return uof_df, citizen_df


if __name__ == "__main__":
    uof, citizen_uof = clean()
    uof.to_csv(deba.data("clean/uof_st_john_so_2023_2026.csv"), index=False)
    citizen_uof.to_csv(
        deba.data("clean/uof_cit_st_john_so_2023_2026.csv"), index=False
    )
