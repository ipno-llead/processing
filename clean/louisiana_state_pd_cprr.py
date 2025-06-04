import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_names,
    clean_ranks,
    clean_dates,
    names_to_title_case,
    standardize_desc_cols,
)
from lib.uid import gen_uid
from functools import reduce


def split_rows_with_multiple_allegations(df):
    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split(";", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = df.action.str.replace(
        r"suspensino", "suspension", regex=False
    )
    return df


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/louisiana_state_pd/cprr_louisiana_state_pd_2019_2020.csv")
        )
        .pipe(clean_column_names)
        .rename(columns={"tracking_id": "tracking_id_og"})
        .pipe(split_rows_with_multiple_allegations)
        .pipe(clean_action)
        .pipe(standardize_desc_cols, ["allegation", "action", "disposition"])
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "action", "uid"], "allegation_uid")
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .drop_duplicates(subset=["allegation_uid"])
    )
    return df

def clean_rank_desc(df):
    rank_map = {
        'lt.': 'lieutenant',
        'sgt.': 'sergeant',
        'tfc': 'trooper first class',
        'dps officer': 'officer',
    }
    df['rank_desc'] = df['rank_desc'].replace(rank_map)
    return df

def clean_department_desc(df):
    # Replace abbreviation expansions
    replace_map = {
        'tss/weight enforcement': 'transportation safety services weight enforcement',
        'tss/cve-region 2': 'transportation safety services commercial vehicle enforcement region 2',
        'commercial motor vehicle enforcement': 'commercial vehicle enforcement',
        'osfm — special services section': 'state fire marshal special services section',
        'lsp — cid- bossier field office': 'criminal investigations division bossier field office',
        'dps police/capitol detail': 'capitol police detail',
        'dps - physical security': 'physical security',
        'department of public safety/indian gaming': 'indian gaming enforcement',
        'office of motor vehicles': 'office of motor vehicles',
        'emergency services unit': 'emergency services unit',
        'protective services unit': 'protective services unit',
    }

    # Generic phrases to strip out (not drop the row, just remove from string)
    phrases_to_strip = [
        'louisiana state police',
        # 'department of public safety and corrections',
        # 'public safety services',
        # 'office of state police',
        'lsp',
    ]

    def strip_generic_phrases(text):
        for phrase in phrases_to_strip:
            text = text.replace(phrase, '')
        return text.strip(' -–—').strip()

    df['department_desc'] = (
        df['department_desc']
        .str.strip()
        .str.lower()
        .replace(replace_map)
        .apply(strip_generic_phrases)
    )
    return df

import re

def split_and_clean_action(df):
    # Function to extract simplified action category
    def simplify_action(val):
        val = val.lower()
        if "termination" in val:
            return "termination"
        elif "demotion" in val or "reduction in pay" in val:
            return "demotion"
        elif "reprimand" in val:
            return "letter of reprimand"
        elif "suspension" in val:
            return "suspension"
        return "other"

    # Function to normalize action description
    def clean_action_desc(val):
        val = val.lower().strip()

        # Convert common written numbers to digits
        val = re.sub(r"eighty\s*\(\s*80\s*\)", "80", val)
        val = re.sub(r"eight\s*\(\s*8\s*\)", "8", val)
        val = re.sub(r"twenty[-\s]*four\s*\(\s*24\s*\)", "24", val)
        val = re.sub(r"thirty[-\s]*two\s*\(\s*32\s*\)", "32", val)
        val = re.sub(r"one hundred\s*\(\s*100\s*\)", "100", val)

        # Remove "intended", fix hour format
        val = re.sub(r"intended\s*", "", val)
        val = re.sub(r"(\d+)[-\s]*hour[s]*", r"\1 hour", val)
        val = re.sub(r"\s{2,}", " ", val)

        # Capitalize for readability
        return val.strip().capitalize()

    # Use the original column to build action_desc before overwriting it
    df["action_desc"] = df["action"].apply(clean_action_desc)
    df["action"] = df["action"].apply(simplify_action)

    return df

import pandas as pd

def split_dates(df):
    # Ensure date columns are properly parsed
    df["incident_date"] = pd.to_datetime(df["incident_date"], errors="coerce", format="%m/%d/%Y")
    df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce", format="%m/%d/%Y")

    # Split incident_date
    df["incident_month"] = df["incident_date"].dt.month
    df["incident_day"] = df["incident_date"].dt.day
    df["incident_year"] = df["incident_date"].dt.year

    # Split action_date
    df["action_month"] = df["action_date"].dt.month
    df["action_day"] = df["action_date"].dt.day
    df["action_year"] = df["action_date"].dt.year

    # Drop original date columns
    df = df.drop(columns=["incident_date", "action_date"])

    return df


def clean_25():
    df = (
        pd.read_csv(
            deba.data("raw/louisiana_state_pd/cprr_louisiana_state_pd_2021_2022.csv")
        )
        .drop(columns=["filename", "source_file", "officer_middle_name"])
        .pipe(clean_column_names)
        .rename(columns={
                "officer_first_name": "first_name",
                "officer_last_name": "last_name",
                "rank": "rank_desc",
                "ia_number": "case_number",
                "ola_number": "tracking_id_og",
                 }
                )
        .pipe(standardize_desc_cols, ["allegation", "action", "disposition", "department_desc", "rank_desc"])
        .pipe(clean_rank_desc)
        .pipe(clean_department_desc)
        .pipe(split_and_clean_action)
        .pipe(split_dates)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "action", "uid"], "allegation_uid")
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        # .drop_duplicates(subset=["allegation_uid"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df25 = clean_25()
    df.to_csv(deba.data("clean/cprr_louisiana_state_pd_2019_2020.csv"), index=False)
    df25.to_csv(deba.data("clean/cprr_louisiana_state_pd_2021_2022.csv"), index=False)
