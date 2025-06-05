
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

import pandas as pd
import re

def clean_department_desc(df):
    # === Replacement patterns for standardization ===
    replace_map = {
        r'tss': 'transportation safety services',
        r'weight enforcement': 'transportation safety services weight enforcement',
        r'cve[- ]?region 2': 'transportation safety services commercial vehicle enforcement region 2',
        r'commercial (motor )?vehicle enforcement': 'commercial vehicle enforcement',
        r'osfm': 'state fire marshal',
        r'special services section': 'special services section',
        r'cid.*bossier': 'criminal investigations division bossier field office',
        r'capitol police': 'capitol police detail',
        r'physical security': 'physical security',
        r'indian gaming': 'indian gaming enforcement',
        r'emergency services': 'emergency services unit',
        r'protective services': 'protective services unit',
        r'office of motor vehicles': 'office of motor vehicles',
    }

    # === Generic phrases to strip ===
    phrases_to_strip = [
        'louisiana state police',
        'louisiana',
        'department of public safety and corrections',
        'public safety services',
        'office of state police',
        'dps',
        'lsp',
    ]

    def strip_phrases(text):
        text = text.lower()
        for phrase in phrases_to_strip:
            text = text.replace(phrase, '')
        return text.strip(' -–—,').strip()

    def apply_replacements(text):
        for pattern, replacement in replace_map.items():
            if re.search(pattern, text):
                return replacement if replacement in text else f"{replacement} ({text})"
        return text

    def normalize_troops(text):
        match = re.match(r"troop\s+([a-i])\b", text.strip(), re.IGNORECASE)
        return f"troop {match.group(1).lower()}" if match else text

    def standardize(text):
        if pd.isna(text) or not isinstance(text, str):
            return ''
        text = strip_phrases(text)
        text = normalize_troops(text)
        text = apply_replacements(text)
        return text

    # Apply the cleaning logic
    df['department_desc'] = (
        df['department_desc']
        .fillna('')
        .astype(str)
        .apply(standardize)
    )

    return df

# def clean_department_desc(df):
#     # Replace abbreviation expansions
#     replace_map = {
#         'tss/weight enforcement': 'transportation safety services weight enforcement',
#         'tss/cve-region 2': 'transportation safety services commercial vehicle enforcement region 2',
#         'commercial motor vehicle enforcement': 'commercial vehicle enforcement',
#         'osfm — special services section': 'state fire marshal special services section',
#         'lsp — cid- bossier field office': 'criminal investigations division bossier field office',
#         'dps police/capitol detail': 'capitol police detail',
#         'dps - physical security': 'physical security',
#         'department of public safety/indian gaming': 'indian gaming enforcement',
#         'office of motor vehicles': 'office of motor vehicles',
#         'emergency services unit': 'emergency services unit',
#         'protective services unit': 'protective services unit',
#     }

#     # Generic phrases to strip out (not drop the row, just remove from string)
#     phrases_to_strip = [
#         'louisiana state police',
#         'louisiana',
#         # 'public safety services',
#         # 'office of state police',
#         'lsp',
#     ]

#     def strip_generic_phrases(text):
#         for phrase in phrases_to_strip:
#             text = text.replace(phrase, '')
#         return text.strip(' -–—').strip()

#     df['department_desc'] = (
#         df['department_desc']
#         .str.strip()
#         .str.lower()
#         .replace(replace_map)
#         .apply(strip_generic_phrases)
#     )
#     return df

import re

def split_and_clean_action(df):
    # === Map for word-to-digit conversion ===
    number_map = {
        r"\beighty\s*\(\s*80\s*\)|eighty": "80",
        r"\beight\s*\(\s*8\s*\)|eight": "8",
        r"\btwenty[-\s]*four\s*\(\s*24\s*\)|twenty[-\s]*four": "24",
        r"\bthirty[-\s]*two\s*\(\s*32\s*\)|thirty[-\s]*two": "32",
        r"\bone hundred\s*\(\s*100\s*\)|one hundred": "100",
        r"\bforty": "40",
        r"\bsixty": "60",
        r"\bfifty": "50",
        r"\bthirty": "30",
        r"\btwelve": "12",
        r"\bsixteen": "16",
        r"\bthirty[-\s]*six": "36"
    }

    # === Clean full action description while keeping details ===
    def clean_action_desc(val):
        if pd.isna(val):
            return ""
        val = val.lower().strip()
        for pattern, replacement in number_map.items():
            val = re.sub(pattern, replacement, val)
        val = re.sub(r"intended\s*", "", val)
        val = re.sub(r"(\d+)[-\s]*hour[s]*", r"\1 hour", val)
        val = re.sub(r"hour of suspension", "hour suspension", val)
        val = re.sub(r"\s{2,}", " ", val)
        return val.strip()

    # === Extract high-level action categories ===
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

    df["action_desc"] = df["action"].apply(clean_action_desc)
    df["action"] = df["action_desc"].apply(simplify_action)

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

import pandas as pd
import re

def clean_allegation_text(text):
    if pd.isna(text):
        return ""
    text = text.lower().strip()
    text = re.sub(r'\s*;\s*', '; ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.rstrip('.')
    return text

# not applying this function because I presume we want to keep the detail in the allegations, but here if needed. 
def classify_allegation(text):
    text = text.lower()
    categories = []

    if "use of force" in text or "excessive force" in text or "deadly force" in text:
        categories.append("Use of Force")
    if "false statements" in text or "misrepresentation" in text or "falsify" in text:
        categories.append("False Statements")
    if "bias" in text or "racial" in text or "profiling" in text:
        categories.append("Bias/Discrimination")
    if "body worn camera" in text or "firearm" in text or "intoxilyzer" in text or "vehicle" in text:
        categories.append("Misuse of Equipment")
    if any(term in text for term in ["dui", "drugs", "fraud", "stalking", "violation of law", "violence in the workplace"]):
        categories.append("Criminal Conduct")
    if "conduct unbecoming" in text or "neglect" in text or "unsatisfactory performance" in text:
        categories.append("Conduct Unbecoming")
    if "failure to report" in text or "failure to complete" in text or "failure to document" in text or "failure to investigate" in text:
        categories.append("Reporting Failure")
    if "threatened" in text or "inappropriate messages" in text or "harassment" in text:
        categories.append("Threats/Harassment")

    return "; ".join(sorted(set(categories))) if categories else "Other"

def clean_and_classify_allegations(df):
    df['allegation_clean'] = df['allegation'].apply(clean_allegation_text)
    df['allegation_category'] = df['allegation_clean'].apply(classify_allegation)
    return df

def clean_citizen_columns(df):
    for col in ['citizen_race', 'citizen_sex']:
        df[col] = df[col].fillna('').astype(str).str.lower().str.strip()

    # I am not sure what to do w 'juvenile' entry, GPT says 17 is apropriate data categoriation for underage. LMK 
    def clean_age(val):
        if pd.isna(val):
            return None
        val = str(val).strip().lower()
        if "brinkerhoff" in val or val.startswith("case_") or val == "":
            return None
        if val == "juvenile":
            return 17
        try:
            return int(val)
        except:
            return None

    df['citizen_age'] = df['citizen_age'].apply(clean_age)

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
                "civilian_race": "citizen_race",
                "civilian_sex": "citizen_sex",
                "civilian_age": "citizen_age",
                 }
                )
        .pipe(standardize_desc_cols, ["allegation", "action", "disposition", "department_desc", "rank_desc"])
        .pipe(clean_rank_desc)
        .pipe(clean_department_desc)
        .pipe(split_and_clean_action)
        .pipe(split_dates)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_citizen_columns)
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
