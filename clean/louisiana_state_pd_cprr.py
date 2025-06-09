
import deba
import pandas as pd
import re 
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

    df['department_desc'] = (
        df['department_desc']
        .fillna('')
        .astype(str)
        .apply(standardize)
    )

    return df



def split_and_clean_action(df):
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

    def extract_actions(val):
        val = val.lower()
        actions = []
        if "termination" in val:
            actions.append("termination")
        if "demotion" in val or "reduction in pay" in val:
            actions.append("demotion")
        if "reprimand" in val:
            actions.append("letter of reprimand")
        if "suspension" in val:
            actions.append("suspension")
        return "; ".join(actions) if actions else "other"

    df["action_desc"] = df["action"].apply(clean_action_desc)
    df["action"] = df["action_desc"].apply(extract_actions)

    for action_type in ["termination", "suspension", "demotion", "letter of reprimand"]:
        key = action_type.replace(" ", "_")
        df[f"{key}_year"] = ""
        df[f"{key}_month"] = ""
        df[f"{key}_day"] = ""

    for idx, row in df.iterrows():
        actions = row["action"].split("; ")
        for action_type in actions:
            key = action_type.replace(" ", "_")
            df.at[idx, f"{key}_year"] = row.get("action_year", "")
            df.at[idx, f"{key}_month"] = row.get("action_month", "")
            df.at[idx, f"{key}_day"] = row.get("action_day", "")

    return df

def split_incident_date(df):
    if "incident_date" in df.columns:
        df["incident_date"] = pd.to_datetime(df["incident_date"], errors="coerce")
        df["incident_year"] = df["incident_date"].dt.year
        df["incident_month"] = df["incident_date"].dt.month
        df["incident_day"] = df["incident_date"].dt.day
        df = df.drop(columns=["incident_date"])
    return df


def clean_allegation_text(text):
    if pd.isna(text):
        return ""
    text = text.lower().strip()
    text = re.sub(r'\s*;\s*', '; ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.rstrip('.')
    return text

def clean_and_classify_allegations(df):
    df['allegation_clean'] = df['allegation'].apply(clean_allegation_text)
    df['allegation_category'] = df['allegation_clean'].apply(classify_allegation)
    return df

def clean_citizen_columns(df):
    for col in ['citizen_race', 'citizen_sex']:
        df[col] = df[col].fillna('').astype(str).str.lower().str.strip()

    def clean_age(val):
        if pd.isna(val):
            return None
        val = str(val).strip().lower()
        if "brinkerhoff" in val or val.startswith("case_") or val == "":
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
        .pipe(split_incident_date)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_citizen_columns)
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "action", "uid"], "allegation_uid")
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .drop_duplicates(subset=["allegation_uid"])
        )
    return df


if __name__ == "__main__":
    df = clean()
    df25 = clean_25()
    df.to_csv(deba.data("clean/cprr_louisiana_state_pd_2019_2020.csv"), index=False)
    df25.to_csv(deba.data("clean/cprr_louisiana_state_pd_2021_2022.csv"), index=False)
