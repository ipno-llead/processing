import pandas as pd
import deba
import re
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values
from lib.clean import (
    strip_leading_comma,
    clean_dates,
    clean_races,
    clean_sexes,
    clean_names,
    clean_datetime,
    standardize_desc_cols,
)


def clean_title_of_report(df):
    def _clean(text):
        if pd.isna(text) or not isinstance(text, str):
            return None

        # Lowercase and strip
        text = text.lower().strip()

        # Remove "use of force" variations
        text = re.sub(r"\buse\s*of\s*force\b", "", text)

        # Remove date-like strings (e.g., "12/31/20", "03-15-2020", "may 2020", "march 29")
        text = re.sub(r"\b(?:\d{1,2}[-/ ]\d{1,2}[-/ ]\d{2,4}|\d{4}|january|february|march|april|may|june|july|august|september|october|november|december)\b", "", text)

        # Remove anything that looks like a badge number or ID
        text = re.sub(r"\b[s#]\d+\b", "", text)

        # Remove leftover numbers
        text = re.sub(r"\d+", "", text)

        # Remove all brackets and punctuation except spaces
        text = re.sub(r"[^\w\s]", "", text)

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()

        return text

    return df.assign(officer=df["title_of_report"].apply(_clean))

import re

import re

def clean_use_of_force_description(df):
    def _clean(text):
        if pd.isna(text) or not isinstance(text, str):
            return None

        text = text.lower().strip(" '\"")
        parts = re.split(r"[;#]+", text)

        cleaned_parts = []
        for part in parts:
            part = part.strip()

            # Normalize terms
            if "k9" in part or "k-9" in part:
                part = "k-9"
            elif "taser" in part:
                part = "taser"
            elif "spray" in part:
                part = "oc spray"
            elif "hands-on" in part:
                part = "hands-on controls"
            elif "firearm" in part or "handgun" in part or "duty weapon" in part or "glock" in part:
                part = "firearm"
            elif "impact weapon" in part:
                part = "impact weapon"
            elif "shield" in part:
                part = "capture shield"
            elif "verba" in part or "order" in part:
                part = "verbal orders"
            elif "gunpoint" in part:
                part = "held at gunpoint"
            elif "displayed" in part and "weapon" in part:
                part = "weapon displayed"
            elif "give up" in part or "surrender" in part:
                part = "k-9 surrender"
            elif "other" in part:
                part = "other"

            if part and part not in cleaned_parts:
                cleaned_parts.append(part)

        return "; ".join(cleaned_parts)

    df["use_of_force_description"] = df["use_of_force_description"].apply(_clean)
    return df

def clean_use_of_force_effective(df):
    def _clean(text):
        if pd.isna(text) or not isinstance(text, str):
            return None

        # Lowercase and strip quotes
        text = text.lower().strip(" '\"")

        # Split on common delimiters
        parts = re.split(r"[;#]+", text)

        cleaned_parts = []
        for part in parts:
            part = part.strip()

            # Fix common typos and standardize
            if "immobilized" in part and "semi" in part:
                part = "semi-immobilized"
            elif "immobilized" in part:
                part = "immobilized"
            elif "no apparent" in part:
                part = "no apparent effect"
            elif "surrender" in part:
                part = "subject surrendered"
            elif "escalated" in part:
                part = "subject escalated resistance"
            elif "subjec" in part:
                part = "subject surrendered"

            if part and part not in cleaned_parts:
                cleaned_parts.append(part)

        return "; ".join(cleaned_parts)

    df["use_of_force_effective"] = df["use_of_force_effective"].apply(_clean)
    return df

def extract_incident_date_parts(df):
    def extract_mmddyyyy(text):
        if pd.isna(text) or not isinstance(text, str):
            return None
        # Extract first MM/DD/YYYY or M/D/YYYY pattern
        match = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", text)
        if match:
            return f"{int(match.group(1)):02d}/{int(match.group(2)):02d}/{match.group(3)}"
        return None

    # Extract cleaned date string
    df["incident_date"] = df["date_and_time"].apply(extract_mmddyyyy)

    # Split into components
    df["incident_month"] = pd.to_datetime(df["incident_date"], errors="coerce").dt.month
    df["incident_day"] = pd.to_datetime(df["incident_date"], errors="coerce").dt.day
    df["incident_year"] = pd.to_datetime(df["incident_date"], errors="coerce").dt.year

    # Drop original columns
    df = df.drop(columns=["date_and_time", "incident_date"])
    return df

def extract_officer_name_and_rank(df):
    # Define mapping of rank abbreviations to full rank descriptions
    rank_map = {
        "sgt": "sergeant",
        "cpl": "corporal",
        "cp": "corporal",
        "lt": "lieutenant",
        "dy": "deputy",
        "pl": "patrolman",
        "det": "detective",
        "dputy": "deputy"
    }

    def _parse(text):
        if pd.isna(text) or not isinstance(text, str) or not text.strip():
            return pd.Series({"rank_desc": '', "first_name": '', "last_name": ''})

        text = text.lower().strip()

        # Separate rank if present
        parts = text.split()
        rank_abbr = ''
        possible_rank = parts[0]

        if possible_rank in rank_map:
            rank_abbr = possible_rank
            parts = parts[1:]
        elif possible_rank[:3] in rank_map:
            rank_abbr = possible_rank[:3]
            parts[0] = possible_rank[3:]

        # Fix stuck names like "josephsmith" → "joseph smith"
        if len(parts) == 1 and len(parts[0]) > 8:
            parts = re.findall(r"[a-zA-Z]{3,}", parts[0])

        # Default fallback
        first = parts[0] if len(parts) > 0 else ''
        last = parts[1] if len(parts) > 1 else ''

        return pd.Series({
            "rank_desc": rank_map.get(rank_abbr) if rank_abbr else '',
            "first_name": first,
            "last_name": last
        })

    # Apply parser
    parsed = df["officer"].apply(_parse)
    return pd.concat([df.drop(columns=["officer"]), parsed], axis=1)

def clean():
    df = (pd.read_csv(deba.data("raw/baton_rouge_so/baton_rouge_so_uof_2020.csv"))
        .pipe(clean_column_names)
        .pipe(clean_title_of_report)
        .pipe(lambda df: df.drop(columns=["title_of_report"]))
        .drop(columns=["title_2"])
        .rename(
            columns={
                "force_used": "use_of_force_description",
                "effectiveness_of_force_used": "use_of_force_effective",
                })
        .pipe(standardize_desc_cols, ["use_of_force_description", "use_of_force_effective"])
        .pipe(clean_use_of_force_description)
        .pipe(clean_use_of_force_effective)
        .pipe(extract_incident_date_parts)
        .pipe(extract_officer_name_and_rank)
        .pipe(set_values, {"agency": "east-baton-rouge-so"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["incident_month", "incident_day", "incident_year", "agency"], "tracking_id")
        .pipe(gen_uid, ["tracking_id", "first_name", "last_name", "agency"], "uof_uid")
        .drop_duplicates(subset=["uof_uid"])
    )
    return df 


if __name__ == "__main__":
    uof = clean()
    uof.to_csv(deba.data("clean/uof_baton_rouge_so_2020.csv"), index=False)
