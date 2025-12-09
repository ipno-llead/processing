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

def split_date_and_time(df):
    def _split(text):
        if pd.isna(text) or not isinstance(text, str):
            return pd.Series({"occur_date": None, "occur_time": None})

        parts = text.strip().split(" ", 1)
        date_part = parts[0] if len(parts) > 0 else None
        time_part = parts[1] if len(parts) > 1 else None

        return pd.Series({"occur_date": date_part, "occur_time": time_part})

    parsed = df["date_and_time"].apply(_split)
    return pd.concat([df.drop(columns=["date_and_time"]), parsed], axis=1)


def clean_force_and_effectiveness(df):
    # Known effectiveness values
    effectiveness_values = {
        'immobilized', 'semi-immobilized', 'no apparent effect',
        'subject surrendered', 'subject escalated resistance',
        'immobitized'  # typo in data
    }

    # Known force types for identification
    force_keywords = [
        'taser', 'k-9', 'k9', 'spray', 'hands-on', 'hands on', 'impact weapon',
        'bean bag', 'firearm', 'handgun', 'hand gun', 'glock', 'duty weapon',
        'pistol', 'gun point', 'gunpoint', 'rifle', 'shotgun', 'pointed',
        'brandished', 'display', 'fire arm', 'shield', 'verbal', 'tear gas',
        'glove', 'g.l.o.v.e', 'pepperball', 'other', 'pressure point', 'lethal'
    ]

    def _parse_items(text):
        if pd.isna(text) or not isinstance(text, str):
            return []
        # Remove JSON artifacts
        text = re.sub(r'[\[\]"\'\{\}\\]', '', text)
        # Split on commas and clean
        items = [p.strip() for p in text.split(',') if p.strip()]
        return items

    def _is_effectiveness(item):
        item_lower = item.lower().strip()
        return item_lower in effectiveness_values

    def _is_force(item):
        item_lower = item.lower().strip()
        return any(kw in item_lower for kw in force_keywords)

    def _normalize_force(item):
        item = item.lower().strip()
        if 'taser' in item:
            return 'taser'
        elif 'k-9' in item or 'k9' in item:
            if 'warning' in item or 'surrender' in item:
                return 'k-9 warning'
            elif 'apprehension' in item or 'deployment' in item or 'deployed' in item:
                return 'k-9 apprehension'
            else:
                return 'k-9'
        elif 'spray' in item or 'pepperball' in item:
            if 'no control' in item or 'not deploy' in item:
                return None
            return 'oc spray'
        elif 'hands-on' in item or 'hands on' in item or item == 'controls':
            return 'hands-on controls'
        elif 'impact weapon' in item or 'bean bag' in item:
            return 'impact weapon'
        elif any(x in item for x in ('firearm', 'handgun', 'hand gun', 'glock', 'duty weapon',
                                      'pistol', 'gun point', 'gunpoint', 'rifle', 'shotgun',
                                      'pointed', 'brandished', 'display', 'withdrew weapon',
                                      'fire arm', 'service pistol', 'lethal')):
            return 'firearm'
        elif 'shield' in item:
            return 'capture shield'
        elif 'verbal' in item or 'order' in item:
            return 'verbal orders'
        elif 'tear gas' in item:
            return 'tear gas'
        elif 'glove' in item or 'g.l.o.v.e' in item:
            return 'glove'
        elif 'pressure point' in item:
            return 'pressure point control'
        elif 'other' in item:
            return 'other'
        return None

    def _normalize_effectiveness(item):
        item = item.lower().strip()
        if 'immob' in item and 'semi' not in item:
            return 'immobilized'
        elif 'semi' in item:
            return 'semi-immobilized'
        elif 'no apparent' in item:
            return 'no apparent effect'
        elif 'surrendered' in item:
            return 'subject surrendered'
        elif 'escalated' in item:
            return 'subject escalated resistance'
        return None

    def _process_row(row):
        force_items = _parse_items(row['force_used'])
        eff_items = _parse_items(row['effectiveness_of_force_used'])

        force_cleaned = []
        eff_cleaned = []

        # Process force_used - separate mixed-in effectiveness values
        for item in force_items:
            if _is_effectiveness(item):
                norm = _normalize_effectiveness(item)
                if norm and norm not in eff_cleaned:
                    eff_cleaned.append(norm)
            elif _is_force(item):
                norm = _normalize_force(item)
                if norm and norm not in force_cleaned:
                    force_cleaned.append(norm)

        # Process effectiveness column
        for item in eff_items:
            norm = _normalize_effectiveness(item)
            if norm and norm not in eff_cleaned:
                eff_cleaned.append(norm)

        return pd.Series({
            'use_of_force_description': '; '.join(force_cleaned) if force_cleaned else None,
            'use_of_force_effective': '; '.join(eff_cleaned) if eff_cleaned else None
        })

    result = df.apply(_process_row, axis=1)
    df = df.drop(columns=['force_used', 'effectiveness_of_force_used'])
    return pd.concat([df, result], axis=1)


def split_deputy_name(df):
    def _parse(text):
        if pd.isna(text) or not isinstance(text, str):
            return pd.Series({"first_name": None, "middle_name": None, "last_name": None})

        text = text.strip()
        parts = text.split()

        if len(parts) == 1:
            return pd.Series({"first_name": parts[0], "middle_name": None, "last_name": None})

        if len(parts) == 2:
            return pd.Series({"first_name": parts[0], "middle_name": None, "last_name": parts[1]})

        # Check if last part is a suffix (Sr, Jr, etc.)
        suffix_pattern = re.match(r"^(sr\.?|jr\.?|ii|iii|iv)$", parts[-1], re.IGNORECASE)
        if suffix_pattern and len(parts) >= 3:
            # Has suffix - check if middle name exists
            if len(parts) == 3:
                # First Last Suffix
                return pd.Series({"first_name": parts[0], "middle_name": None, "last_name": f"{parts[1]} {parts[2]}"})
            else:
                # First Middle Last Suffix
                return pd.Series({"first_name": parts[0], "middle_name": parts[1].rstrip("."), "last_name": f"{parts[2]} {parts[3]}"})

        # No suffix - First Middle Last
        if len(parts) == 3:
            return pd.Series({"first_name": parts[0], "middle_name": parts[1].rstrip("."), "last_name": parts[2]})

        # More than 3 parts without suffix - take first as first, second as middle, rest as last
        return pd.Series({"first_name": parts[0], "middle_name": parts[1].rstrip("."), "last_name": " ".join(parts[2:])})

    parsed = df["deputy_using_force"].apply(_parse)
    return pd.concat([df.drop(columns=["deputy_using_force"]), parsed], axis=1)


def extract_occur_date_parts(df):
    dt = pd.to_datetime(df["occur_date"], errors="coerce")
    df = df.assign(
        incident_month=dt.dt.month,
        incident_day=dt.dt.day,
        incident_year=dt.dt.year
    )
    return df.drop(columns=["occur_date"])


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



def clean25():
    df = (pd.read_csv(deba.data("raw/baton_rouge_so/ebrso_uof_2020_2025.csv"))
        .pipe(clean_column_names)
        .pipe(split_date_and_time)
        .pipe(extract_occur_date_parts)
        .pipe(split_deputy_name)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_force_and_effectiveness)
        .pipe(set_values, {"agency": "east-baton-rouge-so"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["incident_month", "incident_day", "incident_year", "agency"], "tracking_id")
        .pipe(gen_uid, ["tracking_id", "first_name", "last_name", "agency"], "uof_uid")
        .drop_duplicates(subset=["uof_uid"])
    )
    return df 



def remove_duplicates_from_new(df_old, df_new):
    original_count = len(df_new)
    df_new = df_new[~df_new['uof_uid'].isin(df_old['uof_uid'])]
    removed_count = original_count - len(df_new)
    if removed_count > 0:
        print(f"Removed {removed_count} duplicate uof_uid entries from new dataset")
    return df_new


if __name__ == "__main__":
    uof = clean()
    uof25 = clean25()
    uof25 = remove_duplicates_from_new(uof, uof25)
    uof.to_csv(deba.data("clean/uof_baton_rouge_so_2020.csv"), index=False)
    uof25.to_csv(deba.data("clean/uof_baton_rouge_so_2021_2025.csv"), index=False)
