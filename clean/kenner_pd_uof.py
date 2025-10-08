import pandas as pd
import deba
import ast 
import re 
from lib.columns import clean_column_names
from lib.clean import clean_dates, standardize_desc_cols, clean_names
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
    df.loc[:, "allegation"] = (
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
    df.loc[:, "agency"] = "kenner-pd"
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
        .pipe(split_name)
        .pipe(standardize_desc_cols, ["location"])
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "disposition",
                "action",
                "allegation",
                "location",
                "incident_year",
                "incident_month",
                "incident_day",
            ],
            "uof_uid",
        )
        .drop_duplicates(subset=["uid", "uof_uid"])
    )
    return df

import pandas as pd

def clean_rank(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "officer": "officer",
        "off.": "officer",
        "off": "officer",
        "offr.": "officer",
        "ofc": "officer",
        "ofc.": "officer",
        "p/o": "officer",
        "po": "officer",
        "police officer": "officer",
        "o.f.c": "officer",
        
        "liuetenant": "lieutenant",  # misspelling
        "lt": "lieutenant",
        
        "sgt": "sergeant",
        "sergeant": "sergeant",
        
        "patrol": "patrol officer",
        "patrol officer": "patrol officer",
        "cpo": "patrol officer",  # assume same
        
        # everything else just lowercase
        "det": "detective",
        "k.": "",
        "ball": "",
        "not_visible": ""
    }

    def normalize(val):
        if pd.isna(val):
            return val
        val = str(val).strip().lower()
        return mapping.get(val, val)

    df["rank"] = df["rank"].map(normalize)
    return df

def split_race_sex(df: pd.DataFrame) -> pd.DataFrame:
    # Define maps
    race_map = {
        "b": "black",
        "w": "white",
        "h": "hispanic",
        "black": "black",
        "white": "white",
        "hispanic": "hispanic"
    }
    sex_map = {
        "m": "male",
        "f": "female",
        "male": "male",
        "female": "female"
    }

    def normalize(val):
        if pd.isna(val):
            return ("", "")
        val = str(val).lower().strip()
        if "/" in val:
            r, s = val.split("/", 1)
            return (race_map.get(r, r), sex_map.get(s, s))
        return (race_map.get(val, val), "")

    df[["race", "sex"]] = df["race_sex"].apply(lambda x: pd.Series(normalize(x)))
    return df

import pandas as pd
import numpy as np

def clean_age(df: pd.DataFrame) -> pd.DataFrame:
    def normalize(val):
        # Treat NaN or missing as empty string
        if pd.isna(val):
            return ""
        try:
            # If it's numeric, cast to int
            return str(int(float(val)))
        except (ValueError, TypeError):
            return ""  # anything invalid becomes blank

    df["age"] = df["age"].map(normalize)
    return df

def split_and_extract_kenner_officers(df: pd.DataFrame, src_col: str = "officers_involved") -> pd.DataFrame:
    import ast, re
    if src_col not in df.columns:
        return df

    # --- keep the row's "owner" officer BEFORE we overwrite columns ---
    owner = (
        df.assign(
            _owner_first=df["first_name"].fillna("").astype(str) if "first_name" in df.columns else pd.Series([""]*len(df), dtype=str),
            _owner_last=df["last_name"].fillna("").astype(str) if "last_name" in df.columns else pd.Series([""]*len(df), dtype=str),
        )
        .loc[:, ["_owner_first", "_owner_last"]]
    )

    _RANK_MAP = {
        "officer":"officer","off.":"officer","off":"officer","ofc.":"officer","ofc":"officer",
        "sgt.":"sergeant","sgt":"sergeant","sergeant":"sergeant",
        "det.":"detective","det":"detective","detective":"detective",
        "lt.":"lieutenant","lt":"lieutenant",
        "cpl.":"corporal","cpl":"corporal","capt.":"captain","capt":"captain",
        "chief":"chief","sa.":"special agent","sa":"special agent","cpo":"cpo",
    }
    _NA_TOKENS = {"", "n/a", "na", "not visible"}
    _BADGE_PAT = re.compile(r"\b\d{3,6}\b")
    _PAREN_PAT = re.compile(r"\([^)]*\)")
    _MULTI_SPACE = re.compile(r"\s{2,}")

    def _safe_parse_list_literal(x):
        if not isinstance(x, str):
            return []
        s = x.strip()
        if s.lower() in _NA_TOKENS or s.lower() == "['n/a', 'n/a']":
            return []
        try:
            v = ast.literal_eval(s)
            if isinstance(v, (list, tuple)):
                return [str(t) for t in v]
        except Exception:
            pass
        s = s.strip("[] ")
        return [p.strip().strip("'\"") for p in s.split(",") if p.strip()]

    def _normalize_officer_token(tok: str) -> str:
        tok = tok or ""
        tok = _PAREN_PAT.sub("", tok)       # remove (R/O) etc.
        tok = _BADGE_PAT.sub("", tok)       # remove badge numbers like 6556
        tok = tok.replace("/", " ").replace("|", " ")
        tok = _MULTI_SPACE.sub(" ", tok)
        return tok.strip(" .,\t\r\n")

    def _pretty(s: str) -> str:
        return s.title()

    def _extract_rank_and_name(off_str: str):
        raw = _normalize_officer_token(off_str)
        if not raw:
            return "", "", ""
        low = raw.lower()
        m = re.match(r"^(chief|capt\.?|cpl\.?|lt\.?|sgt\.?|sergeant|det\.?|ofc\.?|off\.?|officer|sa\.?|cpo)\b[ .]*", low)
        rank = ""
        if m:
            rank_key = m.group(1).rstrip(".")
            rank = _RANK_MAP.get(rank_key, rank_key)
            raw = raw[m.end():].lstrip()
        if "," in raw:  # "Last, First"
            last, first = [p.strip() for p in raw.split(",", 1)]
            first_tok = first.split()[0] if first else ""
            return rank, _pretty(first_tok.rstrip(".")), _pretty(last)
        parts = raw.split()
        if not parts:
            return rank, "", ""
        if len(parts) == 1:
            return rank, "", _pretty(parts[0])
        first = parts[0].rstrip(".")
        last = " ".join(parts[1:])
        return rank, _pretty(first), _pretty(last)

    def _same_person(f1, l1, f2, l2):
        """Match by last name exact (case-insensitive) and first-name initial or exact."""
        f1 = str(f1 or "").strip().lower()
        f2 = str(f2 or "").strip().lower()
        l1 = str(l1 or "").strip().lower()
        l2 = str(l2 or "").strip().lower()
        if not l1 or not l2:
            return False
        if l1 != l2:
            return False
        if not f1 or not f2:
            return False
        return f1 == f2 or f1[0] == f2[0]  # "v" == "victoria"

    out = df.copy().reset_index(drop=True)
    parsed = out[src_col].apply(_safe_parse_list_literal)
    parsed = parsed.apply(lambda L: [t for t in (_normalize_officer_token(x) for x in L) if t and t.lower() not in _NA_TOKENS])

    # explode
    out = out.assign(_officer_item=parsed).explode("_officer_item", ignore_index=True)

    # extract name for exploded token
    extracted = out["_officer_item"].fillna("").apply(_extract_rank_and_name)
    out[["rank", "first_name", "last_name"]] = pd.DataFrame(extracted.tolist(), index=out.index)

    # bring back owner columns aligned with out's index
    out[["_owner_first", "_owner_last"]] = owner.reindex(out.index)

    # DROP self rows: when exploded name == row owner (supports initial vs full)
    mask_self = out.apply(lambda r: _same_person(r.get("first_name",""), r.get("last_name",""),
                                                 r.get("_owner_first",""), r.get("_owner_last","")), axis=1)
    out = out.loc[~mask_self].copy()

    # keep only rows that actually have an extracted name
    out = out[~((out["first_name"] == "") & (out["last_name"] == ""))].copy()

    return out.drop(columns=["_officer_item", "_owner_first", "_owner_last"])


def clean_25(): 
    df = (
    pd.read_csv(deba.data("raw/kenner_pd/kenner_pd_uof_2021_2025.csv"))
    .pipe(clean_column_names)
    .drop(columns=["incident_number", "incident_type", "subject_name", "source_year", 'type_of_incident', 'discharge_type', 'discharge_intent', 'arrested'])
    .rename(columns={"officer_rank": "rank", "officer_first_name": "first_name", "officer_last_name": "last_name"})
    .pipe(clean_rank)
    .pipe(clean_dates, ["incident_date"])
    .pipe(standardize_desc_cols, ["location"])
    .pipe(split_race_sex)
    .pipe(clean_age)
    .pipe(split_and_extract_kenner_officers, src_col="officers_involved")
    .pipe(clean_names, ["first_name", "last_name"])
    .drop(columns=["race_sex", "officers_involved"])
    .pipe(standardize_desc_cols, ['officers_notes'])
    .pipe(assign_agency)
    .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "location",
                "incident_year",
                "incident_month",
                "incident_day",
            ],
            "uof_uid",
        )
    .rename(columns={"race": "citizen_race", "sex": "citizen_sex", "age": "citizen_age"})
    .pipe(
            gen_uid,
            ["citizen_race", "citizen_sex", "citizen_age"],
            "uof_citizen_uid",
        )
    .drop_duplicates(subset=["uof_citizen_uid"])
)
    return df 

def split_citizen_and_uof(
    df,
    uid_col="uof_uid",
    citizen_uid_col="uof_citizen_uid",
    citizen_cols=("citizen_race", "citizen_sex", "citizen_age")
):
    """
    From a cleaned UOF dataframe, produce:
      - uof_df: original df unchanged
      - citizen_df: [uid_col, citizen_uid_col] + citizen_cols, deduped

    Notes
    -----
    - Drops rows from citizen_df where ALL citizen columns are null/blank.
    - Dedupes on [uid_col, citizen_uid_col] if present, else on [uid_col] + citizen_cols.
    """
    # keep original unchanged
    uof_df = df.copy()

    existing_citizen_cols = [c for c in citizen_cols if c in df.columns]
    cols_for_citizen = [uid_col] + existing_citizen_cols
    if citizen_uid_col in df.columns:
        cols_for_citizen.insert(1, citizen_uid_col)

    # build citizen df
    citizen_df = (
    df.loc[:, [c for c in cols_for_citizen if c in df.columns]]
    .assign(
        # normalize blanks to NaN for easy filtering
        **{
            c: df[c].replace(["", " ", "na", "n/a", "NA", "N/A"], pd.NA)
            for c in existing_citizen_cols
        },
        agency="kenner-pd"  # Add agency here in the initial assign
    )
)

    if existing_citizen_cols:
        mask_all_missing = citizen_df[existing_citizen_cols].isna().all(axis=1)
        citizen_df = citizen_df.loc[~mask_all_missing].copy()

    citizen_df = citizen_df.drop_duplicates(subset=[uid_col])

    return uof_df, citizen_df



if __name__ == "__main__":
    uof = clean_uof()
    uof25 = clean_25() 
    uof25, citizen_df = split_citizen_and_uof(uof25)
    uof.to_csv(deba.data("clean/uof_kenner_pd_2005_2021.csv"), index=False)
    uof25.to_csv(deba.data("clean/uof_kenner_pd_2021_2025.csv"), index=False)
    citizen_df.to_csv(deba.data("clean/uof_cit_kenner_pd_2021_2025.csv"), index=False)
