import pandas as pd
from lib.clean import (
    clean_datetimes,
    float_to_int_str,
    standardize_desc_cols,
    clean_races,
    clean_sexes,
    clean_names,
)
import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid

def clean_disposition(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize values in the disposition column by replacing '||' with ';'."""
    if "disposition" in df.columns:
        df = df.copy()
        df["disposition"] = (
            df["disposition"]
            .astype(str)                # ensure string type
            .str.replace("||", ";")     # replace double pipes
            .str.replace(";", ";")      # normalize spacing if needed
            .str.strip()                # remove leading/trailing spaces
        )
    return df

DEFAULT_STOP_DATETIME = pd.Timestamp("2025-07-28 13:00")

def parse_stop_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse 'stop_date' like '1/1/23 1:50' into:
      - stop_and_search_year
      - stop_and_search_month
      - stop_and_search_day
      - stop_and_search_time (HH:MM)
    Fill any missing/unparseable stop_date with 2025-07-28 13:00.
    """
    if "stop_date" not in df.columns:
        return df

    df = df.copy()

    # Normalize text, parse to datetime; coerce bad rows to NaT
    dt = pd.to_datetime(
        df["stop_date"].astype(str).str.strip(),
        format="%m/%d/%y %H:%M",
        errors="coerce",
    ).fillna(DEFAULT_STOP_DATETIME)

    # Create schema columns
    df["stop_and_search_year"]  = dt.dt.year
    df["stop_and_search_month"] = dt.dt.month
    df["stop_and_search_day"]   = dt.dt.day
    df["stop_and_search_time"]  = dt.dt.strftime("%H:%M")

    return df
import re 
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

def _strip_trailing_period(s: str) -> str:
    return s[:-1] if s and s.endswith(".") else s

def _parse_name(raw: str) -> dict:
    """
    Expected format mostly like: 'Last, First Middle...' or 'Last Suffix, First Middle...'
    Returns dict with first_name, middle_name, last_name.
    """
    if not isinstance(raw, str):
        return {"first_name": None, "middle_name": None, "last_name": None}

    name = raw.strip()
    if not name:
        return {"first_name": None, "middle_name": None, "last_name": None}

    # Default outputs
    first_name = middle_name = last_name = None

    if "," in name:
        # 'Last (maybe suffix), First Middle'
        last_part, first_part = [p.strip() for p in name.split(",", 1)]

        # Handle suffix appended to last name: "Douglas Jr" / "Johansen III"
        last_tokens = last_part.split()
        if last_tokens and last_tokens[-1].lower().rstrip(".") in _SUFFIXES:
            # drop suffix from last name for this task
            last_tokens = last_tokens[:-1]
        last_name = " ".join(last_tokens) if last_tokens else None

        # First + middle(s)
        first_tokens = [t for t in first_part.split() if t]
        if first_tokens:
            first_name = first_tokens[0]
            mids = first_tokens[1:]
            # Strip trailing periods in middle initials like "S." -> "S"
            mids = [_strip_trailing_period(t) for t in mids]
            middle_name = " ".join(mids) if mids else None
    else:
        # Fallback: try "First Middle Last"
        tokens = [t for t in name.split() if t]
        if len(tokens) >= 2:
            first_name = tokens[0]
            last_name = tokens[-1]
            mids = tokens[1:-1]
            mids = [_strip_trailing_period(t) for t in mids]
            middle_name = " ".join(mids) if mids else None
        elif len(tokens) == 1:
            # Single token; assume last name
            last_name = tokens[0]

    return {"first_name": first_name, "middle_name": middle_name, "last_name": last_name}

def explode_and_parse_primary_officer(df: pd.DataFrame, col: str = "primary_officers") -> pd.DataFrame:
    """
    - Splits `col` on '||'
    - Duplicates rows (explode)
    - Adds first_name, middle_name, last_name columns
    """
    if col not in df.columns:
        return df

    df = df.copy()

    # 1) split on '||' with optional surrounding spaces
    split_series = (
        df[col]
        .astype(str)
        .str.split(r"\s*\|\|\s*")
        .apply(lambda lst: [s.strip() for s in lst] if isinstance(lst, list) else lst)
    )

    # 2) explode to duplicate rows per officer
    df = df.assign(**{col: split_series}).explode(col, ignore_index=True)

    # Treat empty strings as NaN after explode
    df.loc[df[col].astype(str).str.strip().eq(""), col] = pd.NA

    # 3) parse each officer string
    parsed = df[col].apply(_parse_name).apply(pd.Series)
    df[["first_name", "middle_name", "last_name"]] = parsed[["first_name", "middle_name", "last_name"]]

    return df

def clean(): 
    df = (
        pd.read_csv(deba.data("raw/baton_rouge_so/east_baton_rouge_so_sas_2023_2025.csv"))
        .pipe(clean_column_names)
        .rename(columns={"case_number": "tracking_id_og", "final_problem_type": "stop_reason", "call_date": "stop_date", "address": "stop_and_search_location", "disposition": "stop_results"})
        .drop(columns=["tracking_id_og"])
        .pipe(standardize_desc_cols, ["stop_reason"])
        .pipe(clean_disposition)
        .pipe(standardize_desc_cols, ["stop_results", "stop_and_search_location"])
        .pipe(parse_stop_date)
        .pipe(explode_and_parse_primary_officer, col="primary_officers")
        .drop(columns=["primary_officers", "stop_date"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "east-baton-rouge-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["agency", "stop_and_search_year", "stop_and_search_month", "stop_and_search_day"], "tracking_id")
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "stop_reason",
                "first_name",
                "last_name",
                "stop_and_search_location",
                "stop_and_search_time",
            ],
            "stop_and_search_uid",
        )
    )
    return df 


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/sas_baton_rouge_so_2023_2025.csv"), index=False)
