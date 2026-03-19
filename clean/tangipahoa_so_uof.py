import re

import pandas as pd
import deba
from lib.clean import (
    clean_races,
    clean_sexes,
    standardize_desc_cols,
    strip_leading_comma,
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def split_age_personnel_id(df):
    """Split 'age_report_personnel_id' into citizen_age and report_personnel_id.

    Values like '11 130' -> age=11, id=130
    Values like '121' -> age='', id=121
    """
    parts = df.age_report_personnel_id.str.extract(r"^(\d+)\s+(\d+)$")
    single = df.age_report_personnel_id.str.extract(r"^(\d+)$")

    df.loc[:, "citizen_age"] = parts[0].fillna("")
    df.loc[:, "report_personnel_id"] = parts[1].fillna(single[0]).fillna("")
    return df.drop(columns=["age_report_personnel_id"])


def clean_event_type(df):
    df.loc[:, "use_of_force_reason"] = (
        df.event_type.str.lower()
        .str.strip()
        .str.replace(r"\.", ",", regex=True)
        .str.replace(r"w\/(\w+)", r"with \1", regex=True)
    )
    return df.drop(columns=["event_type"])


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.report_disposition.str.lower()
        .str.strip()
        .str.replace(r"^$", "", regex=True)
    )
    return df.drop(columns=["report_disposition"])


def clean_injured_by(df):
    df.loc[:, "use_of_force_description"] = (
        df.injured_by.str.lower()
        .str.strip()
        .str.replace(r"himself", "self", regex=False)
    )
    return df.drop(columns=["injured_by"])


def clean_subject_injured(df):
    df.loc[:, "use_of_force_result"] = (
        df.subject_injured.str.lower()
        .str.strip()
        .apply(lambda x: " ".join(dict.fromkeys(str(x).split())) if pd.notna(x) else x)
        .str.replace(r"^no$", "no injury", regex=True)
        .str.replace(r"^yes$", "injured", regex=True)
    )
    return df.drop(columns=["subject_injured"])


def clean_injury_severity(df):
    df.loc[:, "injury_severity"] = (
        df.subject_injury_severity.str.lower().str.strip()
    )
    return df.drop(columns=["subject_injury_severity"])


def normalize_race_col(series):
    normalized = (
        series.str.lower()
        .str.strip()
        .str.replace(
            r"blac?k\s*(or\s*)?african(\s*american)?", "black", regex=True
        )
        .str.replace(r"white\s*(or\s*)?caucasian", "white", regex=True)
        .str.replace(r"\bafrican(\s*american)?\b", "", regex=True)
        .str.strip()
    )
    # Deduplicate repeated race tokens (e.g., "black black" -> "black")
    return normalized.apply(
        lambda x: " ".join(dict.fromkeys(str(x).split())) if pd.notna(x) else x
    )


def clean_employee_race_col(df):
    df.loc[:, "officer_race"] = normalize_race_col(df.employee_race)
    return df.drop(columns=["employee_race"])


def clean_employee_gender(df):
    df.loc[:, "officer_sex"] = (
        df.employee_gender.str.lower()
        .str.strip()
        .apply(lambda x: " ".join(dict.fromkeys(str(x).split())) if pd.notna(x) else x)
    )
    return df.drop(columns=["employee_gender"])


def clean_employee_group(df):
    df.loc[:, "department_desc"] = df.employee_group.str.lower().str.strip()
    return df.drop(columns=["employee_group"])


def clean_employee_rank(df):
    df.loc[:, "rank_desc"] = df.employee_rank_title.str.lower().str.strip()
    return df.drop(columns=["employee_rank_title"])


def split_employee_name(df):
    names = df.employee_name_handler.str.extract(r"(\w+)\s+(.+)")
    df.loc[:, "first_name"] = names[0].str.lower().str.strip()
    df.loc[:, "last_name"] = names[1].str.lower().str.strip()
    return df.drop(columns=["employee_name_handler"])


def clean_citizen_race(df):
    df.loc[:, "citizen_race"] = normalize_race_col(df.subject_race)
    return df.drop(columns=["subject_race"])


def clean_citizen_sex(df):
    df.loc[:, "citizen_sex"] = (
        df.subject_gender.str.lower()
        .str.strip()
        .apply(lambda x: " ".join(dict.fromkeys(str(x).split())) if pd.notna(x) else x)
    )
    return df.drop(columns=["subject_gender"])


def split_datetime_col(df):
    """Split event_date and event_time into occurred_date and occurred_time."""
    df.loc[:, "occurred_date"] = df.event_date.str.strip()
    df.loc[:, "occurred_time"] = df.event_time.str.strip()
    return df.drop(columns=["event_date", "event_time"])


def split_datetime_combined(df):
    """Split combined event_date_time column into occurred_date and occurred_time."""
    parts = df.event_date_time.str.extract(r"(\d{4}-\d{2}-\d{2})\s+(.+)")
    df.loc[:, "occurred_date"] = parts[0].fillna("")
    df.loc[:, "occurred_time"] = parts[1].fillna("").str.strip()
    return df.drop(columns=["event_date_time"])


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean_employee_rank_25(df):
    df.loc[:, "rank_desc"] = df.employee_rank.str.lower().str.strip()
    return df.drop(columns=["employee_rank"])


def dedup_concatenated_rows(df):
    """Fix rows where cell values are duplicated/concatenated.

    Identifies concatenated rows by checking if tracking_id contains two
    10-digit numbers (e.g., '2025003237 2025003237').

    - Same-event duplicates: drops if non-concat rows exist for that event,
      otherwise keeps with tracking_id fixed.
    - Different-event merges: splits into two separate rows.
    """
    concat_mask = df.tracking_id.str.contains(r"^\d+\s+\d+$", na=False)
    if not concat_mask.any():
        return df

    non_concat = df[~concat_mask]
    new_rows = []
    drop_indices = []

    for idx in df[concat_mask].index:
        tid = str(df.loc[idx, "tracking_id"]).strip()
        parts = tid.split()
        first_tid = parts[0]
        second_tid = parts[1] if len(parts) > 1 else first_tid

        if first_tid == second_tid:
            # Same event duplicated
            has_non_concat = (non_concat.tracking_id == first_tid).any()
            if has_non_concat:
                # Safe to drop - non-concat rows cover this event
                drop_indices.append(idx)
            else:
                # Only concat version exists - dedup all fields
                df.loc[idx, "tracking_id"] = first_tid
                for col in df.columns:
                    if col == "tracking_id":
                        continue
                    val = df.loc[idx, col]
                    if pd.isnull(val) or str(val).strip() == "":
                        continue
                    val = str(val).strip()
                    # Try character midpoint split
                    mid = len(val) // 2
                    left = val[:mid].strip()
                    right = val[mid:].strip()
                    if left == right:
                        df.loc[idx, col] = left
                        continue
                    # Try word-based dedup: check if removing
                    # duplicate word sequences yields a shorter value
                    words = val.split()
                    if len(words) >= 2:
                        # Try even split
                        if len(words) % 2 == 0:
                            half = len(words) // 2
                            if words[:half] == words[half:]:
                                df.loc[idx, col] = " ".join(words[:half])
                                continue
                        # Use ordered dedup: remove consecutive dupes
                        result = list(dict.fromkeys(words))
                        if len(result) < len(words):
                            df.loc[idx, col] = " ".join(result)
        else:
            # Two different events merged - drop and add split rows
            drop_indices.append(idx)
            # Split using event_date_time as anchor: find second date
            row_data = df.loc[idx]
            new_rows.extend(
                _split_diff_event_row(row_data, first_tid, second_tid)
            )

    if drop_indices:
        df = df.drop(index=drop_indices)
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    else:
        df = df.reset_index(drop=True)

    return df


def _split_diff_event_row(row, first_tid, second_tid):
    """Split a row containing two different concatenated events.

    Uses known field patterns and event type keywords to find split boundaries.
    Returns a list of two row dicts.
    """
    import re

    row1 = {"tracking_id": first_tid}
    row2 = {"tracking_id": second_tid}

    # Known event type keywords that mark the start of an event type
    event_type_keywords = [
        "Assault", "Disturbance", "Other", "Wanted person",
        "Assisting another officer", "Medical emergency", "Eluding",
        "Subject", "Welfare check", "Domestic", "Threats",
        "Possible", "Motor vehicle",
    ]

    def _split_by_known_patterns(val, patterns):
        """Split a value by finding the second occurrence of known patterns."""
        for pat in patterns:
            # Find second occurrence
            first_pos = val.find(pat)
            if first_pos >= 0:
                second_pos = val.find(pat, first_pos + len(pat))
                if second_pos > 0:
                    return val[:second_pos].strip(), val[second_pos:].strip()
        return None

    # Known race patterns
    race_keywords = [
        "White Caucasian", "White or Caucasian",
        "Black or African American", "Black African American",
    ]

    # Split event_type using known event type keywords
    evt_val = str(row.get("event_type", "")).strip()
    evt_split = None
    for kw in event_type_keywords:
        pos = evt_val.find(kw)
        if pos >= 0:
            second_pos = evt_val.find(kw, pos + len(kw))
            if second_pos > 0:
                evt_split = (evt_val[:second_pos].strip(), evt_val[second_pos:].strip())
                break
    if evt_split is None:
        # Try finding any keyword after position > 0
        for kw in event_type_keywords:
            pos = evt_val.find(kw)
            if pos > 0:
                evt_split = (evt_val[:pos].strip(), evt_val[pos:].strip())
                break
    if evt_split:
        row1["event_type"] = evt_split[0]
        row2["event_type"] = evt_split[1]
    else:
        row1["event_type"] = evt_val
        row2["event_type"] = evt_val

    # Split employee_race using known race patterns
    race_val = str(row.get("employee_race", "")).strip()
    race_split = None
    for kw in race_keywords:
        pos = race_val.find(kw)
        if pos >= 0:
            remaining = race_val[pos + len(kw):].strip()
            if remaining:
                race_split = (kw, remaining)
                break
    if race_split:
        row1["employee_race"] = race_split[0]
        row2["employee_race"] = race_split[1]
    else:
        row1["employee_race"] = race_val
        row2["employee_race"] = race_val

    # Split subject_race using known race patterns
    srace_val = str(row.get("subject_race", "")).strip()
    srace_split = _split_by_known_patterns(srace_val, race_keywords)
    if srace_split:
        row1["subject_race"] = srace_split[0]
        row2["subject_race"] = srace_split[1]
    else:
        row1["subject_race"] = srace_val
        row2["subject_race"] = srace_val

    # Split employee_name_handler by name pattern (FirstName LastName)
    name_val = str(row.get("employee_name_handler", "")).strip()
    name_parts = re.findall(r"[A-Z][a-z]+\s+[A-Z][a-z]+", name_val)
    if len(name_parts) >= 2:
        row1["employee_name_handler"] = name_parts[0]
        row2["employee_name_handler"] = name_parts[1]
    else:
        row1["employee_name_handler"] = name_val
        row2["employee_name_handler"] = name_val

    # Split employee_rank by known ranks
    rank_val = str(row.get("employee_rank", "")).strip()
    ranks = re.findall(
        r"(?:Deputy|Sergeant|Detective|Corporal|Lieutenant|Captain)",
        rank_val,
        re.IGNORECASE,
    )
    if len(ranks) >= 2:
        row1["employee_rank"] = ranks[0]
        row2["employee_rank"] = ranks[1]
    else:
        row1["employee_rank"] = rank_val
        row2["employee_rank"] = rank_val

    # Split single-word pair fields (e.g., "Male Male", "No No")
    simple_split_cols = [
        "employee_gender", "subject_gender", "subject_injured",
    ]
    for col in simple_split_cols:
        val = str(row.get(col, "")).strip()
        words = val.split()
        if len(words) == 2:
            row1[col] = words[0]
            row2[col] = words[1]
        else:
            row1[col] = val
            row2[col] = val

    # Split event_date_time by finding second date
    dt_val = str(row.get("event_date_time", "")).strip()
    dt_matches = list(re.finditer(r"\d{4}-", dt_val))
    if len(dt_matches) >= 2:
        split_pos = dt_matches[1].start()
        row1["event_date_time"] = dt_val[:split_pos].strip()
        row2["event_date_time"] = dt_val[split_pos:].strip()
    else:
        row1["event_date_time"] = dt_val
        row2["event_date_time"] = dt_val

    # Split report_disposition using known disposition keywords
    disp_keywords = ["Within Policy", "No Further Action", "Needs Further Review"]
    disp_val = str(row.get("report_disposition", "")).strip()
    disp_split = _split_by_known_patterns(disp_val, disp_keywords)
    if disp_split:
        row1["report_disposition"] = disp_split[0]
        row2["report_disposition"] = disp_split[1]
    else:
        row1["report_disposition"] = disp_val
        row2["report_disposition"] = disp_val

    # Even word-count split for remaining fields
    even_split_cols = [
        "address", "city", "employee_group",
        "injured_by", "subject_injury_severity",
        "age_report_personnel_id", "employee_years_of_service",
    ]
    for col in even_split_cols:
        val = str(row.get(col, "")).strip()
        if not val or val == "nan":
            row1[col] = ""
            row2[col] = ""
            continue
        words = val.split()
        if len(words) >= 2 and len(words) % 2 == 0:
            half = len(words) // 2
            row1[col] = " ".join(words[:half])
            row2[col] = " ".join(words[half:])
        else:
            row1[col] = val
            row2[col] = val

    return [row1, row2]


def clean_years_of_service(df):
    df.loc[:, "years_of_service"] = (
        df.employee_years_of_service.str.strip()
        .apply(lambda x: " ".join(dict.fromkeys(str(x).split())) if pd.notna(x) else x)
    )
    return df.drop(columns=["employee_years_of_service"])


def split_race_gender(df, col):
    """Split combined race/gender codes like 'BM' into citizen_race and citizen_sex.

    Mappings: B=black, W=white, H=hispanic; M=male, F=female.
    Edge cases: 'Dog', '0', 'WN', empty → left as-is or blank.
    """
    val = df[col].str.strip()
    # Extract single-char race code and single-char gender code
    parts = val.str.extract(r"^([BWH])([MF])$", flags=re.IGNORECASE)
    race_map = {"b": "black", "w": "white", "h": "hispanic"}
    sex_map = {"m": "male", "f": "female"}
    df.loc[:, "citizen_race"] = (
        parts[0].str.lower().map(race_map).fillna("")
    )
    df.loc[:, "citizen_sex"] = (
        parts[1].str.lower().map(sex_map).fillna("")
    )
    return df.drop(columns=[col])


def melt_force_columns(df, force_col_map):
    """Convert boolean force-type columns into a single text description.

    force_col_map: dict mapping column name -> label, e.g. {"oc": "oc", "jpx": "jpx"}
    For each row, collects labels where value is '1' or 1, joins with ', '.
    """
    force_cols = [c for c in force_col_map if c in df.columns]

    def _row_desc(row):
        parts = []
        for col in force_cols:
            v = str(row[col]).strip()
            if v == "1":
                parts.append(force_col_map[col])
        return ", ".join(parts)

    df.loc[:, "use_of_force_description"] = df.apply(_row_desc, axis=1)
    return df.drop(columns=force_cols)


def split_deputy_name_col(df, col):
    """Split a single deputy name column into first_name and last_name."""
    names = df[col].str.extract(r"^(\S+)\s+(.+)$")
    df.loc[:, "first_name"] = names[0].str.lower().str.strip()
    df.loc[:, "last_name"] = names[1].str.lower().str.strip()
    return df.drop(columns=[col])


def clean_arrest_col(df, col):
    """Normalize arrest column to citizen_arrested yes/no."""
    df.loc[:, "citizen_arrested"] = (
        df[col].astype(str).str.strip()
        .replace({"1": "yes", "2": "yes", "0": "no", "": ""})
    )
    return df.drop(columns=[col])


def clean_treated_hospital(df, treated_col=None, hospital_col=None):
    """Normalize treated-at-scene and hospital columns into use_of_force_result."""
    parts = []
    if treated_col and treated_col in df.columns:
        parts.append(("treated at scene", treated_col))
    if hospital_col and hospital_col in df.columns:
        parts.append(("hospital", hospital_col))

    def _result(row):
        results = []
        for label, col in parts:
            if str(row[col]).strip() == "1":
                results.append(label)
        if results:
            return ", ".join(results)
        return ""

    if parts:
        df.loc[:, "use_of_force_result"] = df.apply(_result, axis=1)
        df = df.drop(columns=[c for _, c in parts])
    return df


def clean_time_military(df, col):
    """Convert military time like '1146' or '2300' to HH:MM format.

    Values of '0' or empty become ''.
    """
    def _fmt(val):
        val = str(val).strip()
        if val in ("0", "", "nan"):
            return ""
        # Already has colon
        if ":" in val:
            return val
        # Pad to 4 digits
        val = val.zfill(4)
        if len(val) == 4 and val.isdigit():
            return f"{val[:2]}:{val[2:]}"
        return val

    df.loc[:, "occurred_time"] = df[col].apply(_fmt)
    return df.drop(columns=[col])


def finalize_old_uof(df, year):
    """Shared finalization for 2014-2019 clean functions.

    Generates UIDs, deduplicates, and splits into UOF + citizen dataframes.
    Rows without officer names are excluded from UOF but kept in citizen df.
    """
    has_name = df.first_name.notna() & df.first_name.str.strip().ne("")
    df_with_name = df[has_name].copy()
    df_no_name = df[~has_name].copy()

    df_with_name = (
        df_with_name.pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(set_values, {"agency": "tangipahoa-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "occurred_date",
                "use_of_force_description",
                "citizen_race",
                "citizen_sex",
                "citizen_age",
            ],
            "uof_uid",
        )
    )

    df_with_name = df_with_name.drop_duplicates(subset=["uid", "uof_uid"])

    # Build UOF columns list based on what's available
    uof_cols = ["use_of_force_description", "occurred_date"]
    if "occurred_time" in df_with_name.columns:
        uof_cols.append("occurred_time")
    if "citizen_arrested" in df_with_name.columns:
        uof_cols.append("citizen_arrested")
    if "use_of_force_result" in df_with_name.columns:
        uof_cols.append("use_of_force_result")
    uof_cols.extend(["first_name", "last_name", "agency", "uid", "uof_uid"])

    dfa = df_with_name[uof_cols]

    # Citizen df: include citizens from both named and unnamed officer rows
    cit_named = df_with_name[
        ["uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"]
    ]

    if len(df_no_name) > 0:
        df_no_name = (
            df_no_name.pipe(clean_sexes, ["citizen_sex"])
            .pipe(clean_races, ["citizen_race"])
            .pipe(set_values, {"agency": "tangipahoa-so"})
        )
        # Generate a placeholder uof_uid from citizen + date info
        df_no_name.loc[:, "uof_uid"] = (
            df_no_name[["occurred_date", "citizen_race", "citizen_sex", "citizen_age"]]
            .astype(str)
            .agg(", ".join, axis=1)
            .map(lambda x: __import__("hashlib").md5(x.encode("utf-8")).hexdigest())
        )
        cit_no_name = df_no_name[
            ["uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"]
        ]
        cit_all = pd.concat([cit_named, cit_no_name], ignore_index=True)
    else:
        cit_all = cit_named

    dfb = (
        cit_all
        .drop_duplicates(subset=["uof_uid", "citizen_age", "citizen_sex", "citizen_race"])
        .pipe(
            gen_uid,
            ["uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"],
            "citizen_uid",
        )
    )

    return dfa, dfb


def clean_14():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_uof_2014.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
    )
    df = df.rename(columns={
        "suspect_race": "citizen_race_raw",
        "suspect_gender": "citizen_sex_raw",
        "suspect_age": "citizen_age",
    })
    # Race and gender are separate columns in 2014
    df.loc[:, "citizen_race"] = (
        df.citizen_race_raw.str.lower().str.strip()
        .replace({"w": "white", "b": "black", "h": "hispanic", "u": ""})
    )
    df.loc[:, "citizen_sex"] = (
        df.citizen_sex_raw.str.lower().str.strip()
        .replace({"m": "male", "f": "female"})
    )
    df = df.drop(columns=["citizen_race_raw", "citizen_sex_raw"])
    df.loc[:, "citizen_age"] = df.citizen_age.astype(str).str.strip().replace({"0": ""})
    df.loc[:, "occurred_date"] = df.date.str.strip()
    df = df.drop(columns=["date"])
    df = df.pipe(split_deputy_name_col, "deputy")
    df = df.pipe(melt_force_columns, {
        "hand": "hand", "oc": "oc", "jpx": "jpx", "baton": "baton",
        "display": "display firearm", "other": "other", "k9_bite": "k9 bite",
    })
    df = df.pipe(clean_arrest_col, "arrest")
    df = df.pipe(
        clean_treated_hospital,
        treated_col="treated_at_scene",
        hospital_col="hospital",
    )
    return finalize_old_uof(df, 2014)


def clean_15():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_uof_2015.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
    )
    # Filter out non-date rows (e.g. "Suspect 2")
    df = df[~df.date.str.contains(r"[a-zA-Z]", na=True)].reset_index(drop=True)
    df.loc[:, "citizen_age"] = df.age.astype(str).str.strip().replace({"0": ""})
    df = df.drop(columns=["age"])
    df.loc[:, "occurred_date"] = df.date.str.strip()
    df = df.drop(columns=["date"])
    df = df.pipe(split_deputy_name_col, "name")
    df = df.pipe(split_race_gender, "race")
    df = df.pipe(melt_force_columns, {
        "no_weapons": "weaponless", "oc": "oc", "jpx": "jpx",
        "impact_weapon": "impact weapon",
        "display_firearm": "display firearm",
        "discharge_firearm": "discharge firearm",
    })
    df = df.pipe(clean_arrest_col, "arrest")
    return finalize_old_uof(df, 2015)


def clean_16():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_uof_2016.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
    )
    df.loc[:, "citizen_age"] = (
        df.suspect_age.astype(str).str.strip().replace({"0": ""})
    )
    df = df.drop(columns=["suspect_age"])
    df.loc[:, "occurred_date"] = df.date.str.strip()
    df = df.drop(columns=["date"])
    df = df.pipe(split_deputy_name_col, "deputy_name")
    df = df.pipe(split_race_gender, "suspect_race")
    df = df.pipe(melt_force_columns, {
        "no_weapons": "weaponless", "oc": "oc", "ipx": "ipx",
        "impact_weapon": "impact weapon",
        "display_firearm": "display firearm",
        "discharge_firearm": "discharge firearm",
        "k9_bite": "k9 bite",
    })
    df = df.pipe(clean_arrest_col, "arrest")
    return finalize_old_uof(df, 2016)


def clean_17():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_uof_2017.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
    )
    df.loc[:, "citizen_age"] = (
        df.suspect_age.astype(str).str.strip().replace({"0": ""})
    )
    df = df.drop(columns=["suspect_age"])
    df.loc[:, "occurred_date"] = df.date.str.strip()
    df = df.drop(columns=["date"])
    df = df.pipe(split_deputy_name_col, "name")
    df = df.pipe(split_race_gender, "suspect_race")
    df = df.pipe(melt_force_columns, {
        "no_weapons": "weaponless", "oc": "oc", "ipx": "ipx",
        "impact_weapon": "impact weapon",
        "discharge_firearm": "discharge firearm",
        "display_firearm": "display firearm",
        "k9_bite": "k9 bite",
    })
    df = df.pipe(clean_arrest_col, "arrest_made")
    df = df.pipe(
        clean_treated_hospital,
        treated_col="treated_at_scene",
        hospital_col="hospital",
    )
    if "deputy_injury" in df.columns:
        df = df.drop(columns=["deputy_injury"])
    return finalize_old_uof(df, 2017)


def clean_18():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_uof_2018.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
    )
    df.loc[:, "citizen_age"] = (
        df.suspect_age.astype(str).str.strip().replace({"0": ""})
    )
    df = df.drop(columns=["suspect_age"])
    df.loc[:, "occurred_date"] = df.date.str.strip()
    df = df.drop(columns=["date"])
    df = df.pipe(clean_time_military, "time")
    df = df.pipe(split_deputy_name_col, "deputy_name")
    df = df.pipe(split_race_gender, "suspect_race")
    df = df.pipe(melt_force_columns, {
        "weaponless": "weaponless", "oc": "oc", "cle": "cle",
        "impact_weapon": "impact weapon",
        "discharge_firearm": "discharge firearm",
        "display_firearm": "display firearm",
        "k9_bite": "k9 bite",
    })
    df = df.pipe(clean_arrest_col, "arrest")
    df = df.pipe(
        clean_treated_hospital,
        treated_col="treated_at_scene",
        hospital_col="hospital",
    )
    return finalize_old_uof(df, 2018)


def clean_19():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_uof_2019.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
    )
    df.loc[:, "citizen_age"] = (
        df.suspect_age.astype(str).str.strip().replace({"0": "", "00": ""})
    )
    df = df.drop(columns=["suspect_age"])
    df.loc[:, "occurred_date"] = df.date.str.strip()
    df = df.drop(columns=["date"])
    df = df.pipe(clean_time_military, "time")
    df = df.pipe(split_deputy_name_col, "deputy_name")
    df = df.pipe(split_race_gender, "suspect_race")
    df = df.pipe(melt_force_columns, {
        "weaponless": "weaponless", "oc": "oc", "cle": "cle",
        "baton": "baton",
        "display_fa": "display firearm",
        "discharge_fa": "discharge firearm",
    })
    df = df.pipe(clean_arrest_col, "arrest_made")
    df = df.pipe(
        clean_treated_hospital,
        treated_col="treated_at_scene",
        hospital_col="hospital",
    )
    if "suspect_injuries" in df.columns:
        df = df.drop(columns=["suspect_injuries"])
    return finalize_old_uof(df, 2019)


def read_2025_csv():
    """Read 2025 CSV which has first data row baked into headers.

    Manually assign column names and reconstruct the first data row.
    """
    col_names = [
        "event",
        "event_date_time",
        "event_type",
        "address",
        "city",
        "employee_group",
        "employee_name_handler",
        "employee_race",
        "employee_gender",
        "employee_rank",
        "report_disposition",
        "subject_gender",
        "subject_injured",
        "subject_race",
        "injured_by",
        "subject_injury_severity",
        "age_report_personnel_id",
        "employee_years_of_service",
        "unnamed",
    ]

    # Read data rows (skip the malformed header)
    df = pd.read_csv(
        deba.data("raw/tangipahoa_so/tangipahoa_so_uof_2025.csv"),
        header=None,
        skiprows=1,
    )
    df.columns = col_names

    # Reconstruct first row from header
    first_row = pd.DataFrame(
        [
            {
                "event": "'2025002334",
                "event_date_time": "'2025-01-21 10:29 AM",
                "event_type": "'Disturbance (drinking, fighting, disorderly)",
                "address": "'101 Campo Lane",
                "city": "'Amite",
                "employee_group": "'Corrections Operations",
                "employee_name_handler": "'Jordan Joshlin",
                "employee_race": "'White Caucasian",
                "employee_gender": "'Female",
                "employee_rank": "'Corporal",
                "report_disposition": "'Within Policy",
                "subject_gender": "'Male",
                "subject_injured": "'No",
                "subject_race": "'Black or African American",
                "injured_by": "'",
                "subject_injury_severity": "'",
                "age_report_personnel_id": "'25 554",
                "employee_years_of_service": "'0",
                "unnamed": None,
            }
        ]
    )

    df = pd.concat([first_row, df], ignore_index=True)
    return df


def clean_24():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_uof_2024.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "event": "tracking_id",
                "subject_injured_yes_no": "subject_injured",
            }
        )
        .pipe(strip_leading_comma)
        .pipe(split_datetime_col)
        .pipe(clean_event_type)
        .pipe(clean_disposition)
        .pipe(clean_injured_by)
        .pipe(clean_subject_injured)
        .pipe(clean_injury_severity)
        .pipe(clean_employee_group)
        .pipe(clean_employee_rank)
        .pipe(split_employee_name)
        .pipe(clean_citizen_race)
        .pipe(clean_citizen_sex)
        .pipe(clean_employee_race_col)
        .pipe(clean_employee_gender)
        .pipe(split_age_personnel_id)
        .pipe(
            standardize_desc_cols,
            ["address", "city", "tracking_id", "disposition"],
        )
        .pipe(clean_sexes, ["citizen_sex", "officer_sex"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(set_values, {"agency": "tangipahoa-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "use_of_force_reason",
                "use_of_force_description",
                "occurred_date",
                "occurred_time",
            ],
            "uof_uid",
        )
        .pipe(
            gen_uid,
            ["citizen_race", "uof_uid", "citizen_sex", "citizen_age"],
            "uof_citizen_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )

    df = df.drop_duplicates(subset=["uid", "uof_uid"])

    dfa = df[
        [
            "tracking_id",
            "tracking_id_og",
            "use_of_force_reason",
            "address",
            "city",
            "disposition",
            "occurred_date",
            "occurred_time",
            "use_of_force_result",
            "use_of_force_description",
            "injury_severity",
            "department_desc",
            "rank_desc",
            "officer_race",
            "officer_sex",
            "first_name",
            "last_name",
            "report_personnel_id",
            "agency",
            "uid",
            "uof_uid",
        ]
    ]

    dfb = (
        df[
            ["tracking_id_og", "uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"]
        ]
        .drop_duplicates(subset=["tracking_id_og", "citizen_age", "citizen_sex", "citizen_race"])
        .drop(columns=["tracking_id_og"])
        .pipe(
            gen_uid,
            ["uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"],
            "citizen_uid",
        )
    )

    return dfa, dfb


def clean_25():
    df = (
        read_2025_csv()
        .drop(columns=["unnamed"])
        .rename(columns={"event": "tracking_id"})
        .pipe(strip_leading_comma)
        .pipe(dedup_concatenated_rows)
        .pipe(split_datetime_combined)
        .pipe(clean_event_type)
        .pipe(clean_disposition)
        .pipe(clean_injured_by)
        .pipe(clean_subject_injured)
        .pipe(clean_injury_severity)
        .pipe(clean_employee_group)
        .pipe(clean_employee_rank_25)
        .pipe(split_employee_name)
        .pipe(clean_citizen_race)
        .pipe(clean_citizen_sex)
        .pipe(clean_employee_race_col)
        .pipe(clean_employee_gender)
        .pipe(split_age_personnel_id)
        .pipe(clean_years_of_service)
        .pipe(
            standardize_desc_cols,
            ["address", "city", "tracking_id", "disposition"],
        )
        .pipe(clean_sexes, ["citizen_sex", "officer_sex"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(set_values, {"agency": "tangipahoa-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "use_of_force_reason",
                "disposition",
                "use_of_force_description",
                "occurred_date",
                "occurred_time",
                "use_of_force_result",
            ],
            "uof_uid",
        )
        .pipe(
            gen_uid,
            ["citizen_race", "citizen_sex", "citizen_age"],
            "uof_citizen_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )

    df = df.drop_duplicates(subset=["uid", "uof_uid"])

    dfa = df[
        [
            "tracking_id",
            "tracking_id_og",
            "use_of_force_reason",
            "address",
            "city",
            "disposition",
            "occurred_date",
            "occurred_time",
            "use_of_force_result",
            "use_of_force_description",
            "injury_severity",
            "department_desc",
            "rank_desc",
            "officer_race",
            "officer_sex",
            "first_name",
            "last_name",
            "report_personnel_id",
            "years_of_service",
            "agency",
            "uid",
            "uof_uid",
        ]
    ]

    dfb = (
        df[
            ["tracking_id_og", "uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"]
        ]
        .drop_duplicates(subset=["tracking_id_og", "citizen_age", "citizen_sex", "citizen_race"])
        .drop(columns=["tracking_id_og"])
        .pipe(
            gen_uid,
            ["uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"],
            "citizen_uid",
        )
    )

    return dfa, dfb


if __name__ == "__main__":
    uof_14, citizen_14 = clean_14()
    uof_15, citizen_15 = clean_15()
    uof_16, citizen_16 = clean_16()
    uof_17, citizen_17 = clean_17()
    uof_18, citizen_18 = clean_18()
    uof_19, citizen_19 = clean_19()
    uof_24, citizen_24 = clean_24()
    uof_25, citizen_25 = clean_25()
    uof_14.to_csv(deba.data("clean/uof_tangipahoa_so_2014.csv"), index=False)
    uof_15.to_csv(deba.data("clean/uof_tangipahoa_so_2015.csv"), index=False)
    uof_16.to_csv(deba.data("clean/uof_tangipahoa_so_2016.csv"), index=False)
    uof_17.to_csv(deba.data("clean/uof_tangipahoa_so_2017.csv"), index=False)
    uof_18.to_csv(deba.data("clean/uof_tangipahoa_so_2018.csv"), index=False)
    uof_19.to_csv(deba.data("clean/uof_tangipahoa_so_2019.csv"), index=False)
    uof_24.to_csv(deba.data("clean/uof_tangipahoa_so_2024.csv"), index=False)
    uof_25.to_csv(deba.data("clean/uof_tangipahoa_so_2025.csv"), index=False)
    citizen_14.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2014.csv"), index=False
    )
    citizen_15.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2015.csv"), index=False
    )
    citizen_16.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2016.csv"), index=False
    )
    citizen_17.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2017.csv"), index=False
    )
    citizen_18.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2018.csv"), index=False
    )
    citizen_19.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2019.csv"), index=False
    )
    citizen_24.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2024.csv"), index=False
    )
    citizen_25.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2025.csv"), index=False
    )
