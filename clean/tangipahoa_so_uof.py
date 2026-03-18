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
    uof_24, citizen_24 = clean_24()
    uof_25, citizen_25 = clean_25()
    uof_24.to_csv(deba.data("clean/uof_tangipahoa_so_2024.csv"), index=False)
    uof_25.to_csv(deba.data("clean/uof_tangipahoa_so_2025.csv"), index=False)
    citizen_24.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2024.csv"), index=False
    )
    citizen_25.to_csv(
        deba.data("clean/uof_cit_tangipahoa_so_2025.csv"), index=False
    )
