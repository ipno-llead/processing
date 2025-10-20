import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols, clean_names
from lib.uid import gen_uid
import pandas as pd
import re


def clean_tracking_id(df):
    """Clean IAD tracking numbers"""
    df.loc[:, "tracking_id_og"] = (
        df.iad_number.astype(str)
        .str.strip()
        .str.replace(r"\s*cont\.?.*$", "", regex=True, flags=re.IGNORECASE)
        .str.replace(r"^n/a$", "", regex=True, flags=re.IGNORECASE)
        .str.replace(r"\s+", "-", regex=True)  # Replace spaces with dashes
        .str.strip()
    )
    return df.drop(columns=["iad_number"])


def clean_officer_name(df):
    """Extract officer name and handle special cases"""
    df.loc[:, "officer_name"] = (
        df.officer_name.astype(str)
        .str.strip()
        .str.replace(r"\s*\(.*?\)\s*", " ", regex=True)  # Remove parenthetical info
        .str.replace(r"\s+", " ", regex=True)  # Normalize spaces
        .str.replace(r"^Not legible$", "", regex=True, flags=re.IGNORECASE)
        .str.replace(r"^IA Records Request Letter$", "", regex=True, flags=re.IGNORECASE)
        .str.replace(r"^GPD$", "", regex=True)
        .str.replace(r"^Members of GPD.*", "", regex=True, flags=re.IGNORECASE)
        .str.strip()
    )
    return df


def split_officer_name(df):
    """Split officer name into first and last names"""
    # Pattern handles: First Last, Last, First, etc.
    names = (
        df.officer_name.str.strip()
        .str.replace(r"\s+", " ", regex=True)
        # Handle comma-separated names (Last, First)
        .str.replace(r"^([A-Za-z\-\']+),\s*([A-Za-z]+)", r"\2 \1", regex=True)
        # Remove middle initials/names - just take first and last word
        .str.extract(r"^(?:(\w+).*\s+(\w+)|(\w+))$")
    )

    # If match has 2 names, use them. If only 1 name, put it in first_name
    df.loc[:, "first_name"] = names[0].fillna(names[2]).fillna("").str.strip()
    df.loc[:, "last_name"] = names[1].fillna("").str.strip()

    return df.drop(columns=["officer_name"])


def split_allegation_disposition(df):
    """
    Split the allegation_disposition column which contains
    allegation and disposition separated by '-'
    """
    # First, try to extract disposition at the end
    split_data = (
        df.allegation_disposition.astype(str)
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.extract(
            r"^(.+?)\s*-\s*(Sustained|Unfounded|Unsustained|Not [Ss]ustained|Exonerated|Justified|No Policy Violation)?\s*$"
        )
    )

    df.loc[:, "allegation"] = split_data[0].fillna(df.allegation_disposition).str.strip()
    df.loc[:, "disposition"] = split_data[1].fillna("").str.strip()

    # If no disposition was found but allegation ends with a dash, clean it
    df.loc[:, "allegation"] = (
        df.allegation.str.replace(r"[-\s]+$", "", regex=True).str.strip()
    )

    return df.drop(columns=["allegation_disposition"])


def clean_disposition(df):
    """Standardize disposition values"""
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace("unfounded", "not sustained", regex=False)
        .str.replace("unsustained", "not sustained", regex=False)
        .str.replace("no policy violation", "exonerated", regex=False)
        .str.replace("justified", "exonerated", regex=False)
    )
    return df


def clean_allegation(df):
    """Clean and standardize allegation text"""
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace("inaccurate", "inaccurate", regex=False)
        .str.replace("harassment", "harassment", regex=False)
        .str.replace(r"\biii\b", "illegal", regex=True)
        .str.replace("conduct/unbecoming", "conduct unbecoming", regex=False)
        .str.replace("comm.", "communication", regex=False)
        .str.replace("emp.", "employee", regex=False)
        .str.replace(r"\bdept\b", "department", regex=True)
        .str.replace("unsatis.", "unsatisfactory", regex=False)
        .str.replace("invest.", "investigation", regex=False)
        .str.replace(r"^\.", "", regex=True)  # Remove leading periods
        .str.strip()
    )
    return df


def clean_action(df):
    """Clean and standardize disciplinary action"""
    df.loc[:, "action"] = (
        df.disciplinary_action.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"^nan$", "", regex=True)
        .str.replace(r"^n/a$", "", regex=True)
        .str.replace("suspended", "suspension", regex=False)
        .str.replace("terminated", "termination", regex=False)
        .str.replace(r"(\d+)\s*day\s+suspension", r"\1-day suspension", regex=True)
        .str.replace(r"(\d+)\s*hr\.?\s+suspension", r"\1-hour suspension", regex=True)
        .str.replace(r"\bprob\.?\b", "probation", regex=True)
        .str.replace(r"\byr\.?\b", "year", regex=True)
        .str.strip()
    )
    return df.drop(columns=["disciplinary_action"])


def clean_complainant(df):
    """Extract and standardize complainant information"""
    df.loc[:, "complainant"] = (
        df.complaint_demographics.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"^nan$", "", regex=True)
        .str.replace(r"^departmental supervisor$", "departmental", regex=True)
        .str.replace(r"^policy violation$", "departmental", regex=True)
        .str.replace(r"^damage department equipment$", "departmental", regex=True)
        .str.replace(r"^society$", "civilian", regex=True)
        .str.replace(r"^naacp$", "civilian", regex=True)
        .str.replace(r"^jpso$", "other agency", regex=True)
        .str.replace(r"^cid investigating$", "departmental", regex=True)
        # Extract type from patterns like "civilian-name", "officer-name"
        .str.replace(r"^civilian-.*", "civilian", regex=True)
        .str.replace(r"^officer-.*", "officer", regex=True)
        .str.replace(r"^mayor-.*", "mayor", regex=True)
        .str.replace(r"^employee-.*", "employee", regex=True)
        .str.replace(r"^3rd party civilian.*", "civilian", regex=True)
        .str.replace(r"^complaint\s*-.*", "civilian", regex=True)
        # Handle multiple complainants (keep first type)
        .str.split(",").str[0]
        .str.strip()
    )
    return df.drop(columns=["complaint_demographics"])


def clean_date_fields(df):
    """Clean date fields to handle empty/malformed values before calling clean_dates"""
    for date_col in ["receive_date", "occur_date", "closed_date"]:
        df.loc[:, date_col] = (
            df[date_col].astype(str)
            .str.strip()
            .str.replace(r"^nan$", "", regex=True)
            .str.replace(r"^n/a$", "", regex=True)
            .str.replace(r"^\s*$", "", regex=True)
            # Handle date ranges like "10/2 and 3/2024" - extract first date with year
            .str.replace(r"^(\d+)/(\d+)\s+and\s+\d+/(\d{4})$", r"\1/\2/\3", regex=True)
        )
    return df


def drop_empty_rows(df):
    """Drop rows where essential fields are empty"""
    return df[
        ~(
            (df.first_name == "")
            & (df.last_name == "")
            & (df.allegation == "")
        )
    ].reset_index(drop=True)


def clean():
    df = (
        pd.read_csv(deba.data("raw/gretna_pd/gretna_pd_cprr_2020_2025.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "complaint_received_date": "receive_date",
                "incident_date": "occur_date",
                "complaint_closed_date": "closed_date",
            }
        )
        .pipe(clean_tracking_id)
        .pipe(clean_officer_name)
        .pipe(split_officer_name)
        .pipe(split_allegation_disposition)
        .pipe(clean_disposition)
        .pipe(clean_allegation)
        .pipe(clean_action)
        .pipe(clean_complainant)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_date_fields)
        .pipe(clean_dates, ["receive_date", "occur_date", "closed_date"])
        .pipe(
            standardize_desc_cols,
            ["allegation", "disposition", "action", "complainant"],
        )
        .pipe(drop_empty_rows)
        .pipe(set_values, {"agency": "gretna-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "disposition", "receive_year", "tracking_id_og"],
            "allegation_uid",
        )
        .drop(columns=["source_page"])
        .drop_duplicates(subset=['uid', 'allegation_uid'], keep='first')
        .reset_index(drop=True)
        .fillna("")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_gretna_pd_2020_2025.csv"), index=False)
