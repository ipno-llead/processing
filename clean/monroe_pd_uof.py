import pandas as pd
import deba
from lib.clean import clean_dates, standardize_desc_cols
from lib.columns import set_values
from lib.uid import gen_uid


def split_officer_rows(df):
    """Split semicolon-separated employees into individual rows."""
    df = (
        df.drop("employees", axis=1)
        .join(
            df["employees"]
            .str.split(r"\s*;\s*", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("employees"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_officer_names(df):
    """Parse 'Last, First' or 'Last, Suffix, First' into first/last name."""
    names = df.employees.str.strip().str.lower()
    # handle "Morris, Jr., David" pattern: last, suffix, first
    has_suffix = names.str.contains(r"^[^,]+,\s*(?:jr\.|sr\.|ii|iii|iv),", na=False)
    suffix_parts = names[has_suffix].str.extract(
        r"^([^,]+),\s*(jr\.|sr\.|ii|iii|iv),\s*(.+)$"
    )
    # standard "Last, First" pattern
    std_parts = names[~has_suffix].str.extract(r"^([^,]+),\s*(.+)$")

    df.loc[:, "first_name"] = ""
    df.loc[:, "last_name"] = ""

    df.loc[~has_suffix, "last_name"] = std_parts[0].str.strip()
    df.loc[~has_suffix, "first_name"] = std_parts[1].str.strip()

    df.loc[has_suffix, "last_name"] = suffix_parts[0].str.strip() + " " + suffix_parts[1].str.strip()
    df.loc[has_suffix, "first_name"] = suffix_parts[2].str.strip()

    return df.drop(columns=["employees"])


def clean_tracking_id(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id.str.strip()
    return df


def clean_status(df):
    df.loc[:, "status"] = df.status.str.lower().str.strip()
    return df


def clean_complete():
    df = pd.read_csv(
        deba.data("raw/monroe_pd/monroe_pd_uof_2024_2026_complete.csv"), header=0
    )
    df.columns = [
        "tracking_id", "creator", "employees", "incident_date",
        "incident_location", "status",
    ]
    df = df[df.employees.notna() & (df.employees.str.strip() != "")]
    df = (
        df.pipe(split_officer_rows)
        .pipe(split_officer_names)
        .pipe(clean_tracking_id)
        .pipe(clean_status)
        .pipe(clean_dates, ["incident_date"])
        .pipe(
            standardize_desc_cols,
            ["incident_location", "tracking_id", "creator"],
        )
        .drop(columns=["creator"])
        .rename(
            columns={
                "incident_year": "occurred_year",
                "incident_month": "occurred_month",
                "incident_day": "occurred_day",
            }
        )
    )
    return df


def clean_review():
    df = pd.read_csv(
        deba.data("raw/monroe_pd/monroe_pd_uof_2024_2026_review.csv"), header=0
    )
    df.columns = [
        "tracking_id", "creator", "employees", "incident_date",
        "incident_location", "status", "reviewer",
    ]
    df = df.drop(columns=["reviewer"])
    df = df[df.employees.notna() & (df.employees.str.strip() != "")]
    df = (
        df.pipe(split_officer_rows)
        .pipe(split_officer_names)
        .pipe(clean_tracking_id)
        .pipe(clean_status)
        .pipe(clean_dates, ["incident_date"])
        .pipe(
            standardize_desc_cols,
            ["incident_location", "tracking_id", "creator"],
        )
        .drop(columns=["creator"])
        .rename(
            columns={
                "incident_year": "occurred_year",
                "incident_month": "occurred_month",
                "incident_day": "occurred_day",
            }
        )
    )
    return df


def clean():
    complete = clean_complete()
    review = clean_review()

    df = (
        pd.concat([complete, review], ignore_index=True)
        .pipe(set_values, {"agency": "monroe-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "tracking_id", "occurred_year", "occurred_month", "occurred_day"],
            "uof_uid",
        )
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )

    uof_df = df[
        [
            "tracking_id",
            "tracking_id_og",
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "incident_location",
            "status",
            "first_name",
            "last_name",
            "agency",
            "uid",
            "uof_uid",
        ]
    ].drop_duplicates(subset=["uid", "uof_uid"])

    return uof_df


if __name__ == "__main__":
    uof = clean()
    uof.to_csv(deba.data("clean/uof_monroe_pd_2024_2026.csv"), index=False)
