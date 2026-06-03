import pandas as pd
import deba
from lib.clean import clean_dates, clean_sexes, clean_races, standardize_desc_cols
from lib.columns import set_values
from lib.uid import gen_uid


def read_pprr_names():
    """Read PPRR to build badge-to-name lookup."""
    df = pd.read_csv(
        deba.data("raw/vermilion_so/vermilion_so_pprr_2026.csv"),
        header=0,
        usecols=[0, 1],
    )
    df.columns = ["badge_no", "employee_name"]
    df.loc[:, "badge_no"] = (
        df.badge_no.str.replace(r"^'", "", regex=True).str.strip()
    )
    df.loc[:, "employee_name"] = (
        df.employee_name.str.replace(r"^'", "", regex=True).str.strip()
    )
    df = df[df.badge_no != ""]
    parts = df.employee_name.str.extract(r"^([^,]+),\s*(.+)$")
    df.loc[:, "last_name"] = parts[0].str.lower().str.strip()
    df.loc[:, "first_name"] = parts[1].str.lower().str.strip()
    return df[["badge_no", "first_name", "last_name"]].drop_duplicates(
        subset=["badge_no"]
    )


def extract_rank_and_badge(df):
    """Parse 'Sgt. 9150' or 'Dpty. 117' into rank_desc and badge_no."""
    parts = df.rank_badge.str.extract(r"^([A-Za-z]+)\s*\.?\s*(\d+)$")
    df.loc[:, "rank_desc"] = (
        parts[0]
        .str.strip()
        .str.lower()
        .str.replace(r"^sgt$", "sergeant", regex=True)
        .str.replace(r"^dpty$", "deputy", regex=True)
    )
    df.loc[:, "badge_no"] = parts[1].str.strip()
    return df.drop(columns=["rank_badge"])


def extract_datetime(df):
    parts = df.datetime.str.extract(
        r"^(\d{1,2}/\d{1,2}/\d{4})\s+(.+)$"
    )
    df.loc[:, "occurred_date"] = parts[0].str.strip()
    time_raw = (
        parts[1]
        .str.strip()
        .str.lower()
        .str.replace(r"\s*hrs$", "", regex=True)
        .str.replace(r"^\?$", "", regex=True)
        .str.strip()
    )
    df.loc[:, "occurred_time"] = (
        time_raw
        .str.zfill(4)
        .str.replace(r"^(\d{2})(\d{2})$", r"\1:\2", regex=True)
        .where(time_raw != "", other="")
    )
    return df.drop(columns=["datetime"])


def parse_citizen_demo(df):
    """Parse 'W/M 42' or 'B/M 17' into citizen race, sex, age."""
    parts = df.citizen_demo.str.extract(r"^([A-Za-z])/([A-Za-z])\s+(\d+)$")
    df.loc[:, "citizen_race"] = parts[0].str.strip()
    df.loc[:, "citizen_sex"] = parts[1].str.strip()
    df.loc[:, "citizen_age"] = parts[2].str.strip()
    return df.drop(columns=["citizen_demo"])


def clean_force_type(df):
    df.loc[:, "use_of_force_type"] = (
        df.use_of_force_type.str.lower()
        .str.strip()
        .str.replace(r"\s*/\s*", "/", regex=True)
    )
    return df


def clean_injured(df):
    df.loc[:, "use_of_force_result"] = (
        df.injured.str.lower()
        .str.strip()
        .str.replace(r"^no$", "", regex=True)
    )
    return df.drop(columns=["injured"])


def clean_medical(df):
    df.loc[:, "medical_treatment"] = (
        df.medical.str.lower()
        .str.strip()
        .str.replace(r"^no$", "", regex=True)
        .str.replace(r"^yes$", "yes", regex=True)
    )
    return df.drop(columns=["medical"])


def merge_officer_names(df, pprr):
    """Merge officer first/last names from PPRR using badge number."""
    df = df.merge(pprr, on="badge_no", how="left")
    return df


def clean():
    pprr = read_pprr_names()

    df = pd.read_csv(
        deba.data("raw/vermilion_so/vermilion_so_uof_2023_2025.csv"), header=0
    )
    df.columns = [
        "datetime",
        "incident_location",
        "rank_badge",
        "call_type",
        "citizen_demo",
        "use_of_force_type",
        "injured",
        "medical",
        "disciplinary_action",
    ]
    df = (
        df.pipe(extract_datetime)
        .pipe(clean_dates, ["occurred_date"])
        .pipe(extract_rank_and_badge)
        .pipe(parse_citizen_demo)
        .pipe(clean_force_type)
        .pipe(clean_injured)
        .pipe(clean_medical)
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(
            standardize_desc_cols,
            ["incident_location", "call_type", "disciplinary_action", "badge_no"],
        )
        .pipe(merge_officer_names, pprr)
        .pipe(set_values, {"agency": "vermilion-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "use_of_force_type",
                "occurred_year",
                "occurred_month",
                "occurred_day",
                "call_type",
            ],
            "uof_uid",
        )
    )

    citizen_df = (
        df[["uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"]]
        .copy()
        .pipe(
            gen_uid,
            ["citizen_age", "citizen_sex", "citizen_race", "agency"],
            "citizen_uid",
        )
    )

    uof_df = df[
        [
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "occurred_time",
            "incident_location",
            "rank_desc",
            "badge_no",
            "call_type",
            "use_of_force_type",
            "use_of_force_result",
            "medical_treatment",
            "disciplinary_action",
            "first_name",
            "last_name",
            "agency",
            "uid",
            "uof_uid",
        ]
    ].drop_duplicates(subset=["uid", "uof_uid"])

    return uof_df, citizen_df


if __name__ == "__main__":
    uof, citizen_uof = clean()
    uof.to_csv(deba.data("clean/uof_vermilion_so_2023_2025.csv"), index=False)
    citizen_uof.to_csv(
        deba.data("clean/uof_cit_vermilion_so_2023_2025.csv"), index=False
    )
