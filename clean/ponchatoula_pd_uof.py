import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, clean_names, clean_races, clean_sexes, standardize_desc_cols
from lib.uid import gen_uid


def split_officer_names(df):
    """Parse first_name and last_name from officer_identifier (e.g. 'SGT. LECOUR HALL')"""
    names = (
        df.officer_identifier
        .fillna("")
        .str.strip()
        .str.replace(r"^(SGT|CPL|OFC|LT|DET|CAPT|CHF)\.?\s*", "", regex=True)
        .str.strip()
    )
    parts = names.str.split(r"\s+", n=1, expand=True)
    df.loc[:, "first_name"] = parts[0] if 0 in parts.columns else ""
    df.loc[:, "last_name"] = parts[1] if 1 in parts.columns else ""
    return df


def split_subject_names(df):
    """Parse citizen first/last name from subject_identifier (e.g. 'PARIS BRUMFIELD')"""
    names = (
        df.subject_identifier
        .fillna("")
        .str.strip()
    )
    parts = names.str.split(r"\s+", n=1, expand=True)
    df.loc[:, "citizen_first_name"] = parts[0] if 0 in parts.columns else ""
    df.loc[:, "citizen_last_name"] = parts[1] if 1 in parts.columns else ""
    return df


def clean_arrested(df):
    df.loc[:, "citizen_arrested"] = (
        df.citizen_arrested
        .str.lower()
        .str.strip()
        .str.replace(r"^yes$", "yes", regex=True)
        .str.replace(r"^no$", "no", regex=True)
    )
    return df


def clean_citizen_injured(df):
    df.loc[:, "citizen_injured"] = (
        df.citizen_injured
        .str.lower()
        .str.strip()
        .str.replace(r"^yes$", "yes", regex=True)
        .str.replace(r"^no$", "no", regex=True)
    )
    return df


def clean_officer_injured(df):
    df.loc[:, "officer_injured"] = (
        df.officer_injured
        .str.lower()
        .str.strip()
        .str.replace(r"^yes$", "yes", regex=True)
        .str.replace(r"^no$", "no", regex=True)
    )
    return df


def clean_use_of_force_description(df):
    if "use_of_force_description" in df.columns:
        df.loc[:, "use_of_force_description"] = (
            df.use_of_force_description
            .fillna("")
            .astype(str)
            .str.lower()
            .str.strip()
            .str.replace(r"less\s*-?\s*lethal\s+", "", regex=True)
        )
    return df


def clean_use_of_force_reason(df):
    if "use_of_force_reason" in df.columns:
        df.loc[:, "use_of_force_reason"] = (
            df.use_of_force_reason
            .fillna("")
            .astype(str)
            .str.lower()
            .str.strip()
        )
    return df


def create_tracking_id(df):
    df.loc[:, "tracking_id_og"] = (
        df.incident_id.fillna("").astype(str) + "-" +
        df.occurred_year.fillna("").astype(str) + "-" +
        df.occurred_month.fillna("").astype(str) + "-" +
        df.occurred_day.fillna("").astype(str)
    )
    return df


def clean25():
    df = (
        pd.read_csv(deba.data("raw/ponchatoula_pd/Ponchatoula_PD_Use_of_Force_2025.csv"), low_memory=False)
        .pipe(clean_column_names)
        .rename(columns={
            "incident_date": "occurred_date",
            "type_of_force_used": "use_of_force_description",
            "call_type": "use_of_force_reason",
            "subject_injured": "citizen_injured",
            "arrested": "citizen_arrested",
            "officer_gender": "officer_sex",
            # citizen_gender and citizen_race are swapped in the raw CSV
            "citizen_gender": "citizen_race",
            "citizen_race": "citizen_sex",
            "rank": "rank_desc",
            "service_years": "years_of_service",
        })
        .drop(columns=[
            "incident_time", "incident_location", "case_number", "case_status",
            "force_escalation_level", "medical_assistance_provided", "ems_transport",
            "incident_narrative_summary", "officer_age", "on_duty",
            "citizen_age", "charges_filed", "admin_review_conducted",
            "policy_violation_found", "disciplinary_action_taken",
            "discipline_type", "discipline_date", "final_disposition",
            "document_type", "document_date", "available_for_inspection",
            "redacted", "exemption_applied",
        ], errors="ignore")
        .drop_duplicates()
        .pipe(split_officer_names)
        .pipe(split_subject_names)
        .pipe(clean_arrested)
        .pipe(clean_citizen_injured)
        .pipe(clean_officer_injured)
        .pipe(clean_use_of_force_description)
        .pipe(clean_use_of_force_reason)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_names, ["citizen_first_name", "citizen_last_name"])
        .pipe(clean_sexes, ["citizen_sex", "officer_sex"])
        .pipe(clean_races, ["citizen_race", "officer_race"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_dates, ["occurred_date"])
        .pipe(create_tracking_id)
        .pipe(set_values, {"agency": "ponchatoula-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["tracking_id_og", "agency"],
            "tracking_id"
        )
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "use_of_force_reason",
                "use_of_force_description",
                "citizen_race",
                "citizen_sex",
            ],
            "uof_uid",
        )
    )

    citizen_df = df[
        [
            "uof_uid",
            "citizen_sex",
            "citizen_race",
            "citizen_arrested",
            "citizen_injured",
            "citizen_first_name",
            "citizen_last_name",
            "agency",
        ]
    ].pipe(
        gen_uid,
        ["uof_uid", "citizen_sex", "citizen_race", "agency"],
        "citizen_uid",
    ).drop_duplicates()

    uof_df = df[
        [
            "incident_id",
            "tracking_id",
            "tracking_id_og",
            "use_of_force_reason",
            "use_of_force_description",
            "officer_injured",
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "first_name",
            "last_name",
            "rank_desc",
            "years_of_service",
            "officer_sex",
            "officer_race",
            "agency",
            "uid",
            "uof_uid",
        ]
    ].drop_duplicates()

    return uof_df, citizen_df


if __name__ == "__main__":
    uof_2025, citizen_uof_2025 = clean25()
    uof_2025.to_csv(deba.data("clean/uof_ponchatoula_pd_2025.csv"), index=False)
    citizen_uof_2025.to_csv(deba.data("clean/uof_cit_ponchatoula_pd_2025.csv"), index=False)
