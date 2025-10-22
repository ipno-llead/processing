import pandas as pd
import deba
from lib.columns import clean_column_names
from lib.clean import clean_dates, standardize_desc_cols, clean_names
from lib.uid import gen_uid
from datetime import datetime


def split_officer_name(df):
    """Split officer_name into first_name and last_name"""
    df.loc[:, "officer_name"] = df.officer_name.str.strip()

    # Split on space - assume "FirstName LastName" format
    names = df.officer_name.str.split(n=1, expand=True)
    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "last_name"] = names[1].fillna("")

    return df.drop(columns=["officer_name"])


def clean_rank(df):
    """Standardize rank values"""
    mapping = {
        "officer": "officer",
        "ofc": "officer",
        "ofc.": "officer",
        "off": "officer",
        "sergeant": "sergeant",
        "sgt": "sergeant",
        "sgt.": "sergeant",
        "lieutenant": "lieutenant",
        "lt": "lieutenant",
        "lt.": "lieutenant",
        "detective": "detective",
        "det": "detective",
        "det.": "detective",
        "corporal": "corporal",
        "cpl": "corporal",
        "cpl.": "corporal",
        "captain": "captain",
        "capt": "captain",
        "capt.": "captain",
        "chief": "chief",
    }

    df.loc[:, "rank"] = (
        df["rank"].str.lower()
        .str.strip()
        .str.replace(r"\.", "", regex=True)
        .map(mapping)
        .fillna(df["rank"].str.lower().str.strip())
    )

    return df


def calculate_citizen_age(df):
    """Calculate citizen age from DOB and incident date"""
    def calc_age(row):
        try:
            if pd.isna(row["suspect_dob"]) or pd.isna(row["incident_date"]):
                return ""

            # Parse DOB - try different formats
            dob_str = str(row["suspect_dob"]).strip()

            # Try different date formats
            for fmt in ["%m-%d-%Y", "%m.%d.%Y", "%m/%d/%Y", "%Y-%m-%d"]:
                try:
                    dob = datetime.strptime(dob_str, fmt)
                    break
                except ValueError:
                    continue
            else:
                return ""

            # Parse incident date
            incident_date = pd.to_datetime(row["incident_date"])

            # Calculate age
            age = incident_date.year - dob.year
            if (incident_date.month, incident_date.day) < (dob.month, dob.day):
                age -= 1

            return str(age) if age >= 0 else ""
        except:
            return ""

    df.loc[:, "citizen_age"] = df.apply(calc_age, axis=1)
    return df.drop(columns=["suspect_dob"])


def create_force_description(df):
    """Create a description of force types used"""
    def build_desc(row):
        force_types = []
        if row.get("force_type_physical") == "Yes":
            force_types.append("physical force")
        if row.get("force_type_chemical") == "Yes":
            force_types.append("chemical agent")
        if row.get("force_type_electronic") == "Yes":
            force_types.append("electronic control device")
        if row.get("force_type_impact_weapon") == "Yes":
            force_types.append("impact weapon")
        if row.get("force_type_impact_munitions") == "Yes":
            force_types.append("impact munitions")
        if row.get("force_type_firearm") == "Yes":
            force_types.append("firearm")

        return "/".join(force_types) if force_types else ""

    df.loc[:, "force_type"] = df.apply(build_desc, axis=1)

    return df.drop(columns=[
        "force_type_physical", "force_type_chemical", "force_type_electronic",
        "force_type_impact_weapon", "force_type_impact_munitions", "force_type_firearm"
    ])


def create_officers_notes(df):
    """Combine relevant text fields into officers_notes"""
    def build_notes(row):
        parts = []

        if pd.notna(row.get("nature_of_incident")) and str(row["nature_of_incident"]).strip():
            parts.append(f"nature: {str(row['nature_of_incident']).lower().strip()}")

        if pd.notna(row.get("offense")) and str(row["offense"]).strip():
            parts.append(f"offense: {str(row['offense']).lower().strip()}")

        if pd.notna(row.get("force_type")) and str(row["force_type"]).strip():
            parts.append(f"force type: {str(row['force_type']).lower().strip()}")

        if pd.notna(row.get("injury_description")) and str(row["injury_description"]).strip().lower() not in ["n/a", "na", ""]:
            parts.append(f"injury: {str(row['injury_description']).lower().strip()}")

        return "; ".join(parts) if parts else ""

    df.loc[:, "officers_notes"] = df.apply(build_notes, axis=1)

    return df.drop(columns=[
        "nature_of_incident", "offense", "force_type", "injury_description",
        "officer_injured", "suspect_injured", "others_injured",
        "taser_serial", "taser_model", "taser_manufacturer", "taser_lot",
        "taser_caliber", "incident_number", "item_number", "time"
    ])


def assign_agency(df):
    """Assign agency name"""
    df.loc[:, "agency"] = "grand-isle-pd"
    return df


def add_citizen_columns(df):
    """Add empty citizen race and sex columns (not in source data)"""
    df.loc[:, "citizen_race"] = ""
    df.loc[:, "citizen_sex"] = ""
    return df


def clean():
    """Main cleaning function for Grand Isle PD UOF data"""
    df = (
        pd.read_csv(deba.data("raw/grand_isle/grand_isle_pd_uof_2024.csv"))
        .pipe(clean_column_names)
        .rename(columns={
            "date": "incident_date",
            "officer_rank": "rank",
            "suspect_last_name": "last_name",
            "suspect_first_name": "first_name"
        })
        .pipe(clean_dates, ["incident_date"])
        .pipe(calculate_citizen_age)
        .pipe(create_force_description)
        .pipe(create_officers_notes)
        .pipe(standardize_desc_cols, ["location", "officers_notes"])
        # Now handle officer names (overwriting citizen names)
        .rename(columns={"first_name": "citizen_first_name", "last_name": "citizen_last_name"})
        .pipe(split_officer_name)
        .pipe(clean_rank)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(assign_agency)
        .pipe(add_citizen_columns)
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
        .pipe(
            gen_uid,
            ["citizen_race", "citizen_sex", "citizen_age"],
            "citizen_uid",
        )
        .drop(columns=["citizen_first_name", "citizen_last_name"])
        .drop_duplicates(subset=["uid", "uof_uid"])
    )

    return df


def split_citizen_and_uof(
    df,
    uid_col="uof_uid",
    citizen_uid_col="citizen_uid",
    citizen_cols=("citizen_race", "citizen_sex", "citizen_age")
):
    """
    Split UOF dataframe into officer/UOF file and citizen file.
    """
    # Keep original unchanged
    uof_df = df.copy()

    existing_citizen_cols = [c for c in citizen_cols if c in df.columns]
    cols_for_citizen = [uid_col] + existing_citizen_cols
    if citizen_uid_col in df.columns:
        cols_for_citizen.insert(1, citizen_uid_col)

    # Build citizen df
    citizen_df = (
        df.loc[:, [c for c in cols_for_citizen if c in df.columns]]
        .assign(
            # Normalize blanks to NaN for easy filtering
            **{
                c: df[c].replace(["", " ", "na", "n/a", "NA", "N/A"], pd.NA)
                for c in existing_citizen_cols
            },
            agency="grand-isle-pd"
        )
    )

    # Filter out rows where all citizen columns are missing
    if existing_citizen_cols:
        mask_all_missing = citizen_df[existing_citizen_cols].isna().all(axis=1)
        citizen_df = citizen_df.loc[~mask_all_missing].copy()

    # Deduplicate on uof_uid
    citizen_df = citizen_df.drop_duplicates(subset=[uid_col])

    return uof_df, citizen_df


if __name__ == "__main__":
    uof_df = clean()
    uof_df, citizen_df = split_citizen_and_uof(uof_df)
    uof_df.to_csv(deba.data("clean/uof_grand_isle_pd_2024.csv"), index=False)
    citizen_df.to_csv(deba.data("clean/uof_cit_grand_isle_pd_2024.csv"), index=False)
