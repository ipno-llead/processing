import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_names, clean_races
from lib.uid import gen_uid


def split_incident_date(df):
    """
    Split incident_date column (format: YYYY-MM-DD) into
    incident_year, incident_month, and incident_day columns.
    Drops the original incident_date column.
    """
    df[["incident_year", "incident_month", "incident_day"]] = (
        df["incident_date"].str.split("-", expand=True)
    )
    df = df.drop(columns=["incident_date"])
    return df


def separate_multiple_citizens(df):
    """
    For incidents with multiple citizens, duplicate the row for each citizen
    and split the comma-separated citizen_race values.
    Each duplicated row will represent one citizen.
    """
    # Split citizen_race by comma and strip whitespace
    df["citizen_race_list"] = df["citizen_race"].str.split(",").apply(
        lambda x: [race.strip() for race in x] if isinstance(x, list) else [x]
    )

    # Explode the dataframe so each citizen gets their own row
    df = df.explode("citizen_race_list", ignore_index=True)

    # Replace the original citizen_race with the individual values
    df["citizen_race"] = df["citizen_race_list"]

    # Drop the temporary list column
    df = df.drop(columns=["citizen_race_list"])

    # Update number_of_citizens_involved to 1 since each row now represents one citizen
    df["number_of_citizens_involved"] = 1

    return df


def standardize_rank(raw_rank):
    """
    Standardize rank abbreviations to consistent values.
    """
    if not raw_rank:
        return ""

    raw_rank_upper = raw_rank.upper().replace(".", "").strip()

    # Map rank abbreviations to standardized values
    if raw_rank_upper in ["TPR", "TFC", "TROOPER FIRST CLASS", "TROOPER"]:
        return "trooper"
    elif raw_rank_upper in ["M/T", "MT"]:
        return "master trooper"
    elif raw_rank_upper in ["SGT", "SERGEANT"]:
        return "sergeant"
    elif raw_rank_upper in ["ST"]:
        return "senior trooper"
    else:
        return ""


def parse_officer_name(name):
    """
    Parse a single officer name into rank, first_name, middle_name, and last_name.
    Handles rank prefixes (TPR, TFC, M/T, Trooper, Sgt., etc.) and suffixes (Jr, II, III, Sr).
    """
    import re

    if pd.isna(name) or name.strip() == "":
        return {"rank": "", "first_name": "", "middle_name": "", "last_name": ""}

    name = name.strip()
    extracted_rank = ""

    # Extract and remove rank/title prefixes (case insensitive)
    rank_patterns = [
        (r"^Trooper First Class\s+", "Trooper First Class"),
        (r"^TPR\.?\s+", "TPR"),
        (r"^TFC\.?\s+", "TFC"),
        (r"^Tpr\.?\s+", "TPR"),
        (r"^M/T\s+", "M/T"),
        (r"^MT\s+", "M/T"),
        (r"^ST\s+", "ST"),
        (r"^Sgt\.?\s+", "SGT"),
        (r"^Sergeant\s+", "Sergeant"),
        (r"^Trooper\s+", "Trooper"),
    ]

    for pattern, rank_value in rank_patterns:
        match = re.search(pattern, name, flags=re.IGNORECASE)
        if match:
            extracted_rank = rank_value
            name = re.sub(pattern, "", name, flags=re.IGNORECASE).strip()
            break

    # Extract suffix (Jr, Jr., II, III, Sr, etc.)
    suffix_pattern = r",?\s+(Jr\.?|Sr\.?|II|III|IV)$"
    suffix_match = re.search(suffix_pattern, name, flags=re.IGNORECASE)
    suffix = ""
    if suffix_match:
        suffix = suffix_match.group(1).strip()
        name = name[:suffix_match.start()].strip()

    # Handle quoted nicknames (e.g., Russell "Gene" E Sibley)
    name = re.sub(r'"[^"]*"', "", name).strip()

    # Split name into parts
    name_parts = name.split()

    if len(name_parts) == 0:
        return {"rank": extracted_rank, "first_name": "", "middle_name": "", "last_name": ""}
    elif len(name_parts) == 1:
        # Only one name part
        return {"rank": extracted_rank, "first_name": name_parts[0], "middle_name": "", "last_name": ""}
    elif len(name_parts) == 2:
        # First and last name only
        first_name = name_parts[0]
        last_name = name_parts[1]
        if suffix:
            last_name = f"{last_name} {suffix}"
        return {"rank": extracted_rank, "first_name": first_name, "middle_name": "", "last_name": last_name}
    else:
        # First, middle(s), and last name
        first_name = name_parts[0]
        last_name = name_parts[-1]
        middle_name = " ".join(name_parts[1:-1])
        if suffix:
            last_name = f"{last_name} {suffix}"
        return {"rank": extracted_rank, "first_name": first_name, "middle_name": middle_name, "last_name": last_name}


def separate_multiple_officers(df):
    """
    For incidents with multiple officers, duplicate the row for each officer
    and split the comma-separated officer names and races.
    Assumes races are listed in the same order as names when counts match.
    Each duplicated row will represent one officer.
    """
    # Split officer names by comma
    df["officer_name_list"] = df["trooper_officer_name"].str.split(",").apply(
        lambda x: [name.strip() for name in x] if isinstance(x, list) else [x]
    )

    # Split officer races by comma
    df["officer_race_list"] = df["officer_race"].str.split(",").apply(
        lambda x: [race.strip() for race in x] if isinstance(x, list) else [x]
    )

    # Pad race list to match name list length if they don't match
    def pad_race_list(row):
        name_count = len(row["officer_name_list"])
        race_count = len(row["officer_race_list"])

        if name_count == race_count:
            return row["officer_race_list"]
        elif race_count == 1:
            # If there's only one race, replicate it for all officers
            return row["officer_race_list"] * name_count
        elif race_count < name_count:
            # Pad with empty strings if race list is shorter
            return row["officer_race_list"] + [""] * (name_count - race_count)
        else:
            # Truncate race list if it's longer than name list
            return row["officer_race_list"][:name_count]

    df["officer_race_list"] = df.apply(pad_race_list, axis=1)

    # Explode both lists together so each officer gets their own row with matching race
    df = df.explode(["officer_name_list", "officer_race_list"], ignore_index=True)

    # Parse each officer name into components
    parsed_names = df["officer_name_list"].apply(parse_officer_name)

    # Create new columns from parsed names
    df["rank"] = parsed_names.apply(lambda x: standardize_rank(x["rank"]))
    df["first_name"] = parsed_names.apply(lambda x: x["first_name"])
    df["middle_name"] = parsed_names.apply(lambda x: x["middle_name"])
    df["last_name"] = parsed_names.apply(lambda x: x["last_name"])

    # Replace officer_race with individual race value
    df["officer_race"] = df["officer_race_list"]

    # Drop temporary columns
    df = df.drop(columns=["officer_name_list", "officer_race_list", "trooper_officer_name"])

    # Update number_of_officers_involved to 1 since each row now represents one officer
    df["number_of_officers_involved"] = 1

    return df


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/louisiana_state_pd/lsp_uof_2022.csv")
        )
        .pipe(clean_column_names)
        .drop(columns=["subject_full_name", "justified_y_n"])
        .rename(columns={
                "event_start_date": "incident_date",
                "ren": "tracking_id_og",
                "troop": "department_desc",
                "type_of_force_used_by_officer": "use_of_force_description",
                "subject_count": "number_of_citizens_involved",
                "trooper_officer_count": "number_of_officers_involved",
                "trooper_officer_race": "officer_race",
                "subject_race": "citizen_race",
                "type_of_force_used_by_subject": "use_of_force_by_citizen",
                "of_uses_of_force": "number_of_uses_of_force",
                 }
                )
        .pipe(split_incident_date)
        .pipe(standardize_desc_cols, ["department_desc", "use_of_force_description", "use_of_force_by_citizen"])
        .pipe(separate_multiple_citizens)
        .pipe(separate_multiple_officers)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_races, ["officer_race", "citizen_race"])
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["tracking_id", "uid", "use_of_force_description", "agency"], "uof_uid")
        )
    return df


def extract_citizen(uof):
    """
    Extract citizen-related columns into a separate dataframe.
    Creates a citizen_uid and removes citizen columns from the UOF dataframe.
    Returns both the citizen dataframe and the modified UOF dataframe.
    """
    citizen_df = uof.loc[
        :,
        [
            "citizen_race",
            "use_of_force_by_citizen",
            "agency",
            "uof_uid",
        ],
    ].pipe(
        gen_uid,
        [
            "citizen_race",
            "use_of_force_by_citizen",
            "agency",
        ],
        "citizen_uid",
    )

    uof = uof.drop(
        columns=[
            "citizen_race",
            "use_of_force_by_citizen",
        ]
    )

    return citizen_df, uof

def clean_24():
    df = (
        pd.read_csv(
            deba.data("raw/louisiana_state_pd/lsp_uof_23_24.csv")
        )
        .pipe(clean_column_names)
        .drop(columns=["subject_full_name", "justified_y_n"])
        .rename(columns={
                "event_start_date": "incident_date",
                "ren": "tracking_id_og",
                "troop": "department_desc",
                "type_of_force_used_by_officer": "use_of_force_description",
                "subject_count": "number_of_citizens_involved",
                "trooper_officer_count": "number_of_officers_involved",
                "trooper_officer_race": "officer_race",
                "subject_race": "citizen_race",
                "type_of_force_used_by_subject": "use_of_force_by_citizen",
                "of_uses_of_force": "number_of_uses_of_force",
                 }
                )
        .pipe(split_incident_date)
        .pipe(standardize_desc_cols, ["department_desc", "use_of_force_description", "use_of_force_by_citizen"])
        .pipe(separate_multiple_citizens)
        .pipe(separate_multiple_officers)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_races, ["officer_race", "citizen_race"])
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["tracking_id", "uid", "use_of_force_description", "agency"], "uof_uid")
        )
    return df


if __name__ == "__main__":
    uof_22 = clean()
    uof_24 = clean_24()
    uof = pd.concat([uof_22, uof_24])
    uof_citizen, uof = extract_citizen(uof)
    uof = uof.drop_duplicates(subset=["uid", "uof_uid"])
    uof_citizen = uof_citizen.drop_duplicates(subset=["citizen_uid", "uof_uid"])
    uof.to_csv(deba.data("clean/uof_louisiana_state_pd_2022_2024.csv"), index=False)
    uof_citizen.to_csv(
        deba.data("clean/uof_cit_louisiana_state_pd_2022_2024.csv"), index=False
    )
