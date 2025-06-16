import pandas as pd
import deba
from lib.columns import clean_column_names, set_values 
from lib.clean import standardize_desc_cols, clean_dates, strip_leading_comma, clean_names 
from lib.uid import gen_uid
import re
from lib.rows import duplicate_row


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.violation.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"excess?\b", "excessive", regex=True)
        .str.replace(r"force jevenile", "force in juvenile", regex=False)
        .str.replace(r"malf in off", "malfeasance in office", regex=False)
        .str.replace(r"fail\b", "failure", regex=True)
        .str.replace(r"\/", "; ", regex=True)
        .str.replace(r"introd\b", "introduced", regex=True)
        .str.replace(r" allegation", "", regex=False)
    )
    return df.drop(columns="violation")[~((df.allegation == ""))]


def extract_action_from_disposition(df):
    df.loc[:, "action"] = (
        df.dispostion.str.lower()
        .str.strip()
        .str.replace(r"( ?unfounded ?| ?sustained ?| ?not sustained ?)", "", regex=True)
        .str.replace(r"^-", "", regex=True)
        .str.replace(r"-pending", "", regex=False)
        .str.replace(r"fired", "terminated", regex=False)
        .str.replace(r"-", "; ", regex=False)
        .str.replace(r"suspended (\w+) days?", r"\1-day suspension", regex=True)
        .str.replace(
            "tot lsp; arrested",
            r"arrested; turned over to the Louisiana State Police Department",
            regex=True,
        )
        .str.replace(r"^; ", "", regex=True)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.dispostion.str.lower()
        .str.strip()
        .str.replace(r"^(\w+)-(.+)", r"\1", regex=True)
        .str.replace(r"(tot|terminated)(.+)?", "", regex=True)
    )
    return df.drop(columns=["dispostion"])


def clean_complainant_name(df):
    df.loc[:, "complainant_type"] = (
        df.name_of_complainant.str.lower()
        .str.strip()
        .str.replace(r"(chief|warden|22).+", "internal", regex=True)
        .str.replace(r"(citzen\,|jefferson)", r"", regex=True)
        .str.replace(r"^d.+", "", regex=True)
        .str.replace(r"(\w+) (\w+)", "", regex=True)
    )
    return df.drop(columns="name_of_complainant")


def split_rows_with_multiple_officers(df):
    df = (
        df.drop("deputy", axis=1)
        .join(
            df["deputy"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("deputy"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_names(df):
    names = (
        df.deputy.str.lower()
        .str.strip()
        .str.replace(r"(\w+)  +(\w+)", r"\1 \2", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.extract(r"(dy|de?p|lt)?\.? ?(\w+) (\w+)")
    )

    df.loc[:, "rank_desc"] = (
        names[0]
        .fillna("")
        .str.replace(r"(de?p|dy)", "deputy", regex=True)
        .str.replace("lt", "lieutenant", regex=False)
    )
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df


def standardize_tracking_id(df):
    df.loc[:, "tracking_id"] = df.number.str.replace(r"-", "", regex=True)
    return df.drop(columns=["number"])


def clean_investigator(df):
    df.loc[:, "investigator_name"] = (
        df.investigator.str.lower()
        .str.strip()
        .str.replace(
            r"tot lsp",
            "turned over to the louisiana state police department",
            regex=False,
        )
        .str.replace(r"\, ", "/", regex=True)
    )
    return df.drop(columns=["investigator"])


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df



def clean():
    df = (
        pd.read_csv(deba.data("raw/washington_so/washington_so_cprr_2010_2022.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "receive_date"})
        .pipe(clean_dates, ["receive_date"])
        .pipe(clean_complainant_name)
        .pipe(standardize_tracking_id)
        .pipe(clean_allegation)
        .pipe(extract_action_from_disposition)
        .pipe(clean_disposition)
        .pipe(clean_investigator)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_names)
        .pipe(standardize_desc_cols, ["tracking_id"])
        .pipe(set_values, {"agency": "washington-so"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            ["uid", "disposition", "tracking_id", "action"],
            "allegation_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    citizen_df = df[["complainant_type", "allegation_uid", "agency"]]
    citizen_df = citizen_df.pipe(
        gen_uid, ["complainant_type", "allegation_uid", "agency"], "citizen_uid"
    )
    df = df.drop(columns=["complainant_type"])
    return df, citizen_df


def split_officer_name_24(df, col="officer_name"):
    # Remove any extra quotes and leading/trailing whitespace
    df[col] = df[col].str.strip("'").str.strip()
    
    # Split name by the first space only (handles middle names poorly but avoids errors)
    df[["first_name", "last_name"]] = df[col].str.extract(r"^(\w+)\s+(.*)$")
    
    # Drop the original name column
    df = df.drop(columns=[col])
    
    return df


def clean_allegation_24(df, col="allegation"):
    df = df.copy()

    def clean_entry(entry):
        if pd.isna(entry):
            return ""
        
        entry = str(entry).lower().strip()

        # Standardize separators
        entry = re.sub(r"/\s*;", ";", entry)
        entry = entry.replace(".", ";")
        entry = entry.replace(",", ";")

        # Fix common typos
        entry = entry.replace("perfomance", "performance")

        # Remove duplicate semicolons and spaces
        entry = re.sub(r"\s*;\s*", ";", entry)  # Normalize spacing
        entry = re.sub(r";+", ";", entry).strip(";")  # Remove trailing/leading semicolons

        return entry

    df[col] = df[col].apply(clean_entry)
    return df


def split_disposition_and_action(df):
    df = df.copy()
    
    # Move existing 'disposition' values into a new 'action' column
    df["action"] = df["disposition"].str.strip().str.lower()
    
    # Set 'disposition' to 'sustained' for all rows (if that’s correct contextually)
    df["disposition"] = "sustained"
    
    return df

def clean_complainant_name_24(df):
    df = df.copy()

    df["complainant_type"] = (
        df["complainant_type"]
        .str.lower()
        .str.strip()
        .str.replace(r"^(chief|captain|sheriff|lieutenant|sergeant)\b.*", "internal", regex=True)
    )

    df["complainant_type"] = df["complainant_type"].apply(
        lambda x: "internal" if x == "internal" else "external"
    )

    return df


def clean_24():
    df_24 = (
        pd.read_csv(deba.data("raw/washington_so/washington_so_cprr_2023_2025.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
        .rename(columns={
            "date": "receive_date",
            "offense": "allegation",
            "name_of_accused_deputy": "officer_name",
            "name_of_complainant": "complainant_type"
        })
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(clean_dates, ["receive_date"])
        .pipe(clean_names, ["officer_name"])
        .pipe(clean_complainant_name_24)
        .pipe(split_officer_name_24)
        .pipe(clean_allegation_24)
        .pipe(split_disposition_and_action)
        .pipe(set_values, {"agency": "washington-so"})
    )

    df_24 = df_24.copy()

    df_24 = (
        df_24
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "disposition", "action"], "allegation_uid")
    )

    citizen_df_24 = df_24[["complainant_type", "allegation_uid", "agency"]]
    citizen_df_24 = citizen_df_24.pipe(
        gen_uid, ["complainant_type", "allegation_uid", "agency"], "citizen_uid"
    )
    df_24 = df_24.drop(columns=["complainant_type"])
    return df_24, citizen_df_24



if __name__ == "__main__":
    df, citizen_df = clean()
    df_24, citizen_df_24 = clean_24()
    df.to_csv(deba.data("clean/cprr_washington_so_2010_2022.csv"), index=False)
    citizen_df.to_csv(
        deba.data("clean/cprr_cit_washington_so_2010_2022.csv"), index=False
    )
    citizen_df_24.to_csv(
        deba.data("clean/cprr_cit_washington_so_2023_2025.csv"), index=False
    )
    df_24.to_csv(deba.data("clean/cprr_washington_so_2023_2025.csv"), index=False)
