from lib.columns import clean_column_names
import deba
from lib.clean import (
    clean_names,
    clean_dates,
    names_to_title_case,
    standardize_desc_cols,
)
from lib.uid import gen_uid
import pandas as pd


def standardize_agency(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
        .fillna("")
        .str.replace(r"(\w)\.\s*(\w)\.", r"\1\2", regex=True)
        .str.replace(r"E\. Baton Rouge So", "Baton Rouge SO", regex=True)
        .str.replace(r"E\. Jefferson Levee PD", "East Jefferson Levee PD", regex=True)
        .str.replace(r"^St ", "St. ", regex=True)
        .str.replace(r" ?Parish ?", " ", regex=True)
        .str.replace(r"Pd$", "PD", regex=True)
        .str.replace(r"So$", "SO", regex=True)
        .str.replace(r"Dept\.?", "Department", regex=True)
        .str.replace(r"Univ\. Pd - (.+)", r"\1 University PD", regex=True)
        .str.replace(r"^Lsu\b", "LSU", regex=True)
        .str.replace(r"^Lsuhsc", "LSUHSC", regex=True)
        .str.replace(r"^La\b", "Louisiana", regex=True)
        .str.replace(r"^Orleans DA Office$", "New Orleans DA", regex=True)
        .str.replace(r"DA Office$", "DA", regex=True)
        .str.replace(r"^W\.?\b", "West", regex=True)
        .str.replace(r"\-(\w+)", r"- \1", regex=True)
        .str.replace(
            r"^Se La Flood Protection Auth- E$",
            "Southeast Louisiana Flood Protection Authority",
            regex=True,
        )
        .str.replace(r"Dev\.\,", "Development", regex=True)
        .str.replace("Red River Par", "Red River", regex=False)
        .str.replace("Cc", "Community College", regex=False)
        .str.replace("Constable'S", "Constable's", regex=False)
        .str.replace(r"^Orleans", "New Orleans", regex=True)
        .str.replace(r"Rep\.", "Representatives", regex=True)
        .str.replace(r"Park & Rec\.", "Parks and Recreation", regex=True)
        .str.replace(
            "Housing Authority Of NO", "New Orleans Housing Authority", regex=False
        )
        .str.replace(r"^Ebr", "East Baton Rouge", regex=False)
        .str.replace("City Park PD - NO", "New Orleans City Park PD", regex=False)
        .str.replace("Nd", "nd", regex=False)
        .str.replace(r"^(\w+)St", r"\1st", regex=True)
        .str.replace(r"^(\w+)Th", r"\1th", regex=True)
        .str.replace("Jdc", "Judicial District Court")
        .str.replace("Police", "PD", regex=False)
        .str.replace(r"  +", " ", regex=True)
        .str.replace(r" $", "", regex=True)
        .str.replace("Plaquemines Par ", "Plaquemines ", regex=False)
        .str.replace(r"\bDistrict Attorney\b", "DA", regex=False)
        .str.replace(r"^Uno", "UNO", regex=True)
        .str.replace(
            r"^Medical Center Of La - No$",
            "Medical Center Of Louisiana - New Orleans PD",
            regex=True,
        )
        .str.replace(
            r"^LSUHSC - No University PD$", "LSUHSC - New Orleans University PD"
        )
        .str.replace(
            r"^New Orleans Housing Authority$",
            "Housing Authority of New Orleans",
            regex=True,
        )
        .str.replace(r"West\. ", "West ", regex=True)
        .str.replace(" Par ", " ", regex=False)
        .str.replace("1St", "First", regex=False)
        .str.replace(r"Coroner\'S", "Coroners", regex=True)
        .str.replace(r"^Alcohol & Tobacco", "Alcohol Tobacco", regex=True)
    )
    return df


def replace_impossible_dates(df):
    df.loc[:, "level_1_cert_date"] = df.level_1_cert_date.str.replace(
        "3201", "2013", regex=False
    )
    return df


def clean():
    df = pd.read_csv(deba.data("raw/post_council/post_pprr_11-6-2020.csv"))
    df = clean_column_names(df)
    df.columns = [
        "agency",
        "last_name",
        "first_name",
        "employment_status",
        "hire_date",
        "level_1_cert_date",
        "last_pc_12_qualification_date",
    ]
    df.loc[:, "data_production_year"] = "2020"
    df = df.dropna(0, "all", subset=["first_name"])
    df = (
        df.pipe(names_to_title_case, ["agency"])
        .pipe(standardize_agency)
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(replace_impossible_dates)
        .pipe(
            clean_dates,
            ["level_1_cert_date", "last_pc_12_qualification_date"],
            expand=False,
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            gen_uid,
            [
                "agency",
                "last_name",
                "first_name",
                "hire_year",
                "hire_month",
                "hire_day",
            ],
        )
        .drop_duplicates(
            subset=["hire_year", "hire_month", "hire_day", "uid"], keep="first"
        )
    )
    return df


if __name__ == "__main__":
    df = clean()

    df.to_csv(deba.data("clean/pprr_post_2020_11_06.csv"), index=False)
