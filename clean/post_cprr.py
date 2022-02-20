import pandas as pd
import bolo
from lib.columns import clean_column_names
from lib.uid import gen_uid


def split_name(df):
    col_name = [col for col in df.columns if col.endswith("name")][0]
    names = (
        df[col_name]
        .str.strip()
        .str.lower()
        .str.replace("van tran", "vantran", regex=False)
        .str.replace("de' clouet", "de'clouet", regex=False)
        .str.replace(r"(\w+) \b(\w{2})$", r"\2 \1", regex=True)
        .str.replace(r"\"", "", regex=True)
        .str.extract(r"^(\w+) ?(\w{3,})? ?(jr|sr)? (\w+-?\'?\w+)$")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "middle_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")
    df.loc[:, "last_name"] = names[3]
    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["name", "suffix"])


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.reason.str.replace(r"^(\w+)-(\w+)", r"\1 \2", regex=True)
        .str.replace(r"\/", "|", regex=True)
        .str.replace("-", ": ", regex=False)
    )
    return df.drop(columns="reason")


def assign_action(df):
    df.loc[:, "action"] = "decertified"
    return df


def rename_agency(df):
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
        .str.replace("Jdc", "Judicial District Court", regex=False)
        .str.replace("Police", "PD", regex=False)
        .str.replace(r"  +", " ", regex=True)
        .str.replace(r" $", "", regex=True)
        .str.replace("Plaquemines Par ", "Plaquemines ", regex=False)
        .str.replace("District Attorney", "DA", regex=False)
        .str.replace(r"^LSP$", "Louisiana State PD", regex=True)
    )
    return df


def clean():
    df = (
        pd.read_csv(bolo.data("raw/post_council/post_decertifications_2016_2019.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "decertification_date"})
        .pipe(split_name)
        .pipe(clean_allegations)
        .pipe(assign_action)
        .pipe(rename_agency)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "decertification_date"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(bolo.data("clean/cprr_post_2016_2019.csv"), index=False)
