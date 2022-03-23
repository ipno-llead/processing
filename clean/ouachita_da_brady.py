import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, names_to_title_case, clean_dates
from lib.uid import gen_uid


def extract_disposition(df):
    dispositions = df.charges.str.extract(r"(pled guilty)")
    df.loc[:, "disposition"] = dispositions[0].fillna("")
    return df


def extract_charging_agency(df):
    agency = df.charges.str.extract(r"(federal department of justice)")
    df.loc[:, "charging_agency"] = agency[0]
    return df


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.charges.str.replace("pled guilty to ", "", regex=False)
        .str.replace("indicted by federal department of justice for ", "", regex=False)
        .str.replace(r"\bbatter\b", "battery", regex=True)
        .str.replace(
            "warrant issued for 1 count of simple battery", "1 count of simple battery"
        )
        .str.replace(
            "prosecution pending in union parish for 1 count of simple battery",
            "1 count of simple battery; excessive use of force",
            regex=False,
        )
        .str.replace(r"(\w+)\)? and (\d+)", r"\1; \2", regex=True)
    )
    return df.drop(columns="charges")


def extract_agency_and_department_desc(df):
    agency = df.action.str.extract(r"(lsp)")
    df.loc[:, "agency"] = agency[0].str.replace(
        "lsp", "Louisiana State PD", regex=False
    )

    departments = df.action.str.extract(r"(troof f)")
    df.loc[:, "department_desc"] = departments[0].str.replace(
        "troof", "troop", regex=False
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = df.action.str.replace(
        " from lsp troof f for excessive use of force", "", regex=False
    )
    return df


def assign_agency(df):
    df.loc[(df.last_name == "brown"), "agency"] = "Louisiana State PD"
    df.loc[(df.last_name == "smith"), "agency"] = "Ouachita SO"
    df.loc[(df.last_name == "desadier"), "agency"] = "Monroe PD"
    df.loc[(df.last_name == "dickerson"), "agency"] = "Louisiana State PD"
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/ouachita_da/ouachita_da_cprr_2021_by_hand.csv"))
        .pipe(clean_column_names)
        .rename(columns={"investigation_end_date": "investigation_complete_date"})
        .pipe(clean_dates, ["investigation_complete_date"])
        .pipe(extract_disposition)
        .pipe(extract_charging_agency)
        .pipe(clean_allegations)
        .pipe(extract_agency_and_department_desc)
        .pipe(clean_action)
        .pipe(assign_agency)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(names_to_title_case, ["charging_agency"])
        .pipe(set_values, {"source_agency": "Ouachita DA"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "source_agency"], "brady_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/brady_ouachita_da_2021.csv"), index=False)
