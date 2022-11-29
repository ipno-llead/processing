import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_sexes,
    standardize_desc_cols,
    clean_dates,
    clean_races,
    float_to_int_str,
    clean_names,
)
import deba
from lib.uid import gen_uid


def split_names(df):
    names = (
        df.officer.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(
            r"((.+)?nopd(.+)?|(.+)?unknown(.+)?|anonymous|none)", "", regex=True
        )
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\'$", "", regex=True)
        .str.replace(r"( +$|^ +)", "", regex=True)
        .str.extract(r"^(?:(\w+\'?-?\w+?) ) ?(\w+-?\'? ?\w+?) ?(jr|sr)?$")
    )

    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "last_name"] = names[1]
    df.loc[:, "suffix"] = names[2]

    df.loc[:, "last_name"] = df.last_name.fillna("").str.cat(
        df.suffix.fillna(""), sep=" "
    )
    df.loc[:, "last_name"] = df.last_name.str.replace(r" +$", "", regex=True)
    return df.drop(columns=["suffix", "officer"])[~((df.last_name.fillna("") == ""))]


def clean_complainant_type(df):
    df.loc[:, "complainant_type"] = (
        df.incident_type.str.lower()
        .str.strip()
        .str.replace(" initiated", "", regex=False)
    )
    return df.drop(columns=["incident_type"])


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.rule.str.lower()
        .str.strip()
        .str.cat(df.paragraph.str.lower().str.strip(), sep="; ")
    )

    df.loc[:, "allegation"] = (
        df.allegation.str.replace(r"\bprof\b", "professionalism", regex=True)
        .str.replace(r"\bperf\b", "performance", regex=True)
        .str.replace(r"\binfo\b", "information", regex=True)
        .str.replace("dept", "department", regex=False)
        .str.replace(r"networking websites(.+)", "networking, websites", regex=True)
        .str.replace(r"no allegation(.+); (.+)", r"\2", regex=True)
        .str.replace(r"alcohol\/drugs", "alcohol and/or drugs", regex=True)
    )
    return df.drop(columns=["rule", "paragraph"])


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.allegation_finding.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("nfim", "no further investigation merited", regex=False)
        .str.replace(r" case$", "", regex=True)
        .str.replace(r"\binves\b", "investigation", regex=True)
        .str.replace(r"rui - retire", "retired under investigation", regex=True)
        .str.replace(r"(\w+) ?- (\w+)", r"\1; \2", regex=True)
        .str.replace(
            r"sustained; rui - resign",
            "sustained; resigned while under investigation",
            regex=True,
        )
        .str.replace(
            r"^rui ?- ?resign(ed under investigation)?$",
            "resigned while under investigation",
            regex=True,
        )
        .str.replace(r"^susatined$", "sustained", regex=True)
    )
    return df.drop(columns=["allegation_finding"])


def clean_unit_sub_desc(df):
    df.loc[:, "unit_sub_desc"] = (
        df.unit_additional_details_of_complainant.str.lower()
        .str.strip()
        .str.replace(r"v\.o\.w\.s\. unit", "violent offender warrant squad", regex=True)
        .str.replace(r"(\w+) - ?(\w+)", r"\1 \2", regex=True)
        .str.replace(r"firearm$", "firearms", regex=True)
        .str.replace(r"p\.i\.b\.", "public integrity bureau", regex=True)
        .str.replace(r"^b$", "squad b", regex=True)
        .str.replace(r" assignment$", "", regex=True)
        .str.replace(r" (unit$|officer)", "", regex=True)
    )
    return df


def clean_unit_desc(df):
    df.loc[:, "unit_desc"] = (
        df.unit_of_complainant.str.lower()
        .str.strip()
        .str.replace(r"(\w+) -(\w+)", r"\1 \2", regex=True)
        .str.replace(r"(\w+)  (\w+)", r"\1 \2", regex=True)
        .str.replace("&", "and", regex=False)
        .str.replace(r" (division|section)$", "", regex=True)
        .str.replace(r"^admin$", "administration", regex=True)
        .str.replace(r"^5th$", "5th district", regex=True)
    )
    return df


def clean_division_desc(df):
    df.loc[:, "division_desc"] = (
        df.division_of_complainant.str.lower()
        .str.strip()
        .str.replace(
            r"^pib ?(- public integrity bureau)?", "public integrity bureau", regex=True
        )
        .str.replace("&", "and", regex=False)
        .str.replace("police ", "", regex=False)
        .str.replace(r"divisi$", "division", regex=True)
        .str.replace(r"(\w+) \/ (\w+)", r"\1/\2", regex=True)
        .str.replace(r"^unknown$", "unknown district/division", regex=True)
        .str.replace(r"^isb", "investigative services bureau", regex=True)
        .str.replace(r" (division$|team$|officer$|district\/division$)", "", regex=True)
    )
    return df


def clean_department_desc(df):
    df.loc[:, "department_desc"] = (
        df.bureau_of_complainant.str.lower()
        .str.strip()
        .str.replace(r"^(\w{3}) - ", "", regex=True)
        .str.replace(r"\bservice\b", "services", regex=True)
        .str.replace(r"^fob$", "field operations bureau", regex=True)
        .str.replace("superintendant", "superintendent", regex=False)
        .str.replace(r"^unknown$", "unknown bureau", regex=True)
        .str.replace(r"^nopd officer$", "", regex=True)
    )
    return df


def clean_employment_status(df):
    df.loc[:, "employment_status"] = (
        df.employment_status.str.lower()
        .str.strip()
        .str.replace(r"off duty", "off-duty", regex=True)
        .str.replace(r"^not ?- ?nopd officer$", "not an nopd officer", regex=True)
        .str.replace(r"(\w+)  (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^rui - resigned under investigation$",
            "resigned under investigation",
            regex=True,
        )
    )
    return df


def clean_work_shift(df):
    df.loc[:, "work_shift"] = (
        df.shift_of_complainant.str.lower()
        .str.strip()
        .str.replace(r"(.+) - (.+)", r"\1-\2", regex=True)
        .str.replace(
            r"^\b(?:(\w{1,2}):?(\w{1,2} ?[pmam]) ?)", r"between \1:\2", regex=True
        )
    )
    return df.drop(columns=["shift_of_complainant"])


def clean_allegation_sub_desc(df):
    df.loc[:, "allegation_sub_desc"] = (
        df.allegation_sub_desc.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"(.+)\((\w+)\)", r"\1 (\2)", regex=True)
        .str.replace(r"(\w+)  +\((\w+)\)", r"\1 (\2)", regex=True)
        .str.replace(r"\((\w+)\)  +\((\w+)\)", r"(\1) (\2)", regex=True)
        .str.replace(r"(\w+)  +(\w+)", r"\1 \2", regex=True)
        .str.replace(r"&", "and", regex=False)
    )
    return df


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_da_cprr_giglio_2021.csv"))
        .pipe(clean_column_names)
        .drop(columns=["complaint_disposition", "complaint_classification"])
        .rename(
            columns={
                "complaint_tracking_number": "tracking_id",
                "linked_item_number": "item_number",
                "date_complaint_occurred": "occur_date",
                "date_complaint_received_by_nopd_pib": "receive_date",
                "date_complaint_investigation_complete": "investigation_complete_date",
                "working_status_of_complainant": "employment_status",
                "relevant_policy": "allegation_sub_desc",
                "officer_firstname": "first_name",
                "officer_lastname": "last_name",
                "officer_race_ethnicity": "race",
                "officer_years_of_service": "years_of_service",
                "complainant_gender": "citizen_sex",
                "complainant_age": "citizen_age",
                "officer_age": "age",
                "officer_gender": "sex",
                "complainant_ethnicity": "citizen_race",
            }
        )
        .pipe(split_names)
        .pipe(clean_work_shift)
        .pipe(clean_complainant_type)
        .pipe(clean_allegation)
        .pipe(clean_disposition)
        .pipe(clean_unit_sub_desc)
        .pipe(clean_unit_desc)
        .pipe(clean_division_desc)
        .pipe(clean_department_desc)
        .pipe(clean_unit_desc)
        .pipe(clean_employment_status)
        .pipe(clean_allegation_sub_desc)
        .pipe(float_to_int_str, ["age", "years_of_service", "citizen_ae"])
        .pipe(
            standardize_desc_cols,
            [
                "complainant_type",
                "tracking_id",
                "item_number",
                "investigation_status",
                "work_shift",
            ],
        )
        .pipe(clean_races, ["race", "citizen_race"])
        .pipe(clean_sexes, ["sex", "citizen_sex"])
        .pipe(
            clean_dates, ["occur_date", "receive_date", "investigation_complete_date"]
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["allegation", "allegation_sub_desc", "disposition", "tracking_id", "uid"],
            "allegation_uid",
        )
        .drop(
            columns=[
                "bureau_of_complainant",
                "division_of_complainant",
                "unit_of_complainant",
                "unit_additional_details_of_complainant",
            ]
        )
        .drop_duplicates(subset=["allegation_uid"])
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_new_orleans_da_2016_2020.csv"), index=False)
