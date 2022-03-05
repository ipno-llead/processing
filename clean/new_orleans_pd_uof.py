from lib.columns import clean_column_names
import deba
from lib.clean import clean_sexes, standardize_desc_cols
from lib.columns import set_values
from lib.clean import clean_races, clean_names
from lib.uid import gen_uid
import numpy as np
import pandas as pd
import sys

sys.path.append("../")


def join_citizen_columns(df):
    citizen_columns = [
        "citizen_race",
        "citizen_sex",
        "citizen_hospitalized",
        "citizen_injured",
        "citizen_influencing_factors",
        "citizen_distance_from_officer",
        "citizen_age",
        "citizen_build",
        "citizen_height",
        "citizen_arrested",
        "citizen_arrest_charges",
    ]
    for col in citizen_columns:
        df.loc[:, col] = df[col].str.split(r" \| ")

    def create_citizen_dicts(row: pd.Series):
        d = row.loc[citizen_columns].loc[row.notna()].to_dict()
        keys = d.keys()
        return [
            {k: d[k][i] for k in keys} for i in range(max(len(v) for v in d.values()))
        ]

    df.loc[:, "citizen"] = df.apply(create_citizen_dicts, axis=1)
    df = df.drop(
        columns=[
            "citizen_race",
            "citizen_sex",
            "citizen_hospitalized",
            "citizen_injured",
            "citizen_influencing_factors",
            "citizen_distance_from_officer",
            "citizen_age",
            "citizen_build",
            "citizen_height",
            "citizen_arrested",
            "citizen_arrest_charges",
        ]
    )

    df.loc[:, "citizen"] = df.citizen.apply(pd.DataFrame)
    df = df.drop("citizen", axis=1).join(
        pd.concat(df["citizen"].values, keys=df.index).droplevel(1)
    )

    return df


def join_officer_columns(df):
    officer_columns = [
        "officer_name",
        "race",
        "sex",
        "age",
        "years_of_service",
        "officer_injured",
    ]
    for col in officer_columns:
        df.loc[:, col] = df[col].str.split(r" \| ")

    def create_officer_dicts(row: pd.Series):
        d = row.loc[officer_columns].loc[row.notna()].to_dict()
        keys = d.keys()
        return [
            {k: d[k][i] for k in keys} for i in range(max(len(v) for v in d.values()))
        ]

    df.loc[:, "officer"] = df.apply(create_officer_dicts, axis=1)
    df = df.drop(
        columns=[
            "officer_name",
            "race",
            "sex",
            "age",
            "years_of_service",
            "officer_injured",
        ]
    )

    df.loc[:, "officer"] = df.officer.apply(pd.DataFrame)
    df = df.drop("officer", axis=1).join(
        pd.concat(df["officer"].values, keys=df.index).droplevel(1)
    )

    return df


def split_officer_names(df):
    names = df.officer_name.str.extract(r"(\w+)\, ?(\w+)?")
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    return df.drop(columns=["officer_name"])


def join_uof_columns(df):
    uof_columns = [
        "use_of_force_type",
        "use_of_force_level",
        "use_of_force_effective",
    ]
    for col in uof_columns:
        df.loc[:, col] = df[col].str.split(r" \| ")

    def create_uof_dicts(row: pd.Series):
        d = row.loc[uof_columns].loc[row.notna()].to_dict()
        keys = d.keys()
        return [
            {k: d[k][i] for k in keys} for i in range(max(len(v) for v in d.values()))
        ]

    df.loc[:, "uof"] = df.apply(create_uof_dicts, axis=1)
    df = df.drop(
        columns=["use_of_force_type", "use_of_force_level", "use_of_force_effective"]
    )

    df.loc[:, "uof"] = df.uof.apply(pd.DataFrame)
    df = df.drop("uof", axis=1).join(
        pd.concat(df["uof"].values, keys=df.index).droplevel(1)
    )

    return df


def clean_tracking_number(df):
    df.loc[:, "tracking_number"] = (
        df.filenum.str.lower().str.strip().str.replace(r"^20", "ftn20", regex=True)
    )
    return df.drop(columns=["filenum"])


def clean_originating_bureau(df):
    df.loc[:, "originating_bureau"] = (
        df.originating_bureau.str.lower()
        .str.strip()
        .str.replace(r"^(\w+) - ", "", regex=True)
    )
    return df


def clean_division_level(df):
    df.loc[:, "division_level"] = (
        df.division_level.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("police ", "", regex=False)
        .str.replace(" division", "", regex=False)
        .str.replace(r"\bisb\b", "investigative services", regex=True)
        .str.replace(r" ?staff", "", regex=True)
        .str.replace(" section", "", regex=False)
        .str.replace(" unit", "", regex=False)
        .str.replace(" team", "", regex=False)
        .str.replace("not nopd", "", regex=False)
        .str.replace("pib", "", regex=False)
    )
    return df


def clean_division(df):
    df.loc[:, "division"] = (
        df.division.str.lower()
        .str.strip()
        .str.replace(" section", "", regex=False)
        .str.replace(" team", "", regex=False)
        .str.replace(r"\bisb\b", "investigative services", regex=True)
        .str.replace(r" ?staff", "", regex=True)
        .str.replace(r"^b$", "", regex=True)
        .str.replace(r"\btact\b", "tactical", regex=True)
        .str.replace(r" \| ", "; ", regex=True)
        .str.replace(r"^admin$", "administration", regex=True)
        .str.replace(r" persons$", "", regex=True)
        .str.replace(r"d\.?i\.?u\.?", "", regex=True)
        .str.replace(r"investigation$", "investigations", regex=True)
        .str.replace("unknown", "", regex=False)
        .str.replace(r"^cid$", "criminal investigations", regex=True)
    )
    return df


def clean_unit(df):
    df.loc[:, "unit"] = (
        df.unit.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(" unit", "", regex=False)
        .str.replace("unknown", "", regex=False)
        .str.replace(r"null \| ", "", regex=True)
        .str.replace(r"^patr?$", "patrol", regex=True)
        .str.replace(r"^k$", "k-9", regex=True)
        .str.replace(r" other$", "", regex=True)
        .str.replace("dwi", "district investigative unit", regex=False)
        .str.replace(r"^uof$", "use of force", regex=True)
        .str.replace(r"^admin$", "administration", regex=True)
        .str.replace(r"\btact\b", "tactical", regex=True)
        .str.replace(r" persons diu$", "", regex=True)
        .str.replace(r"d\.?i\.?u\.?", "", regex=True)
        .str.replace(" section", "", regex=False)
        .str.replace(" staff", "", regex=False)
        .str.replace(r"v\.o\.\w\.s\.", "violent offender warrant squad", regex=True)
    )
    return df


def clean_working_status(df):
    df.loc[:, "working_status"] = (
        df.working_status.str.lower()
        .str.strip()
        .str.replace("unknown working status", "", regex=False)
        .str.replace("rui", "resigned under investigation", regex=False)
        .str.replace("off duty", "off-duty", regex=False)
        .str.replace("workingre", "working", regex=False)
    )
    return df


def clean_shift(df):
    df.loc[:, "shift_time"] = (
        df.shift_time.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("unknown shift hours", "", regex=False)
        .str.replace("8a-4p", "between 8am-4pm", regex=False)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"uof", "use of force", regex=False)
    )
    return df


def clean_service_type(df):
    df.loc[:, "service_type"] = (
        df.service_type.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r" \| ", "; ", regex=True)
        .str.replace("not used", "", regex=False)
    )
    return df


def clean_light_condition(df):
    df.loc[:, "light_condition"] = (
        df.light_condition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"null \| ", "", regex=True)
    )
    return df


def clean_weather_condition(df):
    df.loc[:, "weather_condition"] = (
        df.weather_condition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"null \| ", "", regex=True)
    )
    return df


def clean_use_of_force_reason(df):
    df.loc[:, "use_of_force_reason"] = (
        df.use_of_force_reason.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("refuse", "refused", regex=False)
        .str.replace("resisting", "resisted", regex=False)
        .str.replace(r"^no$", "", regex=True)
        .str.replace(r"w\/", "with ", regex=True)
        .str.replace(" police ", " ", regex=False)
    )
    return df.dropna()


def clean_use_of_force_type(df):
    df.loc[:, "use_of_force_type"] = (
        df.use_of_force_type.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("nontrad", "non traditional", regex=False)
        .str.replace(r"\bcew\b", "conducted electrical weapon", regex=True)
        .str.replace(r"(\w+)\((\w+)\)", r"\1 (\2)", regex=True)
        .str.replace(r"\(\)$", ")", regex=True)
        .str.replace(r"^- ", "", regex=True)
        .str.replace(r"^#name\?$", "", regex=True)
        .str.replace(r"\bvehpursuits\b", "vehicle pursuits", regex=True)
        .str.replace(r"w\/(\w+)", r"with \1", regex=True)
        .str.replace(r"\bwep\b", "weapon", regex=True)
        .str.replace(r"\bnonstrk\b", "non-strike", regex=True)
        .str.replace(r"\/pr-24 ", " ", regex=True)
    )
    return df.dropna()


def clean_citizen_arrest_charges(df):
    df.loc[:, "citizen_arrest_charges"] = (
        df.citizen_arrest_charges.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("flight,", "flight;", regex=False)
        .str.replace("null", "", regex=False)
        .str.replace("no", "", regex=False)
    )
    return df


def clean_citizen_age(df):
    df.loc[:, "citizen_age"] = df.citizen_age.str.replace("-1", "", regex=False)
    return df


def clean_citizen():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_uof_2016_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "occurred_date": "occur_date",
                "shift": "shift_time",
                "subject_ethnicity": "citizen_race",
                "subject_gender": "citizen_sex",
                "subject_hospitalized": "citizen_hospitalized",
                "subject_injured": "citizen_injured",
                "subject_influencing_factors": "citizen_influencing_factors",
                "subject_distance_from_officer": "citizen_distance_from_officer",
                "subject_age": "citizen_age",
                "subject_build": "citizen_build",
                "subject_height": "citizen_height",
                "subject_arrested": "citizen_arrested",
                "subject_arrest_charges": "citizen_arrest_charges",
            }
        )
        .drop(
            columns=[
                "officer_years_of_service",
                "officer_name",
                "officer_race_ethnicity",
                "officer_gender",
                "officer_age",
                "officer_injured",
                "use_of_force_effective",
                "use_of_force_type",
                "use_of_force_level",
                "use_of_force_reason",
                "occur_date",
                "originating_bureau",
                "division_level",
                "division",
                "unit",
                "working_status",
                "shift_time",
                "investigation_status",
                "disposition",
                "service_type",
                "light_condition",
                "weather_condition",
            ]
        )
        .pipe(join_citizen_columns)
        .pipe(clean_tracking_number)
        .pipe(clean_citizen_arrest_charges)
        .pipe(clean_citizen_age)
        .pipe(clean_races, ["citizen_race"])
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(
            standardize_desc_cols,
            [
                "citizen_hospitalized",
                "citizen_injured",
                "citizen_distance_from_officer",
                "citizen_age",
                "citizen_arrested",
                "citizen_influencing_factors",
                "citizen_build",
                "citizen_height",
            ],
        )
        .pipe(set_values, {"agency": "New Orleans PD"})
    )
    return df


def clean_officer():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_uof_2016_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "occurred_date": "occur_date",
                "shift": "shift_time",
                "officer_race_ethnicity": "race",
                "officer_age": "age",
                "officer_gender": "sex",
                "officer_years_of_service": "years_of_service",
            }
        )
        .drop(
            columns=[
                "subject_ethnicity",
                "subject_gender",
                "subject_hospitalized",
                "subject_injured",
                "subject_influencing_factors",
                "subject_distance_from_officer",
                "subject_age",
                "subject_build",
                "subject_height",
                "subject_arrested",
                "subject_arrest_charges",
                "occur_date",
                "originating_bureau",
                "division_level",
                "division",
                "unit",
                "working_status",
                "shift_time",
                "investigation_status",
                "disposition",
                "service_type",
                "light_condition",
                "weather_condition",
                "use_of_force_reason",
                "use_of_force_type",
                "use_of_force_effective",
                "use_of_force_level",
            ]
        )
        .pipe(join_officer_columns)
        .pipe(split_officer_names)
        .pipe(clean_tracking_number)
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_names, ["last_name", "first_name"])
        .pipe(standardize_desc_cols, ["officer_injured"])
        .pipe(set_values, {"agency": "New Orleans PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
    )
    return df


def clean_uof():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_uof_2016_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "occurred_date": "occur_date",
                "shift": "shift_time",
                "subject_ethnicity": "citizen_race",
                "subject_gender": "citizen_sex",
                "subject_hospitalized": "citizen_hospitalized",
                "subject_injured": "citizen_injured",
                "subject_influencing_factors": "citizen_influencing_factors",
                "subject_distance_from_officer": "citizen_distance_from_officer",
                "subject_age": "citizen_age",
                "subject_build": "citizen_build",
                "subject_arrested": "citizen_arrested",
                "subject_arrest_charges": "citizen_arrest_charges",
            }
        )
        .drop(
            columns=[
                "officer_years_of_service",
                "officer_name",
                "officer_race_ethnicity",
                "officer_gender",
                "officer_age",
                "officer_injured",
            ]
        )
        .pipe(join_uof_columns)
        .pipe(clean_originating_bureau)
        .pipe(clean_division)
        .pipe(clean_division_level)
        .pipe(clean_unit)
        .pipe(clean_weather_condition)
        .pipe(clean_service_type)
        .pipe(clean_disposition)
        .pipe(clean_working_status)
        .pipe(clean_light_condition)
        .pipe(clean_use_of_force_reason)
        .pipe(clean_use_of_force_type)
        .pipe(clean_tracking_number)
        .pipe(
            standardize_desc_cols,
            [
                "shift_time",
                "investigation_status",
                "use_of_force_reason",
                "use_of_force_type",
                "use_of_force_level",
                "use_of_force_effective",
            ],
        )
        .pipe(set_values, {"agency": "New Orleans PD"})
        .pipe(
            gen_uid,
            [
                "occur_date",
                "originating_bureau",
                "division_level",
                "division",
                "unit",
                "working_status",
                "shift_time",
                "investigation_status",
                "disposition",
                "service_type",
                "light_condition",
                "weather_condition",
                "use_of_force_reason",
                "use_of_force_type",
                "use_of_force_level",
                "use_of_force_effective",
                "tracking_number",
                "agency",
            ],
            "uof_uid",
        )
        .dropna(subset=["uof_uid"])
        .drop_duplicates(subset=["uof_uid"])
    )
    return df


if __name__ == "__main__":
    uof_citizen = clean_citizen()
    uof_officer = clean_officer()
    uof = clean_uof()
    uof_citizen.to_csv(deba.data("clean/uof_citizens_new_orleans_pd_2016_2021.csv"), index=False)
    uof_officer.to_csv(deba.data("clean/uof_officers_new_orleans_pd_2016_2021.csv"), index=False)
    uof.to_csv(deba.data("clean/uof_new_orleans_pd_2016_2021.csv"), index=False)
