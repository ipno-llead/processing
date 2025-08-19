import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import (
    clean_salaries,
    standardize_desc_cols,
    clean_dates,
    float_to_int_str,
    clean_sexes,
    clean_races,
    clean_names,
)
from lib import salary


def clean_division_desc(df):
    df.loc[:, "division_desc"] = (
        df.home_department_description.str.lower()
        .str.strip()
        .str.replace(r"  +", " ", regex=True)
        .str.replace(r"^security- ", "", regex=True)
        .str.replace(r"civil division - (\w+)", r"civil \1", regex=True)
        .str.replace("&", "and", regex=False)
        .str.replace("extended leave", "", regex=False)
        .str.replace(" and processing", "", regex=False)
        .str.replace("iad", "internal affairs", regex=False)
        .str.replace(r"isb- | ?department ?| ?division ?| ?unit ?", "", regex=True)
        .str.replace(r"investigative ?(team)?", "investigations", regex=True)
        .str.replace(r"\badministrative\b", "", regex=True)
        .str.replace("civil moveables", "forfeitures", regex=False)
        .str.replace("chief of corrections staff", "corrections", regex=False)
        .str.replace("civil ", "", regex=False)
        .str.replace("orleans justice complex", "", regex=False)
    )
    return df.drop(columns="home_department_description")


def clean_rank(df):
    df.loc[:, "rank_desc"] = (
        df.job_title_description.str.lower()
        .str.strip()
        .str.replace(r"(\w+) 1", r"1st class \1", regex=True)
        .str.replace(r"(\w+) 2", r"2nd class \1", regex=True)
        .str.replace(" moveables/seizures", " forfeitures", regex=False)
        .str.replace(
            r"hr |human resources |payroll |real estate |sanitation |"
            r"isb |iad |inmate accounts |benefits | - intake and processing|"
            r"part time |corrections monitoring |detail inspections |"
            r"administrative support |grievance | of case|landscaper/ |"
            r" - judicial enforcement| -jto|reserve ",
            "",
            regex=True,
        )
        .str.replace(r"(.+) clerk", "clerk", regex=True)
        .str.replace(r"recruit-(\w+)", "recruit", regex=True)
        .str.replace(r"executive assistant (.+)", "executive assistant", regex=True)
        .str.replace(r"((.+) manager|manager (.+))", r"manager", regex=True)
        .str.replace(r"(.+)? ?chief ?(.+)?", "chief", regex=True)
        .str.replace(r"(.+)? ?director ?(.+)?", r"director", regex=True)
        .str.replace(
            r"administrative assistant (.+)", "administrative assistant", regex=True
        )
        .str.replace(r"(.+) analyst", "analyst", regex=True)
        .str.replace(r"\/(.+)", "", regex=True)
    )
    return df.drop(columns="job_title_description")


def remove_incorrect_salaries(df):
    df.loc[:, "salary"] = df.salary.astype(str).str.replace(
        r"^(\d{4})\..+", "", regex=True
    )
    return df


def clean_officer_inactive(df):
    df.loc[:, "officer_inactive"] = (
        df.position_status.str.lower()
        .str.strip()
        .str.replace("active", "no", regex=False)
    )
    return df.drop(columns="position_status")


def extract_officer_contract_status(df):
    df.loc[:, "worker_category_description"] = (
        df.worker_category_description.str.lower()
        .str.strip()
        .str.replace("temp", "temporary", regex=False)
        .str.replace("regular", "permanent", regex=False)
    )
    status = df.worker_category_description.str.extract(
        r"(temporary|permanent|volunteer)"
    )
    df.loc[:, "officer_contract_status"] = status[0]
    return df


def clean_employment_status(df):
    df.loc[:, "officer_employment_status"] = df.worker_category_description.str.replace(
        r"( ?temporary ?| ?permanent ?|volunteer| sheriff only)", "", regex=True
    )
    return df.drop(columns="worker_category_description")


def split_names(df):
    df.loc[:, "payroll_name"] = (
        df.payroll_name.str.lower()
        .str.strip()
        .str.replace("test2, test1 t", "", regex=False)
        .str.replace(r" u n$", "", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"^(\w+) -(\w),?", r"\1-\2", regex=True)
        .str.replace(r"(\w+) (\w+), (\w+)", r"\1, \3 \2", regex=True)
        .str.replace(r"(\w+), (\w+), (\w+)", r"\1, \2 \3", regex=True)
        .str.replace(r"(\w+), (\w+) (\w{2})", r"\1 \3, \2", regex=True)
        .str.replace("boyd le, cinnamone", "boyd-lee, cinnamone", regex=False)
        .str.replace("brunet da", "brunet-da", regex=False)
        .str.replace("carter jo", "carter-jo", regex=False)
        .str.replace("della sa", "della-sa", regex=False)
        .str.replace("dimitry ja", "dimitry-ja", regex=False)
        .str.replace("coston jo", "coston-jo", regex=False)
        .str.replace("favorite br", "favorite-br", regex=False)
        .str.replace("douglas ge", "douglas-ge", regex=False)
        .str.replace("funmi ti", "funmi-ti", regex=False)
        .str.replace("johnson ka", "johnson-ka", regex=False)
        .str.replace("lewis de", "lewis-de", regex=False)
        .str.replace("levy ra", "levy-ra", regex=False)
        .str.replace("leonard ve", "leonard-ve", regex=False)
        .str.replace("morreale fr", "morreale-fr", regex=False)
        .str.replace("o'neal mi", "o'neal-mi", regex=False)
        .str.replace("spellman mi", "spellman-mi", regex=False)
        .str.replace("savwoir ge", "savwoir-ge", regex=False)
    )

    names = df.payroll_name.str.extract(
        r"(?:(\w+\'?\w+?\-?\w+)) ?(jr|sr|ii)?, (\w+\'?-?\w+) ?(\w{2,})? ?(\w{1})?"
    )
    df.loc[:, "last_name"] = names[0].fillna("")
    df.loc[:, "suffix"] = names[1].fillna("")
    df.loc[:, "first_name"] = names[2].fillna("")
    df.loc[:, "middle_name"] = names[3].fillna("")
    df.loc[:, "middle_name"] = names[4].fillna("")
    return df.drop(columns="payroll_name")


def drop_rows_with_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def assign_agency(df):
    df.loc[:, "agency"] = "new-orleans-so"
    return df


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.rank_code.str.lower().str.strip().str.replace(r"(\w+) - ", "", regex=True)
    )
    return df.drop(columns=["rank_code"])


def split_names_overtime(df):
    names = (
        df.payroll_name.str.lower()
        .str.strip()
        .str.extract(r"(.+)\, (\w+\'?-?\w+) ?(\w+)?$")
    )

    df.loc[:, "last_name"] = names[0].str.replace(r"(\,|\.)", "", regex=True)
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    return df.drop(columns=["payroll_name"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_so/new_orleans_so_pprr_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "location_description": "department_desc",
                "adjusted_service": "time_active",
                "regular_pay_rate_amount": "salary",
            }
        )
        .pipe(clean_division_desc)
        .pipe(standardize_desc_cols, ["department_desc"])
        .pipe(clean_rank)
        .pipe(clean_officer_inactive)
        .pipe(extract_officer_contract_status)
        .pipe(clean_employment_status)
        .pipe(clean_salaries, ["salary"])
        .pipe(remove_incorrect_salaries)
        .pipe(set_values, {"salary_freq": salary.HOURLY})
        .pipe(clean_dates, ["hire_date"])
        .pipe(split_names)
        .pipe(drop_rows_with_missing_names)
        .pipe(assign_agency)
        .pipe(
            gen_uid,
            ["first_name", "middle_name", "last_name", "agency"],
        )
    )
    return df

def split_name_25(df: pd.DataFrame, col: str = "name") -> pd.DataFrame:
    """
    Splits a column of names in the format 'Last, First Middle' 
    into last_name, first_name, and middle_name.
    Returns the same DataFrame with 3 new columns.
    """
    # Ensure no NaNs
    df = df.copy()
    df[col] = df[col].fillna("")

    # Split at comma into Last and the rest
    split = df[col].str.split(",", n=1, expand=True)
    df["last_name"] = split[0].str.strip()

    # Now split the rest into first and (optional) middle
    first_middle = split[1].str.strip().str.split(r"\s+", n=1, expand=True)
    df["first_name"] = first_middle[0].fillna("")
    df["middle_name"] = first_middle[1].fillna("")

    return df

def clean25():
    df25 = (
        pd.read_csv(deba.data("raw/new_orleans_so/new_orleans_so_pprr_2025.csv"))
        .pipe(clean_column_names)
        .drop(columns=["position_id"])
        .rename(
            columns={
                "payroll_name": "name",
                "gender_for_insurance_coverage": "sex",
                "race_description": "race",
                "hire_rehire_date": "hire_date",
                "rank_code": "rank_desc",
                "regular_pay_rate_amount": "salary",
                "position_status": "officer_employment_status"})
        .pipe(clean_rank)
        .pipe(clean_dates, ["hire_date", "birth_date"])
        .pipe(standardize_desc_cols, ["rank_desc", "officer_employment_status"])
        .pipe(split_name_25, col="name")
        .drop(columns=["name"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_races, ["race"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(set_values, {"salary_freq": salary.HOURLY})
        .pipe(assign_agency)
        .pipe(
            gen_uid,
            ["first_name", "middle_name", "last_name", "agency"],
        )
    )
    return df25


def overtime():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_so/new_orleans_so_overtime_2020.csv"))
        .pipe(clean_column_names)
        .rename(columns={"overtime_earnings_total": "overtime_annual_total"})
        .pipe(clean_rank_desc)
        .pipe(split_names_overtime)
        .pipe(set_values, {"overtime_date": "12/31/2020"})
        .pipe(float_to_int_str, ["overtime_annual_total"])
        .pipe(clean_salaries, ["overtime_annual_total"])
        .pipe(set_values, {"overtime_freq": salary.YEARLY})
        .pipe(set_values, {"agency": "new-orleans-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["uid", "overtime_annual_total", "overtime_date"], "overtime_uid"
        )
        .drop_duplicates(subset=["uid", "overtime_date"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df25 = clean25()
    overtime20 = overtime()
    df.to_csv(deba.data("clean/pprr_new_orleans_so_2021.csv"), index=False)
    overtime20.to_csv(
        deba.data("clean/pprr_overtime_new_orleans_so_2020.csv"), index=False
    )
    df25.to_csv(deba.data("clean/pprr_new_orleans_so_2025.csv"), index=False)
