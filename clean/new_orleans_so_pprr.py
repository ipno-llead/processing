import pandas as pd
import dirk
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import clean_salaries, standardize_desc_cols, clean_dates
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
    df.loc[:, "middle_initial"] = names[4].fillna("")
    return df.drop(columns="payroll_name")


def drop_rows_with_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def assign_agency(df):
    df.loc[:, "agency"] = "New Orleans SO"
    return df


def clean():
    df = (
        pd.read_csv(dirk.data("raw/new_orleans_so/new_orleans_so_pprr_2021.csv"))
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
            ["first_name", "middle_initial", "middle_name", "last_name", "agency"],
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(dirk.data("clean/pprr_new_orleans_so_2021.csv"), index=False)
