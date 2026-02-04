from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_dates, clean_names, float_to_int_str, standardize_desc_cols, clean_sexes, clean_races, clean_salaries
from lib.uid import gen_uid
from lib import salary
import pandas as pd


def standardize_race(df):
    df.loc[:, "race"] = df.race.str.replace(r"^cauc$", "white", regex=True)
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "st-tammany-so"
    df.loc[:, "data_production_year"] = 2020
    return df


def clean():
    df = pd.read_csv(deba.data("raw/st_tammany_so/st._tammany_so_pprr_2020.csv"))
    df = clean_column_names(df)
    df = df.rename(
        columns={
            "pay_job_class_desc": "rank_desc",
            "terminated_date": "term_date",
            "employee": "employee_id",
        }
    )
    return (
        df.pipe(float_to_int_str, ["birth_year", "hire_date", "term_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_dates, ["hire_date", "term_date"])
        .pipe(assign_agency)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            gen_uid, ["agency", "employee_id", "first_name", "last_name", "birth_year"]
        )
        .pipe(gen_uid, ["uid", "hire_year", "hire_month", "hire_day"], "perhist_uid")
    )

def split_dates(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r"^\s*$", "", regex=True)
        dates = df[col].str.split("/", expand=True)
        df.loc[:, col + "_month"] = dates[0].fillna("")
        df.loc[:, col + "_day"] = dates[1].fillna("") if 1 in dates.columns else ""
        df.loc[:, col + "_year"] = dates[2].fillna("") if 2 in dates.columns else ""
        df = df.drop(columns=[col])
    return df


def clean_employment_status(df):
    df.loc[:, "employment_status"] = df.employment_status.map({"A": "active", "I": "inactive"})
    return df


def assign_agency26(df):
    df.loc[:, "agency"] = "st-tammany-so"
    df.loc[:, "data_production_year"] = 2026
    return df

def clean26():
    df = pd.read_csv(deba.data("raw/st_tammany_so/stpso_pprr_2022_2026.csv"))
    df = clean_column_names(df)
    df = df.rename(
        columns={
            "gender_code": "sex",
            "eeo_race_ethnicity_code_desc": "race",
            "job_class_code_desc": "rank_desc",
            "department_code_desc": "department_desc",
            "personnel_status_code_desc": "employment_status_desc",
            "active_status": "employment_status",
            "annual_pay": "salary",
            "year_of_birth": "birth_year",
            "employee_number": "employee_id",
            "inactive_date": "inactive",
            "hire_date": "hire",
        })
    df.pipe(clean_names, ["first_name", "last_name"])
    df.pipe(assign_agency26)
    df.pipe(clean_sexes, ["sex"])
    df.pipe(standardize_race)
    df.pipe(clean_races, ["race"]) 
    df.pipe(standardize_desc_cols, ["rank_desc", "department_desc", "employment_status_desc"])
    df.pipe(split_dates, ["hire"])
    df.pipe(split_dates, ["inactive"])
    df.drop(columns=["hire", "inactive"], inplace=True)
    df.pipe(clean_employment_status)
    df.pipe(clean_salaries, ["salary"])
    df.pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "salary_date": "2/4/2026",
            },
        )
    df.pipe(gen_uid, ["agency", "employee_id", "first_name", "last_name", "birth_year"])
    return df

if __name__ == "__main__":
    df = clean()
    df26 = clean26()
    df.to_csv(deba.data("clean/pprr_st_tammany_so_2020.csv"), index=False)
    df26.to_csv(deba.data("clean/pprr_st_tammany_so_2026.csv"), index=False)
