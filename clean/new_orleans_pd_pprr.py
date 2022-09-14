import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_races,
    clean_sexes,
    clean_dates,
    clean_salaries,
    clean_names,
    float_to_int_str,
    standardize_desc_cols,
)
from lib import salary
from lib.uid import gen_uid


def drop_first_and_last_row_overtime(df):
    df = df.iloc[1:-2]
    return df


def drop_rows_missing_name_overtime(df):
    df.loc[:, "name"] = df.name.fillna("")
    return df[~((df.name == ""))]


def split_names_overtime(df):
    names = (
        df.name.str.lower()
        .str.strip()
        .fillna("")
        .str.extract(r"(.+)\,(\w+\'?\-?\w+?) ?\b(\w{1})?$")
    )

    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    return df.drop(columns=["name"])


def clean_department_desc(df):
    df.loc[:, "department_desc"] = (
        df.orgn_desc.fillna("")
        .str.lower()
        .str.strip()
        .str.replace(r"^pol ", "", regex=True)
        .str.replace(r"^police recruits$", "recruits", regex=True)
        .str.replace(r"(\w+)  +(\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^investigations & sprt bure$",
            "investigations and support bureau",
            regex=True,
        )
        .str.replace(
            r"^special investigation div$",
            "special investigations division",
            regex=True,
        )
        .str.replace(
            r"^public integrity$",
            "public integrity bureau (internal affairs)",
            regex=True,
        )
        .str.replace(r"^off\b", "office", regex=True)
        .str.replace(r"prevent dst", "prevention district", regex=True)
        .str.replace(r"managemnt", "management", regex=False)
    )
    return df.drop(columns=["orgn_desc"])


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.title_desc.fillna("")
        .str.lower()
        .str.strip()
        .str.replace(r"^police ", "", regex=True)
        .str.replace(r"^senior police officer$", "senior officer", regex=True)
        .str.replace(r"supt\b", "superintendent", regex=True)
    )

    return df.drop(columns=["title_desc"])


def clean_emp_id_csd(df):
    df.loc[:, "employee_id"] = df.employee_id.astype(str).str.replace(
        r"^00?", "", regex=True
    )
    return df


def drop_rows_missing_names(df):
    return df[~((df.first_name.fillna("") == ""))]


def clean():
    df19 = (
        pd.read_csv(
            deba.data("raw/new_orleans_pd/new_orleans_pd_pprr_overtime_2019.csv")
        )
        .drop(
            columns=[
                "Months",
                "Unnamed: 3",
                "Unnamed: 4",
                "Unnamed: 5",
                "Unnamed: 6",
                "Unnamed: 7",
                "Unnamed: 8",
                "Unnamed: 9",
                "Unnamed: 10",
                "Unnamed: 11",
                "Unnamed: 12",
                "Unnamed: 13",
                "Unnamed: 14",
            ]
        )
        .rename(
            columns={
                "Sum of Earnings per PPE": "employee_id",
                "Unnamed: 1": "name",
                "Unnamed: 15": "overtime_and_detail_annual_total",
            }
        )
        .pipe(set_values, {"overtime_and_detail_date": "12/31/2019"})
        .pipe(drop_first_and_last_row_overtime)
        .pipe(drop_rows_missing_name_overtime)
        .pipe(split_names_overtime)
        .pipe(clean_emp_id_csd)
        .drop(columns=["first_name", "last_name", "middle_name"])
    )

    df20 = (
        pd.read_csv(
            deba.data("raw/new_orleans_pd/new_orleans_pd_pprr_overtime_2020.csv")
        )
        .drop(
            columns=[
                "Months",
                "Unnamed: 3",
                "Unnamed: 4",
                "Unnamed: 5",
                "Unnamed: 6",
                "Unnamed: 7",
                "Unnamed: 8",
                "Unnamed: 9",
                "Unnamed: 10",
                "Unnamed: 11",
                "Unnamed: 12",
                "Unnamed: 13",
                "Unnamed: 14",
            ]
        )
        .rename(
            columns={
                "Sum of Earnings per PPE": "employee_id",
                "Unnamed: 1": "name",
                "Unnamed: 15": "overtime_and_detail_annual_total",
            }
        )
        .pipe(set_values, {"overtime_and_detail_date": "12/31/2020"})
        .pipe(drop_first_and_last_row_overtime)
        .pipe(drop_rows_missing_name_overtime)
        .pipe(split_names_overtime)
        .pipe(clean_emp_id_csd)
        .drop(columns=["first_name", "last_name", "middle_name"])
    )

    df21 = (
        pd.read_csv(
            deba.data("raw/new_orleans_pd/new_orleans_pd_pprr_overtime_2021.csv")
        )
        .drop(
            columns=[
                "Months",
                "Unnamed: 3",
                "Unnamed: 4",
                "Unnamed: 5",
                "Unnamed: 6",
                "Unnamed: 7",
                "Unnamed: 8",
                "Unnamed: 9",
                "Unnamed: 10",
                "Unnamed: 11",
                "Unnamed: 12",
                "Unnamed: 13",
                "Unnamed: 14",
            ]
        )
        .rename(
            columns={
                "Sum of Earnings per PPE": "employee_id",
                "Unnamed: 1": "name",
                "Unnamed: 15": "overtime_and_detail_annual_total",
            }
        )
        .pipe(set_values, {"overtime_and_detail_date": "12/31/2021"})
        .pipe(drop_first_and_last_row_overtime)
        .pipe(drop_rows_missing_name_overtime)
        .pipe(split_names_overtime)
        .pipe(clean_emp_id_csd)
        .drop(columns=["first_name", "last_name", "middle_name"])
    )

    overtime = pd.concat([df19, df20, df21], axis=0).drop_duplicates(
        subset=[
            "overtime_and_detail_annual_total",
            "overtime_and_detail_date",
            "employee_id",
        ]
    )

    personnel = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_pprr_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns=["city_personal_dta", "county_personal_dta"])
        .rename(
            columns={
                "empl_id": "employee_id",
                "hire_date_employment_dta": "hire_date",
                "race_ethnicity": "race",
                "gender": "sex",
                "annual_rate_job_dta": "salary",
                "badge_number": "badge_no",
            }
        )
        .pipe(clean_dates, ["hire_date", "rehire_date"])
        .pipe(clean_department_desc)
        .pipe(clean_rank_desc)
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(float_to_int_str, ["employee_id"])
        .pipe(standardize_desc_cols, ["employee_id"])
        .pipe(clean_salaries, ["salary"])
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "agency": "new-orleans-pd",
                "salary_date": "12/31/2020",
            },
        )
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(
            gen_uid, ["first_name", "last_name", "middle_name", "employee_id", "agency"]
        )
        .drop_duplicates(subset=["uid", "salary", "salary_date"])
    )

    df = pd.merge(personnel, overtime, on="employee_id", how="outer").pipe(
        drop_rows_missing_names
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_new_orleans_pd_2020.csv"), index=False)
