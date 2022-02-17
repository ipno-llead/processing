import pandas as pd

from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_dates,
    clean_names,
    clean_salaries,
    clean_sexes,
    standardize_desc_cols,
)
import dirk
from lib.uid import gen_uid


def clean_department_desc(df):
    df.loc[:, "department_desc"] = (
        df.department_desc.str.lower()
        .str.strip()
        .replace({"pol": "police", "disp": "communications"})
    )
    return df


def clean_pprr():
    return (
        pd.read_csv(dirk.data("raw/ponchatoula_pd/ponchatoula_pd_pprr_2010_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns=["class", "status_code"])
        .rename(
            columns={
                "employee": "employee_id",
                "middle_init": "middle_initial",
                "regular_rate": "salary",
                "work_center": "department_desc",
            }
        )
        .pipe(clean_names, ["first_name", "middle_initial", "last_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(standardize_desc_cols, ["salary_freq"])
        .pipe(clean_department_desc)
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(set_values, {"agency": "Ponchatoula PD"})
        .pipe(gen_uid, ["agency", "employee_id"])
    )


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"\bcommens\b", "comments", regex=True)
        .str.replace(r"\bunbeomcing\b", "unbecoming", regex=True)
        .str.replace(r"\bchemeical\b", "chemical", regex=True)
        .str.replace(r"\bdiscourtest\b", "discourtesy", regex=True)
    )
    return df


def replace_names(df):
    df.loc[df.first_name == "rj", "first_name"] = "randy"
    return df


def clean_cprr():
    return (
        pd.read_csv(dirk.data("raw/ponchatoula_pd/ponchatoula_cprr_2010_2020.csv"))
        .pipe(clean_column_names)
        .rename(columns={"charges": "allegation"})
        .pipe(clean_allegation)
        .pipe(standardize_desc_cols, ["allegation", "disposition", "action"])
        .pipe(set_values, {"agency": "Ponchatoula PD"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(replace_names)
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid, ["agency", "uid", "receive_date", "allegation"], "allegation_uid"
        )
    )


if __name__ == "__main__":
    clean_pprr().to_csv(
        dirk.data("clean/pprr_ponchatoula_pd_2010_2020.csv"), index=False
    )
    clean_cprr().to_csv(
        dirk.data("clean/cprr_ponchatoula_pd_2010_2020.csv"), index=False
    )
