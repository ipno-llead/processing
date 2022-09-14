import pandas as pd
import deba

from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, clean_names, clean_salaries
from lib.uid import gen_uid
from lib import salary


def clean_department_desc(df):
    df.loc[:, "department_desc"] = (
        df.location_deso.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("police ", "", regex=False)
    )
    return df.drop(columns="location_deso")


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.job_class_desc.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("police ", "", regex=False)
    )
    return df.drop(columns="job_class_desc")


def clean():
    df = (
        pd.read_csv(deba.data("raw/hammond_pd/hammond_pd_pprr_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "m": "middle_name",
                "date_of_birth": "birth_date",
                "annual_salary": "salary",
            }
        )
        .drop(columns=["badge_number"])
        .pipe(clean_dates, ["birth_date"])
        .pipe(clean_department_desc)
        .pipe(clean_rank_desc)
        .pipe(clean_dates, ["hire_date"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(set_values, {"agency": "hammond-pd", "salary_freq": salary.YEARLY})
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_hammond_pd_2021.csv"), index=False)
