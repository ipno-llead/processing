import pandas as pd

from lib import salary
from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_salaries, clean_sexes, clean_races
from lib.uid import gen_uid


def split_names(df):
    names = df.employee_name.str.lower().str.strip().str.extract(r"^(\w+),\s+(\w+)$")
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[0]
    return df.drop(columns="employee_name")


def standardize_ranks(df):
    df.loc[:, "rank_desc"] = df.rank_desc.replace(
        {
            "CPT": "captain",
            "OFF": "officer",
            "CPL": "corporal",
            "SGT": "sergeant",
            "CHIEF": "chief",
            "CAPT": "captain",
            "MAJ": "major",
        }
    )
    return df


def change_hire_date_format(df):
    df.loc[:, "hire_date"] = (
        df.hire_date.str.strip()
        .str.replace(r".+/$", "", regex=True)
        .str.replace(r"/([6789]\d)$", r"/19\1", regex=True)
        .str.replace(r"/(\d{2})$", r"/20\1", regex=True)
    )
    return df


def clean():
    return (
        pd.read_csv(deba.data("raw/west_monroe_pd/west_monroe_pd_pprr_2015_2020.csv"))
        .pipe(clean_column_names)
        .dropna(how="all")
        .rename(
            columns={
                "sex": "race",
                "gender": "sex",
                "rank": "rank_desc",
                "mo_salary": "salary",
                "badge": "badge_no",
            }
        )
        .pipe(split_names)
        .pipe(standardize_ranks)
        .pipe(change_hire_date_format)
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_races, ["race"])
        .pipe(clean_salaries, ["salary"])
        .pipe(set_values, {"agency": "west-monroe-pd", "salary_freq": salary.MONTHLY})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_west_monroe_pd_2015_2020.csv"), index=False)
