import pandas as pd

from lib import salary
from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_dates, clean_sexes, clean_races, clean_salaries
from lib.uid import gen_uid


def split_names(df):
    names = df.officer.str.lower().str.strip().str.extract(r"^(\w+) (\w+) (\w)/(\w)$")
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    df.loc[:, "race"] = names[2]
    df.loc[:, "sex"] = names[3]
    return df.drop(columns=["officer"])


def standardize_rank(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.replace(r"\basst\b", "assistant", regex=True)
        .str.replace(r"\bsgt\b", "sergeant", regex=True)
    )
    return df


def remove_current_dates(df):
    df.loc[:, "termination_date"] = (
        df.termination_date.str.lower()
        .str.strip()
        .str.replace("current", "", regex=False)
    )
    return df


def clean():
    return (
        pd.read_csv(deba.data("raw/sterlington_pd/sterlington_csd_ppprr_2010_2020.csv"))
        .pipe(clean_column_names)
        .dropna(axis=1, how="all")
        .rename(columns={"rank": "rank_desc", "dob": "birth_date"})
        .pipe(split_names)
        .pipe(remove_current_dates)
        .pipe(standardize_rank)
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_races, ["race"])
        .pipe(clean_salaries, ["salary"])
        .pipe(clean_dates, ["birth_date"])
        .pipe(set_values, {"agency": "Sterlington PD", "salary_freq": salary.YEARLY})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_sterlington_csd_2010_2020.csv"), index=False)
