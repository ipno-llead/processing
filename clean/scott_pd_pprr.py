import pandas as pd
from lib.columns import clean_column_names, set_values
import bolo
from lib.uid import gen_uid
from lib import salary
from lib.clean import clean_names, clean_dates, clean_sexes, clean_races, clean_salaries


def split_name(df):
    series = df.full_name.fillna("").str.strip()
    for col, pat in [
        ("first_name", r"^([\w'-]+)(.*)$"),
        ("middle_initial", r"^(\w\.|\w\s)(.*)$"),
    ]:
        names = series[series.str.match(pat)].str.extract(pat)
        df.loc[series.str.match(pat), col] = names[0]
        series = series.str.replace(pat, r"\2", regex=True).str.strip()
    df.loc[:, "last_name"] = series
    return df.drop(columns=["full_name"])


def clean_rank(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.replace("asst.", "assistant", regex=False)
        .str.replace("police", "", regex=False)
        .str.replace("admin", "administration", regex=False)
        .str.replace("sro", "school resource officer", regex=False)
        .str.replace("drc", "department records clerk", regex=False)
        .str.replace(
            "lieutenant/ school resource officer sup.",
            "lieutenant | school resource officer supervisor",
            regex=False,
        )
    )
    return df


def clean():
    df = pd.read_csv(bolo.data("raw/scott_pd/scott_pd_pprr_2021.csv"))
    df = clean_column_names(df)
    df.columns = [
        "full_name",
        "sex",
        "race",
        "birth_date",
        "salary",
        "hire_date",
        "termination_date",
        "badge_no",
        "rank_desc",
        "assigned_zone",
    ]
    df = (
        df.drop(columns=["assigned_zone"])
        .rename(columns={"termination_date": "term_date"})
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(clean_salaries, ["salary"])
        .pipe(split_name)
        .pipe(clean_rank)
        .pipe(clean_names, ["rank_desc"])
        .pipe(clean_dates, ["birth_date", "hire_date", "term_date"])
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_names, ["first_name", "last_name", "middle_initial"])
        .pipe(set_values, {"data_production_year": 2021, "agency": "Scott PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name", "birth_year", "badge_no"])
    )
    return df


if __name__ == "__main__":
    df = clean()

    df.to_csv(bolo.data("clean/pprr_scott_pd_2021.csv"), index=False)
