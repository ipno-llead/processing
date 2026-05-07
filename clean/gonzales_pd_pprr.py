import deba
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_dates, clean_salaries
import pandas as pd
from lib import salary
from lib.uid import gen_uid


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.replace(r"polio?ce ", "", regex=True)
        .str.replace("detetive", "detective", regex=False)
    )
    return df


def extract_sex(df):
    df.loc[:, "sex"] = (
        df.race_gender.str.lower()
        .str.strip()
        .str.replace("wm", "w/m", regex=False)
        .str.replace(r"(\w{1})\/(\w{1})", r"\2", regex=True)
        .str.replace("m", "male", regex=False)
        .str.replace("f", "female", regex=False)
    )
    return df


def clean_race(df):
    df.loc[:, "race"] = (
        df.race_gender.str.lower()
        .str.strip()
        .str.replace("wm", "w/m", regex=False)
        .str.replace(r"(\w{1})\/(\w{1})", r"\1", regex=True)
        .str.replace("w", "white", regex=False)
        .str.replace("b", "black", regex=False)
        .str.replace(r"^h$", "hispanic", regex=True)
    )
    return df.drop(columns="race_gender")


def split_names(df):
    names = df.name.str.strip()
    # extract quoted nicknames into middle_name e.g. "PJ", "Colby"
    df.loc[:, "middle_name"] = names.str.extract(r'"([^"]+)"', expand=False)
    # remove quoted nicknames from names before splitting
    names = names.str.replace(r'\s*"[^"]+"\s*', " ", regex=True).str.strip()
    parts = names.str.split(" ", n=1)
    df.loc[:, "first_name"] = parts.str[0]
    df.loc[:, "last_name"] = parts.str[1]
    return df.drop(columns="name")


def clean():
    df = (
        pd.read_csv(deba.data("raw/gonzales_pd/gonzales_pd_pprr_2010_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "badge": "badge_number",
                "firstname": "first_name",
                "lastname": "last_name",
                "rank": "rank_desc",
            }
        )
        .pipe(clean_rank_desc)
        .pipe(extract_sex)
        .pipe(clean_race)
        .pipe(standardize_desc_cols, ["first_name", "last_name"])
        .pipe(clean_dates, ["hire_date", "termination_date"])
        .pipe(clean_salaries, ["salary"])
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(set_values, {"agency": "gonzales-pd"})
        .pipe(gen_uid, ["agency", "first_name", "last_name", "badge_number"])
    )
    return df


def clean_2026():
    df = (
        pd.read_csv(
            deba.data("raw/gonzales_pd/gonzales_pd_pprr_2026.csv"),
            header=2,
        )
        .pipe(clean_column_names)
        .dropna(subset=["name"])
        .pipe(split_names)
        .pipe(standardize_desc_cols, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "gonzales-pd"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_gonzales_pd_2010_2021.csv"), index=False)
    df_2026 = clean_2026()
    df_2026.to_csv(deba.data("clean/pprr_gonzales_pd_2026.csv"), index=False)
