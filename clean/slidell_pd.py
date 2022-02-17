import functools
import operator

import pandas as pd

from lib.columns import clean_column_names, set_values
import dirk
from lib.uid import gen_uid
from lib.clean import (
    clean_names,
    clean_races,
    clean_salaries,
    clean_sexes,
    standardize_desc_cols,
)
from lib import salary


def realign09(df):
    df.loc[:, "name"] = df.name.fillna(method="ffill")
    return df.dropna(
        how="all",
        subset=["rank_desc", "salary_date", "hire_date", "salary", "salary_freq"],
    )


def split_names(df):
    names = (
        df.name.str.lower()
        .str.strip()
        .str.replace(r' "[^"]+"', "", regex=True)
        .str.replace(r",? (ii|iii|iv|jr\.?),", r" \1,", regex=True)
        .str.replace(r"\bjr\.", "jr", regex=True)
        .str.extract(r"^([^,]+), ([^ ]+)$")
    )
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[0]
    return df.drop(columns=["name"])


def clean_rank(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.replace("temp.", "temporary", regex=False)
        .str.replace("asst.", "assistant", regex=False)
        .str.replace(r"(sergeamt|sgt.)", "sergeant", regex=True)
        .str.replace(r"\/tech ", " technology ", regex=True)
        .str.replace(r"support s$", "support special", regex=True)
        .str.replace(r" (c of|chief of( p)?)$", " chief of police", regex=True)
        .str.replace(r"-$", "", regex=True)
        .str.replace(r"\b3rd$", "3rd class", regex=True)
        .str.replace(r"\b2 nd\b", "2nd", regex=True)
        .str.replace(r"\b2nd$", "2nd class", regex=True)
        .str.replace("policesergeant", "police sergeant", regex=False)
        .str.replace(r"\bschol\b", "school", regex=True)
        .str.replace(r"\boff$", "officer", regex=True)
        .str.replace(r"\btemp\b", "temporary", regex=True)
        .str.replace(r"\bresource\b", "resources", regex=True)
    )
    categories = [
        "acting chief of police",
        "chief of police",
        "chief of police elect",
        "captain, assistant chief of police",
        "captain",
        "police captain",
        "police corporal",
        "lieutenant",
        "police lieutenant",
        "sergeant",
        "police sergeant",
        "temporary police sergeant",
        "police officer 1st class",
        "police officer 2nd class",
        "police officer 3rd class",
        "police officer iii",
        "police officer",
        "police",
        "reserve police officer",
        "temporary police officer - sr",
        "temporary police officer 2nd class",
        "temporary police officer 3rd class",
        "temporary police officer",
        "sergeant technology support special",
        "corrections lieutenant",
        "corrections officer ii",
        "corrections officer iii",
        "corrections peace officer",
        "temporary corrections officer",
        "special assistant to chief of police",
        "temporary appointment",
        "school resources officer",
        "special detail - wal-mart",
    ]
    unincluded_vals = df.loc[
        (df.rank_desc != "") & ~df.rank_desc.isin(categories), "rank_desc"
    ].unique()
    if len(unincluded_vals) > 0:
        raise ValueError("rank values not found in categories: %s" % unincluded_vals)
    df.loc[:, "rank_desc"] = df.rank_desc.astype(
        pd.CategoricalDtype(categories, ordered=True)
    )
    return df


def zero_pad_dates(df, cols):
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.replace(r"^(\d)/", r"0\1/", regex=True)
            .str.replace(r"/(\d)/", r"/0\1/", regex=True)
        )
    return df


def fix_month_day_order(df, cols):
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.replace(r"^(2\d)/(\d+)/", r"\2/\1/", regex=True)
            .str.replace(r"^(1[3456789])/(\d+)/", r"\2/\1/", regex=True)
        )
    return df


def clean_pprr_09():
    return (
        pd.read_csv(dirk.data("raw/slidell_pd/slidell_pd_pprr_2009.csv"), delimiter=";")
        .pipe(clean_column_names)
        .rename(
            columns={
                "frequency": "salary_freq",
                "title": "rank_desc",
                "effective": "salary_date",
                "pay": "salary",
            }
        )
        .replace(
            {
                "salary_freq": {
                    "Hourly": salary.HOURLY,
                    "Daily": salary.DAILY,
                    "Monthly": salary.MONTHLY,
                    "Bi-Weekly": salary.BIWEEKLY,
                }
            }
        )
        .pipe(realign09)
        .pipe(fix_month_day_order, ["hire_date"])
        .pipe(clean_salaries, ["salary"])
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(zero_pad_dates, ["hire_date", "salary_date"])
        .reset_index(drop=True)
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_rank)
        .pipe(set_values, {"agency": "Slidell PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


def realign19(df):
    df = df.dropna(how="all")
    df.columns = ["name"] + df.iloc[0].tolist()[1:]
    df.iloc[:, 0] = df.iloc[:, 0].fillna(method="ffill")
    df = df.dropna(axis=1, how="all")
    return (
        df[
            ~functools.reduce(
                operator.and_, [df[col] == col for col in df.columns[1:8]]
            )
        ]
        .reset_index(drop=True)
        .fillna(method="ffill")
    )


def clean_badge_no(df):
    df.loc[:, "badge_no"] = (
        df.badge_no.str.lower()
        .str.strip()
        .str.replace(r"^not found$", "", regex=True)
        .str.replace(r"^n/a new hire$", "", regex=True)
    )
    return df


def clean_pprr_19():
    return (
        pd.read_csv(dirk.data("raw/slidell_pd/slidell_pd_pprr_2019.csv"), delimiter=";")
        .pipe(realign19)
        .pipe(clean_column_names)
        .rename(
            columns={
                "frequency": "salary_freq",
                "title": "rank_desc",
                "effective": "salary_date",
                "pay": "salary",
                "division": "department_desc",
                "badge_number": "badge_no",
            }
        )
        .replace(
            {
                "salary_freq": {
                    "Hourly": salary.HOURLY,
                    "Daily": salary.DAILY,
                    "Monthly": salary.MONTHLY,
                    "Bi-Weekly": salary.BIWEEKLY,
                }
            }
        )
        .pipe(clean_salaries, ["salary"])
        .pipe(clean_badge_no)
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(zero_pad_dates, ["hire_date", "salary_date"])
        .pipe(
            standardize_desc_cols, ["rank_desc", "department_desc", "employment_status"]
        )
        .pipe(clean_rank)
        .pipe(set_values, {"agency": "Slidell PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


def fill_empty_columns_for_pprr_2009(df09, df19):
    for _, row in df19.drop_duplicates(subset=["uid"]).iterrows():
        for col in ["badge_no", "department_desc", "employment_status"]:
            df09.loc[df09.uid == row.uid, col] = row[col]
    return df09


def clean_csd_pprr():
    return (
        pd.read_csv(dirk.data("raw/slidell_pd/slidell_csd_pprr_2010_2019.csv"))
        .dropna(how="all")
        .pipe(clean_column_names)
        .rename(
            columns={
                "frequency": "salary_freq",
                "title": "rank_desc",
                "effective": "salary_date",
                "pay": "salary",
            }
        )
        .replace(
            {
                "salary_freq": {
                    "Hourly": salary.HOURLY,
                    "Daily": salary.DAILY,
                    "Monthly": salary.MONTHLY,
                    "Bi-Weekly": salary.BIWEEKLY,
                }
            }
        )
        .pipe(clean_salaries, ["salary"])
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(zero_pad_dates, ["hire_date", "salary_date", "termination_date"])
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_rank)
        .pipe(set_values, {"agency": "Slidell PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


if __name__ == "__main__":
    # df09 = clean_pprr_09()
    # df19 = clean_pprr_19()
    df_csd = clean_csd_pprr()
    # df09.to_csv(dirk.data(
    #     'clean/pprr_slidell_pd_2009.csv'
    # ), index=False)
    # df19.to_csv(dirk.data(
    #     'clean/pprr_slidell_pd_2019.csv'
    # ), index=False)
    df_csd.to_csv(dirk.data("clean/pprr_slidell_csd_2010_2019.csv"), index=False)
