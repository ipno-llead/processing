import pandas as pd

from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_names, clean_salaries, clean_dates, standardize_desc_cols, strip_leading_comma
from lib.uid import gen_uid
from lib import salary


def split_names(df):
    names = (
        df.name.fillna("")
        .str.lower()
        .str.strip()
        .str.replace(r",? (iii|iv|v|jr\.?),?", r" \1,", regex=True)
        .str.replace(r" +\.$", "", regex=True)
        .str.replace(r"(\w{3,})\. (\w+)", r"\1, \2", regex=True)
        .str.extract(r"^([^,]+), (\w+)(?: (\w+))?$")
    )
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    df.loc[:, "last_name"] = names[0]
    return df[df.name.notna()].reset_index(drop=True).drop(columns=["name"])


def assign_agency(df):
    df.loc[:, "data_production_year"] = 2018
    df.loc[:, "agency"] = "gretna-pd"
    return df


def clean_rank_desc(df):
    df.rank_desc = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.lstrip("'")
        .str.replace(r" \(d.\)$", "", regex=True)
        .str.replace(r"sergeant ?(officer)?", "sargeant", regex=True)
    )
    return df


def clean():
    return (
        pd.read_csv(deba.data("raw/gretna_pd/gretna_pd_pprr_2018.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "rank": "rank_desc",
                "2018_salary": "salary",
            }
        )
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_rank_desc)
        .pipe(assign_agency)
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
    )
def assign_agency_21(df):
    df.loc[:, "data_production_year"] = 2021
    df.loc[:, "agency"] = "gretna-pd"
    return df

def clean_21():
    return (
        pd.read_csv(deba.data("raw/gretna_pd/gretna_pd_pprr_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "rank": "rank_desc",
                "base_annual_pay": "salary",
            }
        )
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_rank_desc)
        .pipe(assign_agency_21)
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
    )

def assign_agency_22(df):
    df.loc[:, "data_production_year"] = 2022
    df.loc[:, "agency"] = "gretna-pd"
    return df

def clean_22():
    return (
        pd.read_csv(deba.data("raw/gretna_pd/gretna_pd_pprr_2022.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "rank": "rank_desc",
                "base_annual_pay": "salary",
            }
        )
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_rank_desc)
        .pipe(assign_agency_22)
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
    )

def assign_agency_23(df):
    df.loc[:, "data_production_year"] = 2023
    df.loc[:, "agency"] = "gretna-pd"
    return df

def clean_23():
    return (
        pd.read_csv(deba.data("raw/gretna_pd/gretna_pd_pprr_2023.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "rank": "rank_desc",
                "base_annual_pay": "salary",
            }
        )
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_rank_desc)
        .pipe(assign_agency_23)
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
    )

def assign_agency_24(df):
    df.loc[:, "data_production_year"] = 2024
    df.loc[:, "agency"] = "gretna-pd"
    return df

def clean_24():
    return (
        pd.read_csv(deba.data("raw/gretna_pd/gretna_pd_pprr_2024.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "rank": "rank_desc",
                "base_annual_pay": "salary",
            }
        )
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_rank_desc)
        .pipe(assign_agency_24)
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
    )

def assign_agency_25(df):
    df.loc[:, "data_production_year"] = 2025
    df.loc[:, "agency"] = "gretna-pd"
    return df

def clean_25():
    return (
        pd.read_csv(deba.data("raw/gretna_pd/gretna_pd_pprr_2025.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "rank": "rank_desc",
                "base_annual_pay": "salary",
            }
        )
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(clean_salaries, ["salary"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(clean_rank_desc)
        .pipe(assign_agency_25)
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
    )


def combine_and_deduplicate():
    """Combine all years 2021-2025 and deduplicate based on uid and hire_date.

    When duplicates exist, keeps the most recent entry (highest data_production_year).
    Deduplicates on both uid alone AND uid+hire_date to prevent duplicate events.
    """
    dfs = [
        clean_21(),
        clean_22(),
        clean_23(),
        clean_24(),
        clean_25()
    ]

    combined = pd.concat(dfs, ignore_index=True)

    combined = combined.sort_values("data_production_year", ascending=False)

    # First deduplicate by uid (keeps most recent data for each officer)
    combined = combined.drop_duplicates(subset=["uid"], keep="first")

    # Then deduplicate by uid + hire_date to prevent duplicate OFFICER_HIRE events
    # (in case same officer appears in multiple files with same hire date)
    combined = combined.drop_duplicates(
        subset=["uid", "hire_year", "hire_month", "hire_day"],
        keep="first"
    )

    return combined.reset_index(drop=True)


if __name__ == "__main__":
    df = clean()
    df_21_25 = combine_and_deduplicate()
    df.to_csv(deba.data("clean/pprr_gretna_pd_2018.csv"), index=False)
    df_21_25.to_csv(deba.data("clean/pprr_gretna_pd_2021_2025.csv"), index=False)
