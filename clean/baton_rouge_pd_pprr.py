from lib.uid import gen_uid
from lib.clean import clean_names

import pandas as pd

from lib.columns import clean_column_names, set_values
import deba


def split_names(df):
    names = (
        df.name.str.strip()
        .str.lower()
        .str.replace(r", (\w) (\w+)$", r", \2 \1", regex=True)
        .str.replace(r"(.+),(.+)( \w{2}\.)$", r"\1\3,\2", regex=True)
        .str.extract(r"^([^,]+), ([^ ]+)(?: (\w\.?))?$")
    )
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    return df.drop(columns="name")


def replace_rank(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.strip()
        .str.lower()
        .replace(
            {
                "cpl": "corporal",
                "off": "officer",
                "sgt": "sergeant",
                "ofc": "officer",
                "lt": "lieutenant",
                "dep. chief": "deputy chief",
                "capt": "captain",
                "mjr": "major",
                "dpty. chief": "deputy chief",
                "cpt": "captain",
            }
        )
    )
    return df


def clean_badge_no(df):
    df.loc[:, "badge_no"] = (
        df.badge_no.fillna("").str.strip().str.replace(r"\*+", "", regex=True)
    )
    return df


def clean():
    return (
        pd.read_csv(deba.data("raw/baton_rouge_pd/baton_rouge_pd_pprr_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "badge": "badge_no",
                "rank": "rank_desc",
            }
        )
        .pipe(replace_rank)
        .pipe(clean_badge_no)
        .pipe(split_names)
        .pipe(clean_names, ["last_name", "first_name", "middle_name"])
        .pipe(set_values, {"data_production_year": "2021", "agency": "baton-rouge-pd"})
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
    )


if __name__ == "__main__":
    df = clean()

    df.to_csv(deba.data("clean/pprr_baton_rouge_pd_2021.csv"), index=False)
