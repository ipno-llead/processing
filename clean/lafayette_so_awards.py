import sys

sys.path.append("../")
import pandas as pd
import deba
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values


def split_officer_name(df):
    names = (
        df.deputy_name.str.lower()
        .str.strip()
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\w+) (\w+)")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns=["deputy_name"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/lafayette_so/lafayette_so_awards_2017.csv"))
        .pipe(clean_column_names)
        .rename(columns={"month": "receive_month", "division": "department_desc"})
        .pipe(split_officer_name)
        .pipe(set_values, {"agency": "Lafaytte SO", "receive_day": "1", "receive_year": "2017"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])\
        .pipe(gen_uid, ["uid", "department_desc", "receive_month"], "award_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/award_lafayette_so_2017.csv"), index=False   )
