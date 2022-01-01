import sys

sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid


def split_name(df):
    names = df.deputy.str.lower().str.strip().str.extract(r"(\w+) (.+)")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1].str.replace(r"(\,|\.)", "", regex=True)
    return df.drop(columns="deputy")


def clean():
    df = (
        pd.read_csv(data_file_path("raw/cameron_so/cameron_so_cprr_2020.csv"))
        .pipe(clean_column_names)
        .pipe(split_name)
        .rename(
            columns={
                "complaint": "allegation",
                "outcome_after_investigation": "disposition",
            }
        )
        .pipe(set_values, {"agency": "Cameron SO"})
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "allegation", "disposition"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path("clean/cprr_cameron_so_2020.csv"), index=False)
