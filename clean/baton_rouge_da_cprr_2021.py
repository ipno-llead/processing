from lib.columns import clean_column_names
from lib.path import data_file_path
from lib.clean import clean_names, standardize_desc_cols
from lib.uid import gen_uid
import pandas as pd
import sys

sys.path.append("../")


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.rule_violaton.str.lower()
        .str.strip()
        .str.replace("sustained ", "", regex=False)
        .str.replace("arrested and/or convicted of a ", "", regex=False)
    )
    return df


def extract_disposition(df):
    df.loc[:, "disposition"] = (
        df.rule_violaton.str.lower()
        .str.strip()
        .str.replace(" finding of untruthfulness", "", regex=False)
        .str.replace(" of a criminal violation", "", regex=False)
    )
    return df.drop(columns="rule_violaton")


def clean():
    df = (
        pd.read_csv(data_file_path("raw/baton_rouge_da/baton_rouge_da_cprr_2021.csv"))
        .pipe(clean_column_names)
        .pipe(clean_allegations)
        .pipe(extract_disposition)
    )
    return (
        df.pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(standardize_desc_cols, ["status"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["uid", "allegation", "disposition"], "allegation_uid")
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path("clean/cprr_baton_rouge_da_2021.csv"), index=False)
