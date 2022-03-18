from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_names, standardize_desc_cols
from lib.uid import gen_uid
import pandas as pd


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


def clean_agency(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
        .fillna("")
        .str.replace("Baton Rouge Police Department", "Baton Rouge PD", regex=False)
        .str.replace("East Baton Rouge Sheriff's Office", "Baton Rouge SO", regex=False)
        .str.replace("Louisiana State Police", "Louisiana State PD", regex=False)
        .str.replace(
            "Louisiana State University Police Department",
            "LSU University PD",
            regex=False,
        )
    )
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/baton_rouge_da/baton_rouge_da_cprr_2021.csv"))
        .pipe(clean_column_names)
        .pipe(clean_allegations)
        .pipe(extract_disposition)
        .pipe(set_values, {"source_agency": "Baton Rouge DA"})
        .rename(columns={"status": "action"})
    )
    return (
        df.pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(standardize_desc_cols, ["action"])
        .pipe(clean_agency)
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["uid", "source_agency"], "brady_uid")
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/brady_baton_rouge_da_2021.csv"), index=False)
