import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_dates,
    float_to_int_str,
    standardize_desc,
    standardize_desc_cols,
)
from lib.uid import gen_uid


def strip_leading_commas(df):
    for col in df.columns:
        df = df.apply(lambda x: x.str.replace(r"\'", "", regex=True))
    return df


def filter_nopd_cases(df):
    df = df[df.case_name.str.contains("Police")]
    return df


def clean_case_names(df):
    df.loc[:, "case_name"] = df.case_name.str.replace(
        r"Nestor James Police", "Nestor James v. Police", regex=True
    )
    return df


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/new_orleans_pd/new_orleans_pd_state_settlements.csv")
        )
        .pipe(clean_column_names)
        .drop(columns=["row", "payee"])
        .rename(columns={"final_judgment_date": "final_judgement_date"})
        .pipe(strip_leading_commas)
        .pipe(filter_nopd_cases)
        .pipe(clean_case_names)
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(
            gen_uid,
            ["case_number", "case_name", "original_judgment_principal"],
            "settlement_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/settlements_new_orleans_pd.csv"), index=False)
