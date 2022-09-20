import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid


def strip_leading_commas(df):
    for col in df.columns:
        df = df.apply(lambda col: col.replace(r"^\'", "", regex=True))
    return df


def fix_dates(df):
    df.loc[:, "check_date"] = df.check_date.str.replace(r"-", "/", regex=False)
    return df


def strip_special_char(df):
    df.loc[:, "settlement_amount"] = df.settlement_amount.str.replace(
        r"^\$", "", regex=True
    )
    return df


def clean():
    df = (
        pd.read_csv(
            deba.data(
                "raw/louisiana_state_pd/louisiana_state_pd_settlements_2015_2020.csv"
            )
        )
        .pipe(clean_column_names)
        .rename(columns={"acct_name": "agency", "year_paid": "pay_year"})
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(strip_leading_commas)
        .pipe(fix_dates)
        .pipe(strip_special_char)
        .pipe(standardize_desc_cols, ["claim_status", "settlement_amount"])
        .pipe(
            gen_uid, ["settlement_amount", "check_date", "pay_year"], "settlement_uid"
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(
        deba.data("clean/settlements_louisiana_state_pd_2015_2020.csv"), index=False
    )
