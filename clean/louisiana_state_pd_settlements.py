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
    df.loc[:, "check_date"] = (
        df.check_date.str.replace(r"-", "/", regex=False)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
    )
    return df


def strip_special_char(df):
    df.loc[:, "settlement_amount"] = df.settlement_amount.str.replace(
        r"^\$", "", regex=True
    )
    return df


def split_rows_with_multiple_officers(df):
    df.loc[:, "employees_involved"] = df.employees_involved.str.replace(
        r" (\(.+\))", "", regex=True
    )
    df = (
        df.drop("employees_involved", axis=1)
        .join(
            df["employees_involved"]
            .str.split(",", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("employees_involved"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df[~((df.employees_involved.fillna("") == ""))]


def split_names(df):
    names = df.employees_involved.str.lower().str.strip().str.extract(r"(\w+) (\w+)")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns=["employees_involved"])[~(df.last_name.fillna("") == "")]


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/louisiana_state_pd/louisiana_state_pd_settlements_2020.csv")
        )
        .pipe(clean_column_names)
        .rename(
            columns={
                "acct_name": "agency",
                "year_paid": "pay_year",
                "payment_amount": "settlement_amount",
            }
        )
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(strip_leading_commas)
        .pipe(fix_dates)
        .pipe(strip_special_char)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_names)
        .pipe(standardize_desc_cols, ["claim_status", "settlement_amount"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["settlement_amount", "check_date"], "settlement_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(
        deba.data("clean/settlements_louisiana_state_pd_2015_2020.csv"), index=False
    )
