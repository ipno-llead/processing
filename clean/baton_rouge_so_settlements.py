import pandas as pd
import deba
from lib.columns import set_values
from lib.uid import gen_uid


def process_dataframe(dataframe):
    new_header = dataframe.iloc[0]
    dataframe = dataframe[1:]
    dataframe.columns = new_header

    dataframe.dropna(axis=1, how="all", inplace=True)

    return dataframe


def strip_col(df):
    for col in df.columns:
        df[col] = df[col].str.replace(r"^\'", "")
    return df


def strip_sheriff_str(df):
    df.loc[:, "officer"] = (
        df.officer.str.lower().str.strip().str.replace(r"^sheriff\, ?", "", regex=True)
    )
    return df[~((df.officer == ""))]


def split_rows_with_multiple_officers(df):
    df = (
        df.drop("officer", axis=1)
        .join(
            df["officer"]
            .str.split(";", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("officer"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def clean_amount(df):
    df.loc[:, "amount"] = df.amount.str.replace(r"\$", "", regex=True).str.replace(r"\.(.+)", "", regex=True).str.replace(r"\,", "", regex=True)
    return df 

def split_names(df):
    names = df.officer.str.replace(r"(\,|\.)", "", regex=True).str.extract(r"(.+) (\w+)$")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]

    ranks = df.first_name.str.extract(r"(deputy|warden\,|detective)")

    df.loc[:, "rank_desc"] = ranks[0]
    df.loc[:, "first_name"] = df.first_name.str.replace(r"(deputy|warden\,|detective)", "", regex=True)
    return df.drop(columns=["officer"])


def clean():
    df = pd.read_csv(deba.data("raw/baton_rouge_so/east-baton-rouge-so-settlements-2021-2023.csv"))
    df = (
        df.pipe(process_dataframe)
        .rename(
            columns={
                "'Defendants": "officer",
                "'Total Amount Paid by Sheriff": "amount",
                "'Date": "claim_close_date",
            }
        )
        .pipe(strip_col)
        .pipe(strip_sheriff_str)
        .pipe(split_rows_with_multiple_officers)
        .pipe(clean_amount)
        .pipe(split_names)
        .pipe(set_values, {"agency": "east-baton-rouge-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["amount", "claim_close_date"], "settlement_uid")
    )
    return df

if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/settlements_baton_rouge_so_2021_2023.csv"), index=False)