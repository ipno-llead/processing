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
        .str.replace("Baton Rouge Police Department", "baton-rouge-pd", regex=False)
        .str.replace(
            "East Baton Rouge Sheriff's Office", "east-baton-rouge-so", regex=False
        )
        .str.replace("Louisiana State Police", "louisiana-state-pd", regex=False)
        .str.replace(
            "Louisiana State University Police Department",
            "lsu-university-pd",
            regex=False,
        )
        .str.replace(
            r"Department of Public Safety", "department-of-public-safety", regex=True
        )
    )
    return df


def split_list18(df):
    data = (
        df.brady_list.str.lower()
        .str.strip()
        .str.extract(r"(\w+)\, (\w+) \((\w+ ?\w+?)\) ?-(.+)")
    )

    df.loc[:, "last_name"] = data[0]
    df.loc[:, "first_name"] = data[1]
    df.loc[:, "agency"] = (
        data[2]
        .str.replace(r"brpd", "baton-rouge-pd", regex=False)
        .str.replace(r"ebrso", "east-baton-rouge-so", regex=False)
        .str.replace(r"lsp", "louisiana-state-pd", regex=False)
        .str.replace(r"baker pd", "baker-pd", regex=False)
    )
    df.loc[:, "allegation_desc"] = data[3]
    return df.drop(columns=["brady_list"])


def clean21():
    df = (
        pd.read_csv(deba.data("raw/baton_rouge_da/baton_rouge_da_cprr_2021.csv"))
        .pipe(clean_column_names)
        .pipe(clean_allegations)
        .pipe(extract_disposition)
        .pipe(
            set_values,
            {"source_agency": "east-baton-rouge-da", "brady_list_date": "2/1/2021"},
        )
        .rename(columns={"status": "action"})
    )
    return (
        df.pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(standardize_desc_cols, ["action"])
        .pipe(clean_agency)
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["uid", "source_agency"], "brady_uid")
    )


def clean18():
    df = (
        pd.read_csv(deba.data("raw/baton_rouge_da/brady_east_baton_rouge_da_2018.csv"))
        .pipe(clean_column_names)
        .pipe(split_list18)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["allegation_desc"])
        .pipe(
            set_values,
            {"source_agency": "east-baton-rouge-da", "brady_list_date": "3/1/2018"},
        )
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(gen_uid, ["uid", "source_agency", "allegation_desc"], "brady_uid")
    )
    return df


if __name__ == "__main__":
    df21 = clean21()
    df18 = clean18()
    df21.to_csv(deba.data("clean/brady_baton_rouge_da_2021.csv"), index=False)
    df18.to_csv(deba.data("clean/brady_baton_rouge_da_2018.csv"), index=False)
