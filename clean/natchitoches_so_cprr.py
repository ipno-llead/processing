import pandas as pd
from lib.clean import clean_dates, standardize_desc_cols, clean_races, clean_sexes
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
import deba


def strip_leading_commas(df):
    for col in df.columns:
        df = df.apply(lambda col: col.str.replace(r"^\'", "", regex=True))
    return df


def format_dates(df):
    df.loc[:, "receive_date"] = (
        df.date_complaint_received.str.lower()
        .str.strip()
        .str.replace(r"2020 july 07", "07/07/2020", regex=False)
        .str.replace(r"2020 october 30", "10/30/2020", regex=False)
    )

    df.loc[:, "disposition_date"] = (
        df.investigation_disposition_date.str.lower()
        .str.strip()
        .str.replace(r"2020 july 12", "07/12/2020", regex=False)
        .str.replace(r"2020 november 02", "11/2/2020", regex=False)
    )
    return df.drop(
        columns=["date_complaint_received", "investigation_disposition_date"]
    )


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower().str.strip().str.replace(r"\/", ";", regex=True)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.investigation_disposition.str.lower()
        .str.strip()
        .str.replace(r"n\/a", "", regex=True)
    )
    return df.drop(columns=["investigation_disposition"])


def clean_action(df):
    df.loc[:, "action"] = (
        df.disciplinary_action.str.lower()
        .str.strip()
        .str.replace(r"n\/a", "", regex=True)
    )
    return df.drop(columns=["disciplinary_action"])


def clean_split_names(df):
    names = (
        df.deputy_s_name.str.lower()
        .str.strip()
        .str.extract(r"(dy)\. (\w+) ?(\w\.)? (\w+)$")
    )

    df.loc[:, "rank_desc"] = names[0].str.replace(r"dy", "deputy", regex=False)
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    df.loc[:, "last_name"] = names[3]
    return df.drop(columns=["deputy_s_name"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/so_natchitoches/natchitoches_so_cprr_2018_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"investigation_notes": "investigation_desc"})
        .pipe(strip_leading_commas)
        .pipe(format_dates)
        .pipe(clean_allegation)
        .pipe(clean_disposition)
        .pipe(clean_action)
        .pipe(clean_split_names)
        .pipe(clean_dates, ["receive_date", "disposition_date"])
        .pipe(standardize_desc_cols, ["allegation", "investigation_desc"])
        .pipe(set_values, {"agency": "Natchitoches SO"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "disposition", "action"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_natchitoches_so_2018_21.csv"), index=False)
