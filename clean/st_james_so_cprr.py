import pandas as pd
import deba
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols, clean_ranks
from lib.rows import duplicate_row
import re


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(" all deputies", "", regex=False)
    )
    return df


def split_rows_w_multiple_officers(df):
    df.loc[:, "deputy_ies"] = (
        df.deputy_ies.str.lower()
        .str.strip()
        .str.replace(r"\.", "", regex=True)
        .str.replace(r" dty (\w+)", r" | dty \1", regex=True)
        .str.replace(r" sgt (\w+)", r" | sgt \1", regex=True)
    )

    i = 0
    for idx in df[df.deputy_ies.str.contains(" \| ")].index:
        s = df.loc[idx + i, "deputy_ies"]
        parts = re.split(r"\|", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "deputy_ies"] = name
        i += len(parts) - 1
    return df


def split_officer_names(df):
    names = df.deputy_ies.str.replace(r"^\'", "", regex=True).str.extract(
        r"(de?p?u?ty|\bsgt\b|lt|capt) (\w+) (\w+) ?(\w+)?"
    )

    df.loc[:, "rank_desc"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "last_name"] = names[2].fillna("")
    df.loc[:, "suffix"] = names[3].fillna("")
    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["deputy_ies", "suffix"])


def clean_action(df):
    df.loc[:, "action"] = (
        df.disciplinary_action.str.lower()
        .str.strip()
        .str.replace("none", "", regex=False)
    )
    return df.drop(columns=["disciplinary_action"])


def clean21():
    df = (
        pd.read_csv(deba.data("raw/st_james_so/st_james_so_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "receive_date", "nature_of_complaint": "allegation"})
        .pipe(clean_dates, ["receive_date"])
        .pipe(clean_disposition)
        .pipe(split_rows_w_multiple_officers)
        .pipe(split_officer_names)
        .pipe(clean_action)
        .pipe(clean_ranks, ["rank_desc"])
        .pipe(standardize_desc_cols, ["allegation"])
        .pipe(set_values, {"agency": "st-james-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "action", "disposition", "uid"], "allegation_uid")
    )

    return df


def clean00(df):
    df = pd.read_csv(deba.data("raw/st_james_so/st_james_so_cprr_1990_2000.csv"))
    return df 

if __name__ == "__main__":
    df = clean21()
    df.to_csv(deba.data("clean/cprr_st_james_so_2019_2021.csv"), index=False)
