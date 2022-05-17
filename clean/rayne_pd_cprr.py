import pandas as pd
import deba

from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, clean_names
from lib.uid import gen_uid
import re
from lib.rows import duplicate_row


def split_rows_with_multiple_officers(df):
    df.loc[:, "focus_officer"] = (
        df.focus_officer.str.lower()
        .str.strip()
        .str.replace(r"^1\-", "", regex=True)
        .str.replace(r"(2\-?|3\-?|4\-?)", "/", regex=True)
    )
    df = (
        df.drop("focus_officer", axis=1)
        .join(
            df["focus_officer"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("focus_officer"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_officer_names(df):
    names = df.focus_officer.str.extract(r"(ptl|pco|sgt|pfc|ptl)\. (\w+) (\w+)")
    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns="focus_officer")


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .str.replace(r"\-", "", regex=True)
        .str.replace(r"officer (.+)", r"officer; \1", regex=True)
    )
    return df.drop(columns="complaint")


def split_investiator_names(df):
    names = (
        df.assigned_investigator.str.lower()
        .str.strip()
        .str.extract(r"(captain) (\w+) (\w+)")
    )

    df.loc[:, "investigator_rank_desc"] = names[0]
    df.loc[:, "investigator_first_name"] = names[1]
    df.loc[:, "investigator_last_name"] = names[2]
    return df.drop(columns="assigned_investigator")


def fix_disposition(df):
    df.loc[
        (df.last_name == "jonson") & (df.allegation == "vehicle pursuit policy"),
        "disposition",
    ] = ""

    df.loc[
        (df.last_name == "cole") & (df.allegation == "vehicle pursuit policy"),
        "disposition",
    ] = ""

    df.loc[
        (df.last_name == "pellerin") & (df.allegation == "vehicle pursuit policy"),
        "disposition",
    ] = ""

    df.loc[
        (df.last_name == "credeur")
        & (df.allegation == "conduct unbecoming of officer; vehicle pursuit policy"),
        "disposition",
    ] = "sustained, 3-day suspension"

    df.loc[
        (df.last_name == "trahan")
        & (df.allegation == "conduct unbecoming of officer; vehicle pursuit policy"),
        "disposition",
    ] = "sustained, 14-day suspension and demotion"

    df.loc[
        (df.last_name == "istre")
        & (df.allegation == "conduct unbecoming of officer; vehicle pursuit policy"),
        "disposition",
    ] = "exonerated"

    return df


def extract_action(df):
    df.loc[:, "action"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"sgt\. joseph credeur ", "", regex=True)
        .str.replace(" on actions", "", regex=False)
        .str.replace(r"(exonerated|unfounded|sustained\,? ?)", "", regex=True)
        .str.replace("suspended- 3 days", "3-day suspension", regex=False)
        .str.replace("suspension and demotion", "suspension; demotion", regex=False)
    )
    return df


def clean_disposition(df):
    dispositions = (
        df.disposition.str.lower()
        .str.strip()
        .str.extract(r"(exonerated|sustained\,?|unfounded|resigned)")
    )

    df.loc[:, "disposition"] = dispositions[0].fillna("")
    return df


def clean_rank(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.replace(r"^ptl$", "patrol officer", regex=True)
        .str.replace(r"^pfc$", "patrol officer first class", regex=True)
        .str.replace(r"^sgt$", "sergeant", regex=True)
        .str.replace(r"^pco$", "patrol communications officer", regex=True)
    )
    return df


def strip_leading_commas(df):
    for col in df.columns:
        df[col] = df[col].str.replace(r"^\'", "", regex=True)
    return df


def split_names14(df):
    names = (
        df.focus_officer.str.lower()
        .str.strip()
        .str.replace(r"(1\)|2\)|3\)|4\)) ", "", regex=True)
        .str.extract(r"(sgt|ptl|pfc)?\.? ?(\w+) (\w+)")
    )

    df.loc[:, "rank_desc"] = (
        names[0]
        .fillna("")
        .str.replace(r"sgt", "sergeant", regex=False)
        .str.replace(r"ptl", "patrol", regex=False)
    )
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "last_name"] = names[2].fillna("")
    return df.drop(columns=["focus_officer"])


def split_rows_w_multiple_allegations14(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .str.replace(r"^(a\)|1\)) ", "", regex=True)
        .str.replace(r"(b\)|c\)|1\)|2\)|3\))", "|", regex=True)
    )

    i = 0
    for idx in df[df.allegation.str.contains(" | ")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:\|)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df.drop(columns=["complaint"])


def extract_allegation_desc(df):
    allegation_desc = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"(1\)|\(a\)|\(b\)|\(c\))", "", regex=True)
        .str.extract(r"\((.+)\)")
    )

    df.loc[:, "allegation_desc"] = allegation_desc[0].fillna("")
    return df


def assign_dispositions14(df):
    df.loc[:, "disposition"] = df.disposition.str.replace(
        r"^a\) sustained b\) sustained ", "sustained", regex=True
    )

    df.loc[
        ((df.last_name == "miller") & (df.allegation == "excessive force")),
        "disposition",
    ] = "unfounded"
    df.loc[
        ((df.last_name == "miller") & (df.allegation == "standards of conduct")),
        "disposition",
    ] = "sustained"
    df.loc[
        (
            (df.last_name == "miller")
            & (df.allegation == "improper or lack of supervision")
        ),
        "disposition",
    ] = "sustained"

    df.loc[
        ((df.last_name == "abshire") & (df.allegation == "excessive force")),
        "disposition",
    ] = "unfounded"
    df.loc[
        ((df.last_name == "abshire") & (df.allegation == "standards of conduct")),
        "disposition",
    ] = "sustained"
    df.loc[
        (
            (df.last_name == "abshire")
            & (df.allegation == "improper or lack of supervision")
        ),
        "disposition",
    ] = ""
    return df


def clean_disposition14(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"(2\)|1\))", "", regex=True)
        .str.replace(r" ", "", regex=False)
    )
    return df


def split_investigator_names14(df):
    names = (
        df.assigned_investigator.str.lower()
        .str.strip()
        .str.replace(r" +$", "", regex=True)
        .str.extract(r"(\w+)\.? (\w+) (\w+)")
    )

    df.loc[:, "investigator_rank_desc"] = names[0].str.replace(
        r"capt\.", "captain", regex=True
    )

    df.loc[:, "investigator_first_name"] = names[1]
    df.loc[:, "investigator_last_name"] = names[2]
    return df.drop(columns=["assigned_investigator"])


def clean20():
    df = (
        pd.read_csv(deba.data("raw/rayne_pd/rayne_pd_cprr_2019_2020.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "date_received": "receive_date",
                "date_completed": "investigation_complete_date",
            }
        )
        .pipe(clean_allegation)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_officer_names)
        .pipe(split_investiator_names)
        .pipe(fix_disposition)
        .pipe(extract_action)
        .pipe(clean_disposition)
        .pipe(clean_rank)
        .pipe(
            clean_names,
            [
                "first_name",
                "last_name",
                "investigator_first_name",
                "investigator_last_name",
            ],
        )
        .pipe(clean_dates, ["receive_date", "investigation_complete_date"])
        .pipe(set_values, {"agency": "Rayne PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["investigator_first_name", "investigator_last_name", "agency"],
            "investigator_uid",
        )
        .pipe(gen_uid, ["uid", "allegation", "disposition", "action"], "allegation_uid")
    )
    return df


def clean14():
    df = (
        pd.read_csv(deba.data("raw/rayne_pd/rayne_pd_cprr_2014_2018.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "date_received": "receive_date",
                "date_completed": "investigation_complete_date",
            }
        )
        .pipe(clean_dates, ["receive_date", "investigation_complete_date"])
        .pipe(strip_leading_commas)
        .pipe(split_names14)
        .pipe(split_rows_w_multiple_allegations14)
        .pipe(extract_allegation_desc)
        .pipe(assign_dispositions14)
        .pipe(clean_disposition14)
        .pipe(split_investigator_names14)
        .pipe(set_values, {"agency": "Rayne PD"})
        .pipe(
            gen_uid,
            ["investigator_first_name", "investigator_last_name", "agency"],
            "investigator_uid",
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["allegation", "disposition", "allegation_desc", "uid"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df20 = clean20()
    df14 = clean14()
    df20.to_csv(deba.data("clean/cprr_rayne_pd_2019_2020.csv"), index=False)
    df14.to_csv(deba.data("clean/cprr_rayne_pd_2014_2018.csv"), index=False)
