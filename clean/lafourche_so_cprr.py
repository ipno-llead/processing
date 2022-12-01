import pandas as pd
import deba
from lib.clean import (
    clean_sexes,
    standardize_desc_cols,
    clean_dates,
    clean_races,
    clean_names,
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.rows import duplicate_row
import re


def split_rows_with_multiple_officers(df):
    i = 0
    for idx in df[df.officer.str.contains("/")].index:
        s = df.loc[idx + i, "officer"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1
    return df


def split_officer_names(df):
    names = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"^lpcc$", "", regex=True)
        .str.replace(r"lt\.", "lieutenant", regex=True)
        .str.replace(r"(\w+) $", "", regex=True)
        .str.extract(r"(lieutenant)? ?(\w+) ?(\w{1})? (.+)")
    )

    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "middle_name"] = names[2]
    df.loc[:, "last_name"] = names[3].fillna("")
    return df.drop(columns=["officer"])


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean_receive_date(df):
    df.loc[:, "receive_date"] = (
        df.date.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"^carry over for 2020$", "", regex=True)
    )
    return df.drop(columns=["date"])


def extract_race_and_sex_columns(df):
    races = df.race_sex_office.str.lower().str.strip().str.extract(r"(^b|^w)")
    df.loc[:, "race"] = (
        races[0]
        .fillna("")
        .str.replace(r"^b$", "black", regex=True)
        .str.replace(r"^w$", "white", regex=True)
    )

    sexes = df.race_sex_office.str.lower().str.strip().str.extract(r"(f$|m$)")

    df.loc[:, "sex"] = (
        sexes[0]
        .fillna("")
        .str.replace(r"^f$", "female", regex=True)
        .str.replace(r"^m$", "male", regex=True)
    )
    return df.drop(columns=["race_sex_office"])


def extract_disposition(df):
    dispositions = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\bnot sustained\b|sustained|unfounded|\bno violations\b)")
    )

    df.loc[:, "disposition"] = dispositions[0]
    return df


def clean_actions(df):
    df.loc[:, "action"] = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\bterminated\b", "termination", regex=True)
        .str.replace(r"\b40 hrs of susp\b", "40-hour suspension", regex=True)
        .str.replace(r" ?\/ ?", " ", regex=True)
        .str.replace(r"^resignation in lieu$", "resignation", regex=True)
        .str.replace(r"transfer 8 hrs", "8-hour transfer", regex=True)
        .str.replace(
            r"^resigned under investigation",
            "resignation while under investigation",
            regex=True,
        )
        .str.replace("refered", "referred", regex=False)
        .str.replace(
            r"(^abandoned$|^handeled at the division$|no violations|^unfounded$|\bnot sustained\b|\bsustained\b)",
            "",
            regex=True,
        )
    )
    return df


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"art\.?", "article", regex=True)
        .str.replace(r"^terminated$", "", regex=True)
        .str.replace(r"takeing\b", "taking", regex=True)
        .str.replace(r"\bherrasment\b", "harrassment", regex=True)
        .str.replace(r"^no disciplin$", "", regex=True)
        .str.replace(r"^po ", "", regex=True)
        .str.replace(r"^(\w{2})\b", r"article \1", regex=True)
        .str.replace(r"\/", ",", regex=True)
        .str.replace(r"(\w{2})\, (\w{2})", r"\1,\2", regex=True)
        .str.replace(r"^fratinization$", "fraternization", regex=True)
    )
    return df.drop(columns=["outcome"])


def strip_leading_aps(df):
    df = df.apply(lambda x: x.str.replace(r"^\'", "", regex=True))
    return df


def extract_action(df):
    actions = df.outcome.str.lower().str.strip().str.extract(r"(terminated)")

    df.loc[:, "action"] = actions[0].fillna("")
    return df


def clean_disposition15(df):
    df.loc[:, "disposition"] = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(r"resign\b", "resigned", regex=True)
        .str.replace(r"(\/|\,)", "; ", regex=True)
        .str.replace(r";? ?terminated", "", regex=True)
        .str.replace(r"\breff?e?r?e?d?\b", "referred", regex=True)
        .str.replace(r"capt\b", "captain", regex=True)
        .str.replace("accidental discharge", "", regex=False)
        .str.replace("crim", "criminal", regex=False)
        .str.replace("earley", "early", regex=False)
        .str.replace(r"^sus-appeal-.+", "sustained", regex=True)
        .str.replace(r"\bda\b", "district attorney's", regex=True)
        .str.replace("tran", "transferred", regex=False)
        .str.replace(
            r" (before)? ?(prior)? ?(to)? ?com(plete)?",
            " before completion",
            regex=True,
        )
        .str.replace(r"completionmander", "completion", regex=False)
        .str.replace(r"resigned; before", "resigned before", regex=False)
        .str.replace(r"  +", " ", regex=True)
        .str.replace(r"\badm\b", "administrative", regex=True)
        .str.replace(r"\((.+)\)", "", regex=True)
        .str.replace(r"adm rem;", "", regex=False)
        .str.replace(r" +$", "", regex=True)
    )

    return df.drop(columns=["outcome"])


def split_rows_with_multiple_officers15(df):
    df.loc[:, "officer"] = df.officer.str.replace(r"\,", r"/ ", regex=True).str.replace(
        r"See #8.+", "", regex=True
    )

    i = 0
    for idx in df[df.officer.str.contains("/")].index:
        s = df.loc[idx + i, "officer"]
        parts = re.split(r"\/", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1
    return df


def drop_rows_missing_allegations(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower().str.strip().str.replace(r"na", "", regex=True)
    )
    return df[~((df.allegation == ""))]


def split_rows_with_multiple_allegations15(df):
    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split(",", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_names(df):
    names = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"6?-?(lspo|twp\b).+", "", regex=True)
        .str.replace(r"^ +", "", regex=True)
        .str.replace(r" +$", "", regex=True)
        .str.extract(r"(\w+) (\w+) ?(\w+)?")
    )

    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "last_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")
    return df[~((df.first_name == "") & (df.last_name == ""))].drop(columns=["officer"])


def allegation_dict():
    df = pd.read_csv(deba.data("raw/lafourche_so/allegation_dict.csv"))
    df.loc[:, "code"] = df.code.str.lower().str.strip().replace(r"^\. ", "", regex=True)
    df.loc[:, "description"] = (
        df.description.str.lower().str.strip().str.replace(r"^art\. ", "", regex=True)
    )

    allegation_dict = dict(zip(df.code, df.description))
    return allegation_dict


def map_allegation_desc(df):
    allegations = allegation_dict()
    df.loc[:, "allegation"] = df.allegation.map(allegations)
    df.loc[:, "allegation"] = df.allegation.str.replace(r"^ ", "", regex=True)
    return df


def correct_actions(df):
    df.loc[
        (df.last_name == "peterson")
        & (df.disposition == "sustained; resigned prior to discipline"),
        "action",
    ] = "resgined prior to discipline"
    df.loc[
        (df.last_name == "durand") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "fields") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "guidry") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "marulli") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "wintzel") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "bearden") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "guilliot") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "lymous")
        & (df.disposition == "transferred before completion; appeal; resigned"),
        "action",
    ] = "resigned"
    df.loc[
        (df.last_name == "campo") & (df.disposition == "sustained; resigned"), "action"
    ] = "resigned"
    df.loc[
        (df.last_name == "bogle") & (df.disposition == "sustained; resigned"), "action"
    ] = "resigned"
    df.loc[
        (df.last_name == "guidry") & (df.disposition == "sustained; resigned"), "action"
    ] = "resigned"
    df.loc[
        (df.last_name == "jennings") & (df.disposition == "sustained; resigned"),
        "action",
    ] = "resigned"
    df.loc[
        (df.last_name == "kaufman") & (df.disposition == "sustained; resigned"),
        "action",
    ] = "resigned"
    df.loc[
        (df.last_name == "johnson")
        & (df.disposition == "    resigned; refused to cooperate"),
        "action",
    ] = "resigned"
    return df


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def split_rows_with_multiple_officers_10(df):
    df.loc[:, "officer"] = df.officer.str.replace(r"(\,|&)", "/", regex=True)
    i = 0
    for idx in df[df.officer.str.contains("/")].index:
        s = df.loc[idx + i, "officer"]
        parts = re.split(r"\/", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1
    return df


def split_names_10(df):
    names = df.officer.str.lower().str.strip()\
        .str.replace(r"^ (\w+)", r"\1", regex=True)\
        .str.replace(r"(\w+) $", r"\1", regex=True)\
        .str.replace(r"tpd off ricky ross", "ricky ross", regex=False)\
        .str.replace(r"(unknown.+|lps.+|tpd.+|inmate.+|\?|\((\w+)\)|detention.+)", "", regex=True)\
        .str.extract(r"(dty\.?|agt\.?|clerk|lt\.?|sgt\.?|capt\.?")
    df.loc[:, ""]


def clean21():
    df = (
        pd.read_csv(deba.data("raw/lafourche_so/lafourche_so_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"case": "tracking_id"})
        .drop(columns=["complainant", "race_sex"])
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_officer_names)
        .pipe(drop_rows_missing_names)
        .pipe(clean_receive_date)
        .pipe(clean_dates, ["receive_date"])
        .pipe(extract_race_and_sex_columns)
        .pipe(extract_disposition)
        .pipe(clean_actions)
        .pipe(clean_allegation)
        .pipe(standardize_desc_cols, ["action", "tracking_id", "allegation"])
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "lafourche-so"})
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "tracking_id", "allegation", "disposition", "action"],
            "allegation_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


def clean15():
    df = (
        pd.read_csv(deba.data("raw/lafourche_so/lafourche_so_cprr_2015_2018.csv"))
        .pipe(clean_column_names)
        .drop(columns=["ia"])
        .rename(columns={"date": "receive_date", "case": "tracking_id"})
        .pipe(strip_leading_aps)
        .pipe(clean_dates, ["receive_date"])
        .pipe(extract_action)
        .pipe(clean_disposition15)
        .pipe(split_rows_with_multiple_officers15)
        .pipe(split_names)
        .pipe(drop_rows_missing_allegations)
        .pipe(split_rows_with_multiple_allegations15)
        .pipe(map_allegation_desc)
        .pipe(correct_actions)
        .pipe(set_values, {"agency": "lafourche-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "disposition", "action"], "allegation_uid")
        .drop_duplicates(subset=["allegation_uid"], keep="first")
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df



def clean10():
    df = (pd.read_csv(deba.data("raw/lafourche_so/lafourche_so_cprr_2010_2014.csv"))\
        .pipe(clean_column_names)
        .pipe(strip_leading_aps)
        .pipe(split_rows_with_multiple_officers_10)
    )
    return df

if __name__ == "__main__":
    df21 = clean21()
    df15 = clean15()
    df21.to_csv(deba.data("clean/cprr_lafourche_so_2019_2021.csv"), index=False)
    df15.to_csv(deba.data("clean/cprr_lafourche_so_2015_2018.csv"), index=False)
