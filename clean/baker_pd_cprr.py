import deba
import pandas as pd
from lib.columns import clean_column_names
from lib.uid import gen_uid
from lib.columns import set_values
from lib.clean import standardize_desc_cols


def split_names(df):
    names = df.officer_name.str.lower().str.strip().str.extract(r"(\w+)\. (\w+) (\w+)")

    df.loc[:, "rank_desc"] = (
        names[0]
        .fillna("")
        .str.replace(r"\.", "", regex=True)
        .str.replace("ofc", "officer", regex=False)
        .str.replace("det", "detective", regex=False)
        .str.replace("lt", "lieutenant", regex=False)
        .str.replace("sgt", "sergeant", regex=False)
        .str.replace("capt", "captain", regex=False)
    )
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "last_name"] = names[2].fillna("")
    return df.drop(columns="officer_name")[
        ~((df.first_name == "") & (df.last_name == ""))
    ]


def join_disposition_columns20(df):
    df.loc[:, "disposition"] = (
        df.sustained.str.lower()
        .str.strip()
        .fillna("")
        .str.cat(df.not_sustained.str.lower().str.strip().fillna(""))
    )

    df.loc[:, "disposition"] = (
        df.disposition.str.replace(
            r"resigned his employment prior to investigation being performed\.",
            "resigned",
            regex=True,
        )
        .str.replace("yes", "sustained", regex=False)
        .str.replace("no", "not sustained", regex=False)
    )
    return df.drop(columns=["sustained", "not_sustained"])


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegations_complaints.str.lower()
        .str.strip()
        .str.replace(r"^z ", "", regex=True)
        .str.replace("; z", ";", regex=False)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\bdept\b", "department", regex=True)
        .str.replace(r"\badmin\b", "administration", regex=True)
        .str.replace(
            "cooperation with other police agencies - (failure to)",
            "failure to cooperate with other police agencies",
            regex=False,
        )
        .str.replace("zcooperation", "cooperation", regex=False)
        .str.replace(
            "presenting statement of fact - (falsified)",
            "false presentation of statment of fact",
            regex=False,
        )
        .str.replace(
            "use of force - (excessive)", "excessive use of force", regex=False
        )
        .str.replace(r"\bperf\b", "perform", regex=True)
        .str.replace(r"\breq\b", "required", regex=True)
        .str.replace(
            r"use of force -\( duty weapon\) on “pitbull”",
            "use of force (duty weapon) on pitbull",
            regex=True,
        )
        .str.replace(r"\bunauth\b", "unauthorized", regex=True)
        .str.replace(r"\bdam\b", "damaged", regex=True)
        .str.replace(r"^failed - ", "failed ", regex=True)
        .str.replace(r"^misconduct; ", "", regex=True)
        .str.replace(r"(\w+) \/ (\w+)", r"\1/\2", regex=True)
    )
    return df.drop(columns="allegations_complaints")


def clean_dispositions17(df):
    df.loc[:, "not_sustained"] = (
        df.not_sustained.astype(str)
        .str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^yes$", "not sustained", regex=True)
        .str.replace(r"^nan$", "", regex=True)
    )

    df.loc[:, "unknown"] = (
        df.unknown.astype(str)
        .str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^yes$", "unknown", regex=True)
        .str.replace(r"^nan$", "", regex=True)
    )

    df.loc[:, "sustained"] = (
        df.sustained.astype(str)
        .str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^yes$", "sustained", regex=True)
        .str.replace(r"^nan$", "", regex=True)
    )

    unknowns = df.unknown.str.extract(r"(unknown)")
    not_sustained = df.not_sustained.str.extract(r"(not sustained)")
    sustained = df.sustained.str.extract(r"(sustained)")

    df.loc[:, "disposition_1"] = unknowns[0]
    df.loc[:, "disposition_2"] = not_sustained[0]
    df.loc[:, "disposition_3"] = sustained[0]

    df.loc[:, "disposition"] = df.disposition_1.fillna("").str.cat(
        df.disposition_2.fillna(""), sep=""
    )

    df.loc[:, "disposition"] = df.disposition.str.cat(
        df.disposition_3.fillna(""), sep=""
    )

    return df.drop(
        columns=[
            "disposition_1",
            "disposition_2",
            "disposition_3",
            "unknown",
            "sustained",
            "not_sustained",
        ]
    )


def split_rows_with_multiple_allegations(df):
    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split(";", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def clean20():
    df = (
        pd.read_csv(deba.data("raw/baker_pd/baker_pd_cprr_2018_2020.csv"))
        .pipe(clean_column_names)
        .pipe(clean_allegation)
        .pipe(join_disposition_columns20)
        .pipe(split_names)
        .pipe(set_values, {"agency": "baker-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "disposition", "allegation"], "allegation_uid")
        .drop_duplicates(subset=["allegation_uid"])
    )
    return df


def clean17():
    df = (
        pd.read_csv(
            deba.data("raw/baker_pd/baker_pd_cprr_2014_2017.csv"), encoding="cp1252"
        )
        .pipe(clean_column_names)
        .pipe(split_names)
        .pipe(clean_allegation)
        .pipe(split_rows_with_multiple_allegations)
        .pipe(clean_dispositions17)
        .pipe(standardize_desc_cols, ["allegation"])
        .pipe(set_values, {"agency": "baker-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "disposition"], "allegation_uid")
        .drop_duplicates(subset=["allegation_uid"])
    )

    return df


if __name__ == "__main__":
    df20 = clean20()
    df17 = clean17()
    df20.to_csv(deba.data("clean/cprr_baker_pd_2018_2020.csv"), index=False)
    df17.to_csv(deba.data("clean/cprr_baker_pd_2014_2017.csv"), index=False)
