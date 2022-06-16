import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_names, clean_dates, clean_names
from lib.uid import gen_uid


def split_names(df):
    names = (
        df.name.str.lower()
        .str.strip()
        .str.replace(r"(\w+)\.(\w+)", r"\1\2", regex=True)
        .str.replace(r"(\w+?\'?\.?\w+) \, \'(\w+)", r"\2 \1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\w+) ?(\w+?-?\'?\w+)? ?(iv|los angeles|charles)?")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")

    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["name", "suffix"])


def strip_leading_commas(df):
    for col in df.columns:
        df[col] = df[col].str.replace(r"^\'", "", regex=True)
    return df


def sanitize_dates(df):
    df.loc[:, "left_date"] = df.separation_date.str.replace(
        r"(\w+) $", r"\1", regex=True
    ).str.replace(r"^(\w+)$", "", regex=True)
    df.loc[:, "hire_date"] = df.hire_date.str.replace(
        r"(\w+) $", r"\1", regex=True
    ).str.replace(r"^(\w+)$", "", regex=True)
    return df.drop(columns=["separation_date"])


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.job_title.str.lower().str.strip().str.replace(r" ?police ", " ", regex=True)
    )
    return df.drop(columns=["job_title"])


def clean_left_reason_desc21(df):
    df.loc[:, "left_reason_desc"] = df.reason.str.cat(df.resignation_reason, sep=" ")
    df.loc[:, "left_reason_desc"] = (
        df.left_reason_desc.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"(\w+) +", r"\1 ", regex=True)
        .str.replace(
            r"(deceased|resign(ed)?-?|terminat(ed|ion)|retired|involuntary|dismissal ?-? ?)-? ? ?",
            "",
            regex=True,
        )
        .str.replace(r"better job better job", "better job", regex=False)
        .str.replace(r"(\w+)\((.+)\)", r"\1 \2", regex=True)
        .str.replace(r"unknown other", "unknown/other", regex=False)
        .str.replace(r"oft", "off", regex=True)
        .str.replace(r"obligation\b", "obligations", regex=True)
        .str.replace(r"other (.+)", r"other/\1", regex=True)
        .str.replace(r"better job (.+)", r"better job/\1", regex=True)
        .str.replace(r"personal- (.+)", r"personal/\1", regex=True)
        .str.replace(r"home-(\w+)", r"home \1", regex=True)
        .str.replace(
            r"dissatisfied &better job", "better job/dissatisfied with job", regex=False
        )
        .str.replace(r"(\w+)\, (\w+)", r"\1/\2", regex=True)
        .str.replace(r"(\w+)job", r"\1 job", regex=True)
        .str.replace(r"unsatifactory", "unsatisfactory", regex=False)
        .str.replace(r"city unknown", "city/unknown", regex=False)
    )
    return df.drop(columns=["resignation_reason"])


def extract_left_reason_desc18(df):
    df.loc[:, "left_reason_desc"] = (
        df.reason.str.lower()
        .str.strip()
        .str.replace(r"(\w+)-? ?(.+)", r"\2", regex=True)
        .str.replace(r"w\/", "with", regex=True)
    )
    return df


def clean_left_reason(df):
    reasons = (
        df.reason.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r" $", "", regex=True)
        .str.replace(r"^etired", "retired", regex=True)
        .str.replace(r"^esign", "resigned", regex=True)
        .str.extract(
            r"(deceased-? ?|resigne?d?-? ?|retirement-? ?|involuntary.+|dismissal-? ?|termination|retired)"
        )
    )

    df.loc[:, "left_reason"] = (
        reasons[0]
        .fillna("")
        .str.replace(r"(-| $)", "", regex=True)
        .str.replace(r"^resign$", "resigned", regex=True)
    )
    return df.drop(columns=["reason"])


def clean21():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/nopd_cprr_separations_2019-2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "class": "class_id",
                "assignment": "assignment_id",
                "date": "left_date",
            }
        )
        .pipe(strip_leading_commas)
        .pipe(split_names)
        .pipe(clean_rank_desc)
        .pipe(clean_left_reason_desc21)
        .pipe(clean_left_reason)
        .pipe(sanitize_dates)
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["employee_id", "rank_desc", "assignment_id"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "New Orleans PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["left_reason", "left_reason_desc", "left_date"], "separation_uid"
        )
    )
    return df


def clean18():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_separations_2018.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "class": "class_id",
                "assign": "assignment_id",
                "date": "left_date",
            }
        )
        .pipe(extract_left_reason_desc18)
        .pipe(clean_left_reason)
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["class_id", "assignment_id"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "New Orleans PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["left_reason", "left_reason_desc", "left_date"], "separation_uid"
        )
    )
    return df


def clean(df18, df21):
    df = pd.concat([df18, df21], axis=0).drop_duplicates(subset=["uid"])
    return df


if __name__ == "__main__":
    df18 = clean18()
    df21 = clean21()
    df = clean(df18, df21)
    df.to_csv(deba.data("clean/pprr_seps_new_orleans_pd_2018_2021.csv"), index=False)
