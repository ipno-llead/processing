import deba
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_names
from lib.uid import gen_uid
import pandas as pd


def join_allegation_cols(df):
    df.loc[:, "allegation_rule"] = (
        df.allegation_rule.str.lower()
        .str.strip()
        .str.replace(r"perf\b", "performance", regex=True)
        .str.replace(r"prof\b", "professional", regex=True)
        .str.replace(r"rule 1:(.+)", r"rule 1: \1", regex=True)
        .str.replace(r"chief admin\. office", "", regex=True)
        .str.replace(
            r"^(no violation observed|policy|civil service rules|no allegation assigned at this time)$",
            "",
            regex=True,
        )
        .str.replace(r"dept", "department", regex=False)
    )

    df.loc[:, "allegation_paragraph"] = (
        df.allegation_paragraph.str.lower()
        .str.strip()
        .str.replace(
            r"paragraph 13 - social networking (.+)",
            "paragraph 13 - social networking, websites, print or transmitted media, etc.",
            regex=True,
        )
        .str.replace(
            r"^paragraph 14 - social networking, websites, facebook, myspace, print or transmitted media, etc.$",
            "paragraph 14 - social networking, websites, print or transmitted media, etc.",
            regex=True,
        )
        .str.replace(r"paragraph 2-effective$", r"paragraph 2 - effective", regex=True)
    )

    df.loc[:, "allegation"] = df.allegation_rule.str.cat(
        df.allegation_paragraph, sep="; "
    )
    return df.drop(columns=["allegation_rule", "allegation_paragraph"])[
        ~((df.allegation.fillna("") == ""))
    ]


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.strip()
        .str.lower()
        .str.replace(
            r"(.+)?\brui\b(.+)?",
            "resigned or retired while under investigation",
            regex=True,
        )
        .str.replace(r"^nfim case$", "no further investigation merited", regex=True)
        .str.replace(r"^withdrawn- mediation$", "mediation", regex=True)
        .str.replace(r"duplicate (.+)", "", regex=True)
        .str.replace(r"dui-(.+)", "dismissed under investigation", regex=True)
        .str.replace(
            r"charges proven resigned",
            "resigned or retired while under investigation",
            regex=True,
        )
        .str.replace(r"invest\.$", "investigation", regex=True)
        .str.replace(r"\.$", "", regex=True)
        .str.replace(r"^pending$", "pending investigation", regex=True)
        .str.replace(r"^charges proven$", "sustained", regex=True)
        .str.replace(r"(cancelled|info\b(.+)?)", "", regex=True)
        .str.replace(r"reclassified as di-2", "di-2", regex=False)
        .str.replace(r"^dui$", "dismissed under investigation", regex=True)
        .str.replace(r"^dui sustained$", "sustained", regex=True)
        .str.replace(r"reclassified as info$", "info", regex=True)
        .str.replace(r"^investigation cancelled$", "cancelled", regex=True)
        .str.replace(r"^sustained -(.+)", "sustained", regex=True)
        .str.replace(r"charges withdrawn", "withdrawn", regex=False)
        .str.replace(
            r"^resigned$", "resigned or retired while under investigation", regex=True
        )
        .str.replace(r"^di-3 nfim$", "no further investigation merited", regex=True)
        .str.replace(
            r"^retired under investigation$",
            "resigned or retired while under investigation",
            regex=True,
        )
        .str.replace(r"dismissal -(.+)", "dismissed under investigation", regex=True)
        .str.replace(r"sustained-(.+)", "sustained", regex=True)
        .str.replace(r"^ di-3", r"di-3", regex=True)
        .str.replace(r"unfounded- dui", "unfounded", regex=False)
        .str.replace(
            r"^(nat|moot(.+)?|investigation|reclassified as|redirect(.+)|bwc(.+)|grievance|awaiting(.+)|non-applicable|deceased|other)",
            "",
            regex=True,
        )
        .str.replace(r"^nfim$", "no further investigation merited", regex=True)
    )
    return df


def extract_investigation_start_year(df):
    df.loc[:, "investigation_start_year"] = df.tracking_id_og.str.replace(
        r"^(\w{4})-(.+)", r"\1", regex=True
    )
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/cprr_new_orleans_pd_2005_2023.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "2023_2086_p": "tracking_id_og",
                "8913": "employee_id",
                "jeffrey": "first_name",
                "vappie": "last_name",
                "public_initiated": "complainant_type",
                "initial": "status",
            }
        )
        .drop(columns=["allegation_number"])
        .pipe(join_allegation_cols)
        .pipe(clean_disposition)
        .pipe(extract_investigation_start_year)
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(
            standardize_desc_cols, ["tracking_id_og", "allegation_desc", "disposition", "complainant_type", "status"]
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "allegation_desc", "tracking_id"], "allegation_uid")
    )
    return df
