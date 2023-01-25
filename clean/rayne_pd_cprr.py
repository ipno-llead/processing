import pandas as pd
import deba

from lib.columns import clean_column_names, set_values
from lib.clean import (
    clean_dates,
    clean_names,
    strip_leading_comma,
    standardize_desc_cols,
    float_to_int_str,
)
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


def split_investigator_names(df):
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
    desc = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"[bc1-3]\)", "", regex=True)
        .str.extract(r"\((.+)\)")
    )

    df.loc[:, "allegation_desc"] = (
        desc[0].fillna("").str.replace(r"(.+) \((.+)", r"\2", regex=True)
    )
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


def clean_disposition14(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"(2\)|1\))", "", regex=True)
        .str.replace(r" ", "", regex=False)
    )
    return df


def split_investiator_names(df):
    ranks = {
        "lt": "lieutenant",
        "cpl": "corporal",
        "det": "detective",
        "capt": "captain",
        "sgt": "sergeant",
    }

    names = (
        df.assigned_investigator.str.lower()
        .str.strip()
        .str.replace(r"^cpat", "capt", regex=True)
        .str.replace(r"c\.", "chastity", regex=True)
        .str.replace(r"l\. mike", "lt. mike", regex=True)
        .str.extract(r"(capta?i?n?|det|lt|cpl|sgt)?\.? ?(\w+)?\.? (\w+)")
    )

    df.loc[:, "investigator_rank_desc"] = names[0].map(ranks)
    df.loc[:, "investigator_first_name"] = names[1].fillna("")
    df.loc[:, "investigator_last_name"] = names[2]
    return df.drop(columns=["assigned_investigator"])


def clean_tracking_id(df):
    df.loc[:, "tracking_id_og"] = (
        df.ad_number.str.lower()
        .str.strip()
        .str.replace(r"\s+", "", regex=True)
        .str.replace(r"-", "", regex=False)
    )
    return df.drop(columns=["ad_number"])


def split_rows_with_multiple_allegations_13(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .str.replace(r"cubo\/", "cubo-", regex=True)
        .str.replace(r"cubo", "conduct unbecoming an officer", regex=False)
    )

    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df.drop(columns=["complaint"])


def clean_allegations_13(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"- (\w+)", r"-\1", regex=True)
        .str.replace(r"^ffde$", "", regex=True)
        .str.replace(r"ind\.", "indecent", regex=True)
        .str.replace(r" \(missing evidence\)", r"-missing evidence", regex=True)
        .str.replace(r" \((.+)\)?", "", regex=True)
        .str.replace(r"equiptment", "equipment", regex=False)
        .str.replace(r"duites", "duties", regex=False)
        .str.replace(r"^rude and$", "rude", regex=True)
        .str.replace(r"^nb\)$", "", regex=True)
    )
    return df[~((df.allegation.fillna("") == ""))]


def clean_receive_and_investigation_dates(df):
    df.loc[:, "receive_date"] = (
        df.date_received.astype(str)
        .str.replace(r"20100", "2010", regex=False)
        .str.replace(r"0517", "05/17", regex=False)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"0\/29\/2009", "", regex=True)
        .str.replace(r"\/14$", "2014", regex=True)
        .str.replace(r"\/10$", "2010", regex=True)
        .str.replace(r"\/11$", "2011", regex=True)
        .str.replace(r"\/13$", "2013", regex=True)
        .str.replace(r"\/15$", "2015", regex=True)
        .str.replace(r"\/09$", "2009", regex=True)
        .str.replace(r"7\/32014", r"7/3/2014", regex=True)
        .str.replace(r"(\w{1,2})\/(\w{2})(\w{4})", r"\1/\2/\3", regex=True)
    )

    df.loc[:, "investigation_complete_date"] = (
        df.date_completed.astype(str)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"\/14$", "2014", regex=True)
        .str.replace(r"\/10$", "2010", regex=True)
        .str.replace(r"\/11$", "2011", regex=True)
        .str.replace(r"\/13$", "2013", regex=True)
        .str.replace(r"\/15$", "2015", regex=True)
        .str.replace(r"\/09$", "2009", regex=True)
        .str.replace(r"8\/12014", r"8/1/2014", regex=True)
        .str.replace(r"(\w{1,2})\/(\w{1})(\w{4})", r"\1/\2/\3", regex=True)
        .str.replace(r"(\w{1,2})\/(\w{1})\/(\w{1})(\w{4})", r"\1/\2\3/\4", regex=True)
    )

    return df.drop(columns=["date_received", "date_completed"])


def split_rows_with_multiple_officers_13(df):
    df.loc[:, "focus_officers"] = (
        df.focus_officers.str.lower()
        .str.strip()
        .str.replace(r"\,", "/", regex=True)
        .str.replace(r"\((.+)\/ ?(.+)\)", r"(\1-\2)", regex=True)
        .str.replace(r"\((\w+)\) ?-(\w+)", r"(\1)/\2", regex=True)
        .str.replace(
            r"thompson\/ \(all metro narcotics agents\)",
            "thompson (all metro narcotics agents)/",
            regex=True,
        )
        .str.replace(r"\(k- 9\)\-cpl", r"(k-9)/cpl", regex=True)
        .str.replace(
            r"greg cormier scott poiencot", "greg cormier/scott poiencot", regex=False
        )
        .str.replace(r"\(2b\) and officer", r"(2b)/officer", regex=True)
        .str.replace(r"\(csu\) and ross", r"(csu)/ross", regex=True)
        .str.replace(r"\(k-9\)-vance", r"(k-9)/vance", regex=True)
        .str.replace(r"moore officer jeremy", "moore/officer jeremy", regex=False)
        .str.replace(r"and joel grayson", "/joel grayson", regex=False)
        .str.replace(r"^\s+(\w+)\s+$", r"\1", regex=True)
        .str.replace(r"lebreton and guidry", "lebreton/guidry", regex=False)
        .str.replace(r"^(\w+)\/ (\w+)$", r"\1/\2", regex=True)
    )
    df = (
        df.drop("focus_officers", axis=1)
        .join(
            df["focus_officers"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("focus_officers"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_names_13(df):
    ranks = {
        "sgt": "sergeant",
        "ofc": "officer",
        "lt": "lieutenant",
        "cpl": "corporal",
        "det": "detective",
        "cpt": "captain",
        "officer": "officer",
        "park ranger": "park ranger",
        "city marshall": "city marshall",
        "dispatcher": "dispatcher",
    }
    names = (
        df.focus_officers.str.replace(r"^(and| ) ", "", regex=True)
        .str.replace(r"^drc ", "", regex=True)
        .str.replace(r"^lpd-eeoc complain$", "", regex=True)
        .str.replace(r"(\w)\.", r"\1", regex=True)
        .str.replace(r"officers\b", "officer", regex=True)
        .str.replace(r"officer -", "officer", regex=False)
        .str.replace(r"lpd dispatcher", "dispatcher", regex=False)
        .str.extract(
            r"(officer|ofc\b|lt|sgt|det\b|cpl|cpt|park ranger|city marshall|dispatcher)?\.? ?(?:(\w+) )?(?:(\w+) ?)(?:\((.+)\))?"
        )
    )

    df.loc[:, "rank_desc"] = names[0].map(ranks, na_action=None)
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    df.loc[:, "department_desc"] = (
        names[3]
        .str.replace(r"narc\b", "narcotics", regex=True)
        .str.replace(r"da\'s office", "", regex=True)
        .str.replace(r"communication$", "communications", regex=True)
        .str.replace(r"-sro$", "school resource office", regex=True)
        .str.replace(r"cid\b", "criminal investigations division", regex=True)
        .str.replace(r"(\(|\))", "", regex=True)
    )
    return df[~((df.last_name.fillna("") == ""))]


def extract_action(df):
    df.loc[:, "action"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"((.+)sta rling(.+)|exonorated|\/ brent 3 (.+))", "", regex=True)
        .str.replace(r"sutained", "sustained", regex=False)
        .str.replace(r"counseli ng", "counseling")
        .str.replace(
            r"(susta?i?n?e?d?|unfounded|not sustained|exonerated|justified|use of|force|matrix|det|cnboc)\.?\/? ?",
            "",
            regex=True,
        )
        .str.replace(r"lor\b", "letter of reprimand", regex=True)
        .str.replace(r"repremaind", "reprimand", regex=False)
        .str.replace(r"(-?|; ?|\/ ?|\((.+)\))", "", regex=True)
        .str.replace(
            r"(complaint withdrawn|fit for duty|unprofessional conduct|officer ?|ds\b)",
            "",
            regex=True,
        )
        .str.replace(r"invest\.", "investigation", regex=True)
        .str.replace(
            r"^(ned fowler  cuboletter of reprimand gabe thompson  cubocounseling form|anboc deficiency|policy issue)$",
            "",
            regex=True,
        )
        .str.replace(
            r"( for both s|performance inspection report|on all s|overturned by civil service bd)",
            "",
            regex=True,
        )
        .str.replace(
            r"all focus s listed received counseling forms", "counseling", regex=False
        )
        .str.replace(r"^2 ?days?$", "2 day", regex=True)
        .str.replace(
            r"(excessive failure to complete report|sent to hr| "
            r"insubordination rude and unprofessional |withdrawn|training issue|exonerated)",
            "",
            regex=True,
        )
        .str.replace(r"(\w+) days? suspension", r"\1-day suspension", regex=True)
        .str.replace(r"nbocresigned", "resigned", regex=False)
        .str.replace(r"loc\b", "letter of caution", regex=True)
        .str.replace(r"deroussel", "", regex=False)
        .str.replace(r"resignedterminated", "terminated", regex=False)
        .str.replace(r"^termination $", "terminated", regex=True)
        .str.replace(r"no violation ex", "", regex=False)
        .str.replace(r"counseling form", "counseling", regex=False)
        .str.replace(r" false statement", "", regex=False)
    )
    return df


def clean_disposition(df):
    dispos = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"sutained", "sustained", regex=False)
        .str.replace(r"invest\.", "investigation", regex=True)
        .str.replace(r"hr\b", "human resources")
        .str.extract(
            r"(resigned under investigation|resigned prior to termination|resigned|"
            r"exonerated|susta?i?n?e?d?|unfounded|not sustained|"
            r"exonerated|justified|sent to human resources|"
            r"excessive failure to complete report)"
        )
    )

    df.loc[:, "disposition"] = dispos[0].str.replace(r"^sust$", "sustained", regex=True)
    return df


def drop_rows_missing_dates(df):
    df = df[~((df.receive_date.fillna("") == ""))]
    df = df[~((df.investigation_complete_date.fillna("") == ""))]
    return df

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
        .pipe(set_values, {"agency": "rayne-pd"})
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
        .pipe(set_values, {"agency": "rayne-pd"})
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


def clean13():
    df = (
        pd.read_csv(deba.data("raw/rayne_pd/rayne_pd_cprr_2009_2013.csv"))
        .pipe(clean_column_names)
        .pipe(clean_receive_and_investigation_dates)
        .pipe(strip_leading_comma)
        .pipe(clean_tracking_id)
        .pipe(split_rows_with_multiple_allegations_13)
        .pipe(clean_allegations_13)
        .pipe(split_investigator_names)
        .pipe(extract_action)
        .pipe(clean_disposition)
        .pipe(split_rows_with_multiple_officers_13)
        .pipe(split_names_13)
        .pipe(drop_rows_missing_dates)
        .pipe(standardize_desc_cols, ["action", "disposition"])
        .pipe(set_values, {"agency": "rayne-pd"})
        .pipe(
            gen_uid,
            ["investigator_first_name", "investigator_last_name", "agency"],
            "investigator_uid",
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "allegation",
                "disposition",
                "action",
                "uid",
                "receive_date",
                "investigation_complete_date"
            ],
            "allegation_uid",
        )
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(
            float_to_int_str,
            [
                "tracking_id_og",
                "allegation",
                "rank_desc",
                "first_name",
                "last_name",
                "department_desc",
                "investigator_rank_desc",
                "investigator_first_name",
                "investigator_last_name",
                "action",
                "agency",
                "investigator_uid",
                "uid",
                "allegation_uid",
                "tracking_id",
            ],
        )
    )
    return df.drop_duplicates(subset=["allegation_uid"])


if __name__ == "__main__":
    df20 = clean20()
    df14 = clean14()
    df13 = clean13()
    df20.to_csv(deba.data("clean/cprr_rayne_pd_2019_2020.csv"), index=False)
    df14.to_csv(deba.data("clean/cprr_rayne_pd_2014_2018.csv"), index=False)
    df13.to_csv(deba.data("clean/cprr_rayne_pd_2009_2013.csv"), index=False)
