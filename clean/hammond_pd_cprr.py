import pandas as pd
import dirk
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols
from lib.uid import gen_uid
from lib.rows import duplicate_row
import re


def clean_tracking_number(df):
    df.loc[:, "tracking_number"] = (
        df.internal_affairs.fillna("")
        .str.lower()
        .str.strip()
        .str.replace(r"(ia-|ia )", "", regex=True)
        .str.replace(".", "-", regex=False)
        .str.replace("2018", "18", regex=False)
        .str.replace("1801", "18-01", regex=False)
        .str.replace("#", "", regex=False)
        .str.replace(r"(\d+) (\d+)", r"\1-\2", regex=True)
    )
    return df.drop(columns="internal_affairs")


def clean_incident_date(df):
    df.loc[:, "incident_date"] = df.incident_date.fillna("").str.replace(
        "7/2010 til 5/2012", "07/01/2010", regex=False
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.finding.fillna("")
        .str.lower()
        .str.strip()
        .str.replace("no action", "", regex=False)
    )
    return df.drop(columns="finding")


def split_name(df):
    names = df.ee_name.str.lower().str.strip().str.extract(r"^(\w+) (\w+)")
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns="ee_name")


def clean_department_desc(df):
    df.loc[:, "department_desc"] = (
        df.division.str.lower()
        .str.strip()
        .str.replace("jailer", "corrections", regex=False)
    )
    return df.drop(columns=["dept", "division"])


def clean_action(df):
    df.loc[:, "action"] = (
        df.final_recommendation.fillna("")
        .str.lower()
        .str.strip()
        .str.replace(
            "three (3) consecutive shift-days suspenion without pay",
            "3-day suspension without pay",
            regex=False,
        )
        .str.replace(
            "5-five consecutive shift-days suspension without pay",
            "5-day suspension without pay",
            regex=False,
        )
        .str.replace("| day suspension commence 1/28/17", "suspended", regex=False)
        .str.replace("days suspension 2 days", "2-day suspension", regex=False)
        .str.replace(r"^(\w+) resigned", "resigned", regex=True)
        .str.replace(
            "suspended without pay period of 3 days", "3-day suspension", regex=False
        )
        .str.replace("frrom", "from", regex=False)
        .str.replace("edwin bergeron 10/2/2019", "", regex=False)
        .str.replace(r"^(\d+) (\w+)", r"\1-\2", regex=True)
        .str.replace("no action", "", regex=False)
        .str.replace("/", "", regex=False)
    )
    return df.drop(columns="final_recommendation")


def combine_allegations_columns(df):
    def combine(row):
        txts = []
        if pd.notnull(row.violation_1):
            txts.append("%s" % row.violation_1)
        if pd.notnull(row.violation_2_3):
            txts.append("%s" % row.violation_2_3)
        return "| ".join(txts)

    df.loc[:, "allegation"] = df.apply(combine, axis=1, result_type="reduce")
    df = df.drop(columns=["violation_1", "violation_2_3"])
    return df


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^(\d+) (\w+)", r"\1 - \2", regex=True)
        .str.replace(".", "", regex=False)
        .str.replace(r" \bgo\b ", " ", regex=True)
        .str.replace("/", " and ", regex=False)
        .str.replace(r"\(founded\)", "", regex=True)
        .str.replace(r"^excessive force ", "excessive force | ", regex=True)
        .str.replace("omissionn", "omission", regex=False)
        .str.replace(
            r" ?(unbec?o?i?mo?ing) ?( ?(of)? ?(an officer)?)? ?",
            " unbecoming ",
            regex=True,
        )
        .str.replace(
            "video conduct unbecoming", "video | conduct unbecoming", regex=False
        )
    )
    return df


def split_rows_with_multiple_allegations_20(df):
    i = 0
    for idx in df[df.allegation.str.contains(" | ")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:\|)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df


def realign_action_column(df):
    df.loc[
        (df.tracking_number == "15-10") & (df.incident_month == "10"),
        "action",
    ] = "3-day suspension without pay"

    df.loc[
        (df.tracking_number == "15-09") & (df.incident_month == "10"),
        "action",
    ] = "5-day suspension without pay"
    return df


def drop_rows_without_tracking_number(df):
    return df[df.tracking_number != ""].reset_index(drop=True)


def split_name_09(df):
    names = df.employee.str.lower().str.strip().str.extract(r"(\w+) (\w+)")
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns="employee")


def clean_allegations_09(df):
    df.loc[:, "allegation"] = (
        df.violation.str.lower()
        .str.strip()
        .str.replace(r"compla?ia?nt ?-? ?", "", regex=True)
        .str.replace("&", "-", regex=False)
        .str.replace(r"^use of ", "", regex=True)
        .str.replace(r"force (\w+)", r"force - \1", regex=True)
        .str.replace(r"^rude$", "rude/discourteous", regex=True)
        .str.replace(r"officer-(\w+)", r"officer - \1", regex=True)
        .str.replace(r"(\w+) - (\w+)", r"\1-\2", regex=True)
        .str.replace(r" - threatened$", r"/threatened", regex=True)
        .str.replace("nonlethal", "non-lethal", regex=False)
        .str.replace(" rude-discourteous", "-rude/discourteous", regex=False)
        .str.replace(
            "conduct unbecoming-", "conduct unbecoming an officer-", regex=True
        )
        .str.replace("-making", "/making", regex=False)
    )
    return df.drop(columns="violation")


def clean_disposition_09(df):
    df.loc[:, "disposition"] = df.founded_unfounded.str.lower().str.strip().fillna("")
    return df.drop(columns="founded_unfounded")


def clean_action_09(df):
    df.loc[:, "action"] = (
        df.recommended_action.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"(\d+) (\w+)", r"\1-\2", regex=True)
        .str.replace("hrs", "hour", regex=False)
        .str.replace("n o", "no", regex=False)
        .str.replace("days", "day", regex=False)
        .str.replace("actions", "action", regex=False)
        .str.replace("suspension - 40-day", "40-day suspension", regex=False)
    )
    return df.drop(columns="recommended_action")


def extract_rank_from_name(df):
    ranks = df.name.str.lower().str.strip().str.extract(r"(sgt)")
    df.loc[:, "rank_desc"] = ranks[0].str.replace("sgt", "sergeant", regex=False)
    return df


def split_name_08(df):
    df.loc[:, "name"] = (
        df.name.str.lower()
        .str.strip()
        .str.replace(".", ",", regex=False)
        .str.replace("sgt ", "", regex=False)
        .str.replace(r"(thad)? ?gauth?ier", "gautier thaddeus", regex=True)
        .str.replace("michael damato", "damato michael", regex=False)
        .str.replace("eric watson", "watson eric", regex=False)
        .str.replace("corey stewart", "stewart corey", regex=False)
        .str.replace("patrick peterman", "peterman patrick", regex=False)
        .str.replace("john alexander", "alexander john", regex=False)
        .str.replace("daniel boudreaux", "boudreaux daniel", regex=False)
        .str.replace("jeannine cruz", "cruz jeannine", regex=False)
        .str.replace("vicki corkern", "corkern vicki", regex=False)
        .str.replace("terry zaffuto", "zaffuto terry", regex=False)
        .str.replace("james king", "king james", regex=False)
        .str.replace("thomas miller", "miller thomas", regex=False)
        .str.replace("derrick foster", "foster derrick", regex=False)
        .str.replace(r"jo(seph|ey) drago", "drago joseph", regex=True)
    )

    names = df.name.str.extract(r"(\w+),? (\w+)")
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    return df.drop(columns="name")


def extract_complaint_type(df):
    types = (
        df.nature_of_incident.str.lower()
        .str.strip()
        .str.extract(r"(citizen complaint)")
    )
    df.loc[:, "complaint_classification"] = types[0].str.replace(
        "complaint", "", regex=False
    )
    return df


def clean_allegations_08(df):
    df.loc[:, "allegation"] = (
        df.nature_of_incident.str.lower()
        .str.strip()
        .str.replace("citizen complaint", "", regex=False)
        .str.replace(r"(\w+)[\,\;] (\w+)", r"\1/\2", regex=True)
        .str.replace(r"ia-? ?(threatening|alleged)? ", "", regex=True)
        .str.replace("&", "and", regex=False)
        .str.replace("invest", "investigation", regex=False)
        .str.replace("complaint ", "", regex=False)
        .str.replace(
            r"^conduct unbecoi?ming", "conduct unbecoming an officer", regex=True
        )
    )
    return df.drop(columns="nature_of_incident")


def extract_and_clean_action(df):
    df.loc[:, "suspension_hrs"] = (
        df.suspension_hrs.str.lower()
        .str.strip()
        .str.replace(r"(\d+) days? ?(susp)?", r"\1 day suspension", regex=True)
    )

    df.loc[:, "letter_of_rep"] = (
        df.letter_of_rep.str.lower().str.strip().str.replace("x", "letter of reprimand")
    )

    df.loc[:, "action"] = (
        df.no_action.str.lower()
        .str.strip()
        .str.replace("x", "", regex=False)
        .str.replace("no action", "", regex=False)
        .str.replace(
            "suspension - 1 day per gen order 107",
            "1 day suspension per gen order 107",
            regex=False,
        )
        .str.replace(r"(\w+) - (\w+)", r"\1|\2", regex=True)
    )

    df.loc[:, "action"] = df.action.fillna("") + "" + df.letter_of_rep.fillna("")
    df.loc[:, "action"] = df.action + "" + df.suspension_hrs.fillna("")

    df.loc[:, "action"] = df.action.str.replace(
        "reprimand2", "reprimand|2", regex=False
    )
    return df.drop(columns=["no_action", "letter_of_rep", "suspension_hrs"])


def extract_and_clean_dispositions_08(df):
    df.loc[:, "dismiss"] = (
        df.dismiss.str.lower().str.strip().str.replace("x", "dismissed", regex=False)
    )

    df.loc[:, "disposition"] = (
        df.founded_un.str.lower()
        .str.strip()
        .str.replace(r"^u$", "unfounded", regex=True)
        .str.replace("x", "founded", regex=False)
    )

    df.loc[:, "disposition"] = df.disposition.fillna("").str.cat(df.dismiss.fillna(""))
    return df.drop(columns=["founded_un", "dismiss"])


def split_rows_with_multiple_allegations_08(df):
    i = 0
    for idx in df[df.allegation.str.contains("/")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df


def clean_incident_date_08(df):
    df.loc[:, "incident_date"] = df.date_of_incident.fillna("").str.replace(
        "5/8/8/04", "5/08/2004", regex=False
    )
    return df.drop(columns="date_of_incident")


def clean_action_date_08(df):
    df.loc[:, "initial_action_date"] = df.date_of_action.str.replace(
        "010/7/04", "10/7/04", regex=False
    ).str.replace("23/05", "2/3/05", regex=False)
    return df.drop(columns="date_of_action")


def clean_tracking_number_08(df):
    df.loc[:, "tracking_number"] = (
        df.ia_no.str.lower()
        .str.strip()
        .str.replace(r"(\d+) (\w+)?(\d+)?(\w{1})", r"\1-\2\3", regex=True)
        .str.replace(r"(\d+)-apr", r"04-\1", regex=True)
    )
    return df.drop(columns="ia_no")


def clean_20():
    df = pd.read_csv(dirk.data("raw/hammond_pd/hammond_pd_cprr_2015_2020.csv")).pipe(
        clean_column_names
    )
    df = (
        df.pipe(split_name)
        .pipe(clean_tracking_number)
        .pipe(clean_incident_date)
        .pipe(clean_disposition)
        .pipe(clean_action)
        .pipe(combine_allegations_columns)
        .pipe(clean_allegations)
        .pipe(split_rows_with_multiple_allegations_20)
        .pipe(clean_department_desc)
        .pipe(clean_dates, ["incident_date", "investigation_start_date"])
        .pipe(realign_action_column)
        .pipe(drop_rows_without_tracking_number)
        .pipe(standardize_desc_cols, ["department_desc", "action", "allegation"])
        .pipe(
            set_values,
            {
                "agency": "Hammond PD",
            },
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "tracking_number", "disposition"],
            "allegation_uid",
        )
    )
    return df


def clean_14():
    df = pd.read_csv(dirk.data("raw/hammond_pd/hammond_pd_cprr_2009_2014.csv"))
    df = (
        df.pipe(clean_column_names)
        .rename(columns={"date": "investigation_start_date"})
        .pipe(clean_dates, ["investigation_start_date"])
        .pipe(split_name_09)
        .pipe(clean_tracking_number)
        .pipe(clean_allegations_09)
        .pipe(clean_disposition_09)
        .pipe(clean_action_09)
        .pipe(
            set_values,
            {
                "agency": "Hammond PD",
            },
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "tracking_number", "disposition"],
            "allegation_uid",
        )
    )
    return df


def clean_08():
    df = (
        pd.read_csv(dirk.data("raw/hammond_pd/hammond_pd_cprr_2004_2008.csv"))
        .pipe(clean_column_names)
        .drop(columns=["letter_of_instruction"])
        .pipe(extract_rank_from_name)
        .pipe(split_name_08)
        .pipe(extract_complaint_type)
        .pipe(clean_allegations_08)
        .pipe(extract_and_clean_action)
        .pipe(extract_and_clean_dispositions_08)
        .pipe(clean_incident_date_08)
        .pipe(clean_action_date_08)
        .pipe(clean_tracking_number_08)
        .pipe(clean_dates, ["initial_action_date", "incident_date"])
        .pipe(set_values, {"agency": "Hammond PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "action", "tracking_number"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df20 = clean_20()
    df14 = clean_14()
    df08 = clean_08()
    df20.to_csv(dirk.data("clean/cprr_hammond_pd_2015_2020.csv"), index=False)
    df14.to_csv(dirk.data("clean/cprr_hammond_pd_2009_2014.csv"), index=False)
    df08.to_csv(dirk.data("clean/cprr_hammond_pd_2004_2008.csv"), index=False)
