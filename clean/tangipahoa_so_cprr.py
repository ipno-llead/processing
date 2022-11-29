import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import clean_dates, float_to_int_str, clean_names, standardize_desc_cols
import pandas as pd
from lib.rows import duplicate_row
import re


def split_rows_with_name(df):
    for idx, row in df[df.full_name.fillna("").str.contains(" & ")].iterrows():
        names = row.full_name.split(" & ")
        df.loc[idx, "full_name"] = names[0]
        row = row.copy()
        row.loc["full_name"] = names[1]
        df = df.append(row)
    return df


def split_full_name(df):
    df.loc[:, "full_name"] = (
        df.full_name.str.lower()
        .str.strip()
        .str.replace(
            r"(unknown|unk|tpso|tp715|facebook comments|^deputy$)", "", regex=True
        )
        .str.replace(".", "", regex=False)
        .str.replace(r"(\w+), (\w+)", r"\2 \1", regex=True)
        .str.replace("d'amatto", "d'amato", regex=False)
        .str.replace("francoois", "francois", regex=False)
    )
    parts = df.full_name.str.extract(r"(?:(dy|sgt|lt|capt) )?(?:([^ ]+) )?(.+)")
    df.loc[:, "rank_desc"] = parts[0].replace(
        {"dy": "deputy", "sgt": "sergeant", "lt": "lieutenant", "capt": "captain"}
    )
    df.loc[:, "first_name"] = (
        parts[1].str.strip().str.replace("jessical", "jessica", regex=False)
    )
    df.loc[:, "last_name"] = parts[2].str.strip().str.replace("deputy", "", regex=False)
    df.loc[df.full_name == "deputy", "rank_desc"] = "deputy"
    return df.drop(columns=["full_name"])


def clean_dept_desc(df):
    df.loc[:, "department_desc"] = (
        df.dept_desc.str.lower()
        .str.strip()
        .str.replace("sro", "school resource department", regex=False)
        .str.replace(r"^res$", "reserve", regex=True)
        .str.replace("cid", "criminal investigations division", regex=False)
        .str.replace(r"pat(?:roll?)?", r"patrol", regex=True)
        .str.replace("comm", "communications", regex=False)
        .str.replace("admin", "administration", regex=False)
    )
    return df.fillna("").drop(columns="dept_desc")


def clean_complaint_type(df):
    df.complaint_type = (
        df.complaint_type.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("citizen complaint", "citizen", regex=False)
        .str.replace("admin inv", "administrative", regex=False)
        .str.replace(r"int(?:\.?)?", r"internal", regex=True)
        .str.replace(r"ex[rt](?:\.?)?", r"external", regex=True)
    )
    return df


def clean_rule_violation(df):
    df.loc[:, "allegation"] = (
        df.rule_violation.str.lower()
        .str.strip()
        .str.replace(r" ?/ ?", "/", regex=True)
        .str.replace("w/", "with ", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("-", "", regex=False)
        .str.replace(r"cour?ter?se?y", "courtesy", regex=True)
        .str.replace("authoruty", "authority", regex=False)
        .str.replace(r"unsar?tasfactory", "unsatisfactory", regex=True)
        .str.replace(r"pefr?ormance", "performance", regex=True)
        .str.replace("accidnet", "accident", regex=False)
        .str.replace("rudness", "rudeness", regex=False)
        .str.replace("policy violation", "", regex=False)
        .str.replace("mishandeling", "mishandling", regex=False)
        .str.replace("handeling", "handling", regex=False)
        .str.replace("uof", "use of force", regex=False)
        .str.replace("trafic", "traffic", regex=False)
        .str.replace("mistratment", "mistreatment", regex=False)
        .str.replace("misued", "misuse", regex=False)
        .str.replace(" pursuit", "pursuit", regex=False)
        .str.replace("delayed responsetime", "delayed response time", regex=False)
        .str.replace("social mediathreat", "social media threat", regex=False)
        .str.replace("behaivor", "behavior", regex=False)
        .str.replace(r"(?:(\w+)  (\w+))", r"\1 \2", regex=True)
    )
    return df.drop(columns="rule_violation")


def clean_investigating_supervisor(df):
    df.loc[:, "investigating_supervisor"] = (
        df.investigating_supervisor.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(".", "", regex=False)
        .str.replace(r"\bca?pt\b", "captain", regex=True)
        .str.replace("det", "detective", regex=False)
        .str.replace("sgt", "sargeant", regex=False)
        .str.replace("km", "kim", regex=False)
        .str.replace("mke", "mike", regex=False)
        .str.replace(r"(^p$|p$)", "panepinto", regex=True)
        .str.replace(r"\b(lt|ltj|it)\b", "lieutenant", regex=True)
        .str.replace("/", "", regex=False)
        .str.replace(r"^captain$", "", regex=True)
        .str.replace(r"fe[cr]ra[nm]d", "ferrand", regex=True)
        .str.replace(r"(\w+\') (\w+)", r"\1\2", regex=True)
    )
    parts = df.investigating_supervisor.str.extract(
        r"(?:(lieutenant|captain|detective|chief|major|sargeant) )?(?:([^ ]+) )?(.+)"
    )
    df.loc[:, "supervisor_rank"] = parts[0].fillna("")
    df.loc[:, "supervisor_first_name"] = parts[1].fillna("")
    df.loc[:, "supervisor_last_name"] = parts[2].fillna("")
    return df.drop(columns="investigating_supervisor")


def clean_disposition(df):
    df.disposition = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace("not sustained", "unsustained", regex=False)
        .str.replace("exonerted", "exonerated", regex=False)
        .str.replace(r"(?:admin[\.]?[closed]?)", "administrative", regex=True)
    )
    return df


def clean_appeal(df):
    df.appeal = df.appeal.str.lower().str.strip().str.replace(r"^$", "yes", regex=True)
    return df


def clean_policy_failure(df):
    df.policy_failure = (
        df.policy_failure.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^$", "yes", regex=True)
    )
    return df


def clean_submission_type(df):
    df.loc[:, "complainant_type"] = (
        df.submission_type.str.lower()
        .str.strip()
        .str.replace(r"ph(one)?", "complaint by telephone", regex=True)
        .str.replace(r"(in )?per(son)?", "complainant appeared in person", regex=True)
        .str.replace(r"^l$", "complaint by letter", regex=True)
        .str.replace(r"(l/)?email", "complaint by email", regex=True)
    )
    return df.drop(columns="submission_type")


def clean_action(df):
    df.action = (
        df.action.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"susp(ens?ion|ended)?", "suspended", regex=True)
        .str.replace("dem", "demoted", regex=False)
        .str.replace(r"terminat(e|ion)", "terminated", regex=True)
        .str.replace(r"disciplin(e|ary)", "disciplined", regex=True)
        .str.replace(r"(counseling|^verbal$)", "verbal counseling", regex=True)
        .str.replace(r"o[cr]al", "verbal counseling", regex=True)
        .str.replace(r"^0$", "none", regex=True)
        .str.replace(r"^3$", "verbal counseling", regex=True)
    )
    return df


def clean_received_by(df):
    df.loc[:, "received_by"] = (
        df.receive_by.str.lower()
        .str.strip()
        .str.replace("/", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace(r"^d\b", "dawn", regex=True)
        .str.replace(r"^panepinto$", "dawn panepinto", regex=True)
        .str.replace(r"^chief$", "", regex=True)
        .str.replace("panepito", "panepinto", regex=False)
        .str.replace(r"^in", "lt", regex=True)
        .str.replace(r"^facebook comments$", "", regex=True)
        .str.replace(r"shuma(haker|cher)", "schumacher", regex=True)
        .str.replace(r"^ry\b", "ryan", regex=True)
        .str.replace("dennise", "denise", regex=False)
        .str.replace("whittingtton", "whittington", regex=False)
    )
    parts = df.received_by.str.extract(r"(?:(lt|cpl|capt|sgt|dy|major|chief) )?(.+)")
    df.loc[:, "received_by"] = parts[1].fillna("")
    return df.drop(columns="receive_by")


def drop_rows_with_allegation_disposition_action_all_empty(df):
    return df[~((df.allegation == "") & (df.disposition == "") & (df.action == ""))]


def clean_completion_date(df):
    df.completion_date = (
        df.completion_date.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^8/1/200$", "8/1/2020", regex=True)
    )
    df = df.loc[~df.index.duplicated(keep="first")]
    return df


def discard_impossible_dates(df):
    df.loc[df.completion_year > "2021", "completion_year"] = ""
    df.loc[df.completion_year > "2021", "completion_month"] = ""
    df.loc[df.completion_year > "2021", "completion_day"] = ""
    return df


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def correct_dates(df):
    df.loc[:, "receive_date"] = df.date_time_received_00_00_hours.astype(
        str
    ).str.replace(r"\'", "", regex=True)
    return df.drop(columns=["date_time_received_00_00_hours"])


def clean_received_by_13(df):
    df.loc[:, "received_by"] = (
        df.complaint_received_by.str.lower()
        .str.strip()
        .str.replace(r"^tpso$", "tangipahoa parish sheriff's office", regex=True)
    )
    return df.drop(columns=["complaint_received_by"])


def clean_supervisor(df):
    names = (
        df.complaint_referred_or_assigned_to.str.lower()
        .str.strip()
        .str.replace(r"^\'", "", regex=True)
        .str.extract(r"(^l[ti][\,\.]|^sg[tl]\.|^capt\.) ?(\w+)")
    )

    df.loc[:, "supervisor_rank_desc"] = (
        names[0]
        .str.replace(r"^l[ti][\,\.]", "lieutenant", regex=True)
        .str.replace(r"^sg[tl]\.", "sergeant", regex=True)
        .str.replace(r"^capt\.", "captain", regex=True)
    )
    df.loc[:, "supervisor_last_name"] = names[1]
    return df.drop(columns=["complaint_referred_or_assigned_to"])


def extract_actions(df):
    actions = (
        df.final_status_and_dispo.str.lower()
        .str.strip()
        .str.extract(r"(terminated|12 hr susp|written reprimand)")
    )
    df.loc[:, "action"] = (
        actions[0]
        .fillna("")
        .str.replace(r"^12 hr susp$", "12-hour suspension", regex=True)
    )
    return df


def clean_disposition13(df):
    df.loc[:, "disposition"] = (
        df.final_status_and_dispo.str.lower()
        .str.strip()
        .str.replace(r"(terminated|12 hr susp|written reprimand)", "", regex=True)
        .str.replace("compl withdrawn", "withdrawn", regex=False)
        .str.replace("discipline", "disciplinary", regex=False)
        .str.replace(r"\'", "", regex=True)
    )
    return df.drop(columns=["final_status_and_dispo"])


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation_charge.str.lower().str.strip().str.replace(r"\'", "", regex=True)
    )
    return df


def split_rows_with_multiple_allegations_13(df):
    i = 0
    for idx in df[df.allegation.str.contains("/")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\/", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df.drop(columns=["allegation_charge"])


def split_names(df):
    names = (
        df.investigated_member_s_name_last_name_first_initial_badge.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"\'", "", regex=True)
        .str.replace(r"unknown", "", regex=True)
        .str.extract(r"(sgt\.)? ?(?:(\w+) )? ?(\w+)")
    )

    df.loc[:, "rank_desc"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "last_name"] = names[2].fillna("")
    return df[~((df.first_name == "") & (df.last_name == ""))].drop(
        columns=["investigated_member_s_name_last_name_first_initial_badge"]
    )


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean21():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_cprr_2015_2021.csv"))
        .pipe(split_rows_with_name)
        .pipe(split_full_name)
        .pipe(clean_dept_desc)
        .pipe(clean_complaint_type)
        .pipe(clean_rule_violation)
        .pipe(clean_investigating_supervisor)
        .pipe(clean_disposition)
        .pipe(clean_appeal)
        .pipe(clean_policy_failure)
        .pipe(clean_submission_type)
        .pipe(clean_received_by)
        .pipe(clean_action)
        .pipe(clean_completion_date)
        .pipe(float_to_int_str, ["level"])
        .pipe(drop_rows_with_allegation_disposition_action_all_empty)
        .pipe(clean_dates, ["completion_date"], expand=True)
        .pipe(clean_dates, ["receive_date"], expand=False)
        .pipe(clean_names, ["first_name", "last_name", "rank_desc"])
        .pipe(discard_impossible_dates)
        .pipe(set_values, {"agency": "tangipahoa-so", "data_production_year": "2021"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .drop_duplicates(subset=["receive_date", "uid", "allegation"], keep="first")
        .pipe(gen_uid, ["receive_date", "uid", "allegation"], "allegation_uid")
        .pipe(
            gen_uid,
            [
                "supervisor_rank",
                "supervisor_first_name",
                "supervisor_last_name",
                "agency",
            ],
            "supervisor_uid",
        )
        .pipe(drop_rows_missing_names)
    )
    return df


def clean13():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_so/tangipahoa_so_cprr_2013.csv"))
        .pipe(clean_column_names)
        .drop(
            columns=[
                "complainant_name_address_age_telephone",
                "air",
                "ccf",
                "per",
                "ph",
                "l",
            ]
        )
        .rename(columns={"air_pid_complaint_number_yr_mo": "tracking_id"})
        .pipe(correct_dates)
        .pipe(clean_dates, ["receive_date"])
        .pipe(clean_received_by_13)
        .pipe(clean_supervisor)
        .pipe(extract_actions)
        .pipe(clean_disposition13)
        .pipe(standardize_desc_cols, ["disposition"])
        .pipe(clean_allegation)
        .pipe(split_rows_with_multiple_allegations_13)
        .pipe(split_names)
        .pipe(set_values, {"agency": "tangipahoa-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "disposition", "tracking_id"],
            "allegation_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


if __name__ == "__main__":
    df21 = clean21()
    df13 = clean13()
    df21.to_csv(deba.data("clean/cprr_tangipahoa_so_2015_2021.csv"), index=False)
    df13.to_csv(deba.data("clean/tangipahoa_so_cprr_2013.csv"), index=False)
