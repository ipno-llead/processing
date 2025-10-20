import pandas as pd
from typing import Optional, List
from lib.columns import clean_column_names, set_values
import deba
from lib.clean import (
    clean_names,
    clean_salaries,
    clean_sexes,
    clean_races,
    float_to_int_str,
    clean_dates,
    strip_leading_comma,
    standardize_desc_cols
)
from lib.uid import gen_uid
from lib import salary
from lib.standardize import standardize_from_lookup_table
from lib.rows import duplicate_row
import re


def split_names(df):
    names = (
        df.name.str.strip()
        .str.lower()
        .str.replace(r"^(.+), pete,", r"pete, \1,", regex=True)
        .str.replace(r"^(\w) (\w{3,}),(.+)", r"\2,\3, \1", regex=True)
        .str.replace(r"^(\w+) (\w), (\w+)$", r"\1, \3, \2", regex=True)
        .str.replace(r"^(\w+), (\w+) (\w)$", r"\1, \2, \3", regex=True)
        .str.replace(r"^(\w+) (\w{2}), (\w+)$", r"\1, \3 \2", regex=True)
        .str.extract(r"^(\w+), ([^,]+)(?:, ([^ ]+))?$")
    )
    df.loc[:, "last_name"] = names[1]
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "middle_name"] = names[2]
    return df[df.name.notna()].drop(columns=["name"])


def standardize_rank(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.replace(r"\bdept\b", "department", regex=True)
        .str.replace(r"\brec\b", "records", regex=True)
        .str.replace("-level ", " ", regex=False)
        .str.replace(r"\bcomm\b", "communications", regex=True)
        .str.replace(r"\bsupv\b", "supervisor", regex=True)
        .str.replace(r"\badmin\b", "administrative", regex=True)
        .str.replace(r"\basst\b", "assistant", regex=True)
        .str.replace(r"\baccredi?ation\b", "accreditation", regex=True)
        .str.replace("/", " to ", regex=False)
        .str.replace(r"^pco$", "communications officer", regex=True)
    )
    return df


def clean_pprr():
    return (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_pprr_2010_2021.csv"))
        .pipe(clean_column_names)
        .drop(columns=["assigned_zone", "badge_number"])
        .rename(
            columns={
                "gender": "sex",
                "year_of_birth": "birth_year",
                "rank": "rank_desc",
                "date_of_termination": "left_date",
                "date_of_hire": "hire_date",
            }
        )
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_races, ["race"])
        .pipe(clean_salaries, ["salary"])
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "data_production_year": "2021",
                "agency": "lafayette-pd",
            },
        )
        .pipe(float_to_int_str, ["birth_year"])
        .pipe(standardize_rank)
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
    )


def clean_tracking_id(df):
    df.loc[:, "tracking_id"] = (
        df.tracking_id.str.lower()
        .str.strip()
        .str.replace(" ", "", regex=False)
        .str.replace(r"(\w+)(\d+)", r"\1 \2", regex=True)
        .str.replace(r"\s+", "", regex=True)
    )
    return df[df.tracking_id != "\x1a"].reset_index(drop=True)


def split_rows_with_multiple_officers(df):
    df.loc[:, "officer"] = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"\.(\w)", r". \1", regex=True)
        .str.replace(r"- +", r"-", regex=True)
        .str.replace("reserve officer", "reserve-officer", regex=False)
    )

    # split rows where officers are clearly delineated
    for pat in [r", *", r"/ *", r" (?=\w{3}\.)", r"; *"]:
        df = (
            df.set_index([col for col in df.columns if col != "officer"])
            .officer.str.split(pat=pat, expand=True)
            .stack()
            .reset_index(drop=False)
            .drop(columns=["level_6"])
            .rename(columns={0: "officer"})
        )

    # split rows where there are no token to split
    pat = r"^(.*[^ ]+ [^ ]+) ([^ ]+ [^ ]+)$"
    rows = (
        df.set_index([col for col in df.columns if col != "officer"])
        .officer.str.extract(pat)
        .stack()
        .reset_index(drop=False)
        .drop(columns=["level_6"])
        .rename(columns={0: "officer"})
    )
    return (
        pd.concat(
            [
                df[~df.officer.str.match(pat)],
                rows,
            ]
        )
        .sort_values("tracking_id")
        .reset_index(drop=True)
    )


def extract_rank(df):
    df.loc[:, "officer"] = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"^and +", "", regex=True)
        .str.replace(r"^cpl\.? ", "corporal ", regex=True)
        .str.replace(r"^det\.? ", "detective ", regex=True)
        .str.replace(r"^sgt\.? ", "sergeant ", regex=True)
        .str.replace(r"^lt\.? ", "lieutenant ", regex=True)
        .str.replace(r"^po ", "officer ", regex=True)
        .str.replace(r"^capt\.? ", "captain ", regex=True)
        .str.replace(r"^off\.? ", "officer ", regex=True)
        .str.replace(r"^officers ", "officer ", regex=True)
        .str.replace("reserve-officer", "reserve officer", regex=False)
    )

    ranks = [
        "corporal",
        "sergeant",
        "lieutenant",
        "officer",
        "captain",
        "reserve officer",
        "communications officer",
        "detective",
    ]
    rank_name = df.officer.str.extract(r"^(?:(%s) )?(.+)" % "|".join(ranks))
    df.loc[:, "rank_desc"] = rank_name[0]
    df.loc[:, "name"] = rank_name[1]
    return df[df.name != "and"].drop(columns=["officer"])


def split_cprr_name(df):
    names = (
        df.name.str.strip()
        .str.replace("unknown", "", regex=False)
        .str.replace(" (3d)", "", regex=False)
        .str.extract(r"^(?:([^ ]+) )?([^ ]+)$")
    )
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns=["name"])


def split_investigator(df):
    df.loc[:, "investigator"] = (
        df.investigator.str.lower()
        .str.strip()
        .str.replace(r"^det\. ?", "detective ", regex=True)
        .str.replace(r"^lt\. ?", "lieutenant ", regex=True)
        .str.replace(r"^sgt\. ?", "sergeant ", regex=True)
        .str.replace(r"^capt\. ?", "captain ", regex=True)
    )

    ranks = ["detective", "lieutenant", "sergeant", "captain", "major"]
    parts = df.investigator.str.extract(
        r"^(?:(%s) )?(?:([^ ]+) )?([^ ]+)$" % "|".join(ranks)
    )
    df.loc[:, "investigator_rank"] = parts[0]
    df.loc[:, "investigator_first_name"] = parts[1]
    df.loc[:, "investigator_last_name"] = parts[2]
    return df


def lower_strip(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.lower().str.strip()
    return df


def split_disposition(df):
    # splitting for 'AD2015-005': 'sustained-benoit-3 days, all others letter of reprimand'
    df.loc[
        (df.tracking_id == "AD2015-005") & (df.last_name == "benoit"),
        "disposition",
    ] = "sustained-3 days"
    df.loc[
        (df.tracking_id == "AD2015-005") & (df.last_name != "benoit"),
        "disposition",
    ] = "sustained-letter of reprimand"

    # splitting for 'AD2015-015': 'sustained- bajat- loc; allred- counseling form'
    df.loc[
        (df.tracking_id == "AD2015-015") & (df.last_name == "bajat"),
        "disposition",
    ] = "sustained-loc"
    df.loc[
        (df.tracking_id == "AD2015-015") & (df.last_name != "allred"),
        "disposition",
    ] = "sustained-counseling form"

    # 'AD2019-004': 'excessive force - not sustained att. to duty - sustained/deficiency'
    df.loc[
        (df.tracking_id == "AD2019-004") & (df.allegation == "excessive force"),
        "disposition",
    ] = "not sustained"
    df.loc[
        (df.tracking_id == "AD2019-004") & (df.allegation != "attention to duty"),
        "disposition",
    ] = "sustained/deficiency"

    # 'AD2019-005': 'terminated - trent (overturned - 10 days) kyle 3 days susp.'
    df.loc[
        (df.tracking_id == "AD2019-005") & (df.first_name == "trent"),
        "disposition",
    ] = "10 days"
    df.loc[
        (df.tracking_id == "AD2019-005") & (df.first_name != "kyle"),
        "disposition",
    ] = "3 days susp"

    # 'CC1801': 'thibodeaux - counseling form; brasseaux - counseling form'
    df.loc[
        (df.tracking_id == "CC1801")
        & ((df.last_name == "thibodeaux") | (df.last_name == "brasseaux")),
        "disposition",
    ] = "counseling form"
    df.loc[
        (df.tracking_id == "CC1801")
        & ~((df.last_name == "thibodeaux") | (df.last_name == "brasseaux")),
        "disposition",
    ] = ""

    return df


def clean_cprr_dates(df):
    df.loc[:, "complete_date"] = (
        df.complete_date.str.strip()
        .str.replace("2B", "28", regex=False)
        .str.replace(r"/(\d)(\d{2})$", r"/\1/\2", regex=True)
        .str.replace(r"/(1\d$)", r"/20\1", regex=True)
    )
    df.loc[:, "receive_date"] = df.receive_date.str.strip().str.replace(
        "i", "/", regex=False
    )
    return df


def split_rows_with_multiple_allegations(df):
    indices_to_remove = []
    records = []
    for idx, row in df.loc[df.allegation.str.contains(";")].iterrows():
        indices_to_remove.append(idx)
        allegations = row.allegation.split("; ")
        dispositions = row.disposition.split("; ")
        values = [
            (k, v)
            for k, v in row.to_dict().items()
            if k not in ["allegation", "disposition"]
        ]
        if len(dispositions) <= 1:
            for allegation in allegations:
                records.append(
                    dict(
                        values
                        + [("allegation", allegation), ("disposition", row.disposition)]
                    )
                )
        elif len(dispositions) == len(allegations):
            for i, allegation in enumerate(allegations):
                records.append(
                    dict(
                        values
                        + [("allegation", allegation), ("disposition", dispositions[i])]
                    )
                )
        else:
            assert dispositions[0] == allegations[0]
            start = 1
            end = 1
            for i, allegation in enumerate(allegations):
                if i == len(allegations) - 1:
                    end = len(dispositions)
                else:
                    while dispositions[end] != allegations[i + 1] and end < len(
                        dispositions
                    ):
                        end += 1
                records.append(
                    dict(
                        values
                        + [
                            ("allegation", allegation),
                            ("disposition", "; ".join(dispositions[start:end])),
                        ]
                    )
                )
                start = end + 1
    return (
        pd.concat(
            [
                df.drop(index=indices_to_remove),
                pd.DataFrame.from_records(records),
            ]
        )
        .sort_values("tracking_id")
        .reset_index(drop=True)
    )


def split_action_from_disposition(df):
    dispositions = [
        "sustained",
        "not sustained",
        "unfounded",
        "exonerated",
    ]
    disp_action = df.disposition.str.extract(
        r"^(?:(%s)(?:; (.+))?|(.+))$" % ("|".join(dispositions))
    )
    df.loc[:, "action"] = disp_action[1].fillna("").str.cat(disp_action[2].fillna(""))
    df.loc[:, "disposition"] = disp_action[0]
    return df


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean_cprr_20():
    return (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_cprr_2015_2020.csv"))
        .pipe(clean_column_names)
        .dropna(how="all")
        .rename(
            columns={
                "cc_number": "tracking_id",
                "complaint": "allegation",
                "date_received": "receive_date",
                "date_completed": "complete_date",
                "assigned_investigator": "investigator",
                "focus_officer_s": "officer",
            }
        )
        .pipe(clean_tracking_id)
        .pipe(split_rows_with_multiple_officers)
        .pipe(extract_rank)
        .pipe(split_cprr_name)
        .pipe(split_investigator)
        .pipe(clean_cprr_dates)
        .pipe(lower_strip, ["allegation", "disposition"])
        .pipe(
            standardize_from_lookup_table,
            "allegation",
            [
                ["attention to duty", "att. to duty", "attention", "atd"],
                ["professional conduct", "prof. conduct", "prof conduct", "pc"],
                ["violation of pursuit policy", "pursuit violation", "pursuit policy"],
                [
                    "rude and unprofessional",
                    "rude & unprofessional",
                    "rude/unprofessi onal",
                    "unprof",
                ],
                ["excessive force"],
                ["cubo"],
                ["bwc"],
                ["untruthful"],
                ["tech management"],
                ["lt. scott morgan"],
                ["use of force"],
                ["disobey direct order", "disobeyed direct order"],
                ["vehicle crashes", "vehicle crash"],
                ["accident review"],
                ["failure to report accident"],
                ["handling of evidence"],
                ["vehicle pursuit"],
                ["social media"],
                ["insubordination"],
                ["arrest"],
                ["misappropriation of funds"],
                ["threats"],
                ["officer involved shooting"],
                ["racial profiling"],
                ["substance abuse policy"],
                ["off duty security"],
                ["general conduct fighting"],
                ["criminal violation"],
                [
                    "professional conduct and responsibilities",
                    "professional conduct & resp.",
                    "professional conduct  & responsibilities",
                    "prof. conduct & resp.",
                    "prof. conduct & responsibilities",
                ],
                ["residency requirement"],
                ["theft"],
                ["late reports"],
                ["oen conduct sick leave"],
                ["falsifying records"],
                ["drug policy"],
                [
                    "managing recovered, found, or seized property",
                    "managing recovered property",
                ],
                ["failure to supervise"],
                ["civil rights violation"],
                ["failed to attend inservice"],
                ["ois"],
                ["reckless driving", "speeding"],
                ["operation of police vehicle"],
                ["destruction of evidence"],
                ["violation of informant policy"],
                ["policy nol."],
                ["reports"],
                ["rumors"],
            ],
        )
        .pipe(split_disposition)
        .pipe(
            standardize_from_lookup_table,
            "disposition",
            [
                ["sustained", "sust.", "sus"],
                ["not sustained"],
                ["unfounded"],
                ["exonerated"],
                ["justified use of force", "justified uof", "j.u.f."],
                ["letter of reprimand", "lor"],
                ["letter of counseling"],
                ["letter of caution", "loc"],
                ["termination", "terminated"],
                ["training issue", "trainin g issue"],
                ["counseling form", "cf", "c.f."],
                ["suspension 1 day", "1 day suspension", "1 day"],
                ["suspension 2 days", "2 day sus"],
                ["suspension 3 days", "3 days", "3 days susp"],
                ["suspension 5 days", "suspended 5 days", "5 day suspension", "5 days"],
                ["suspension 7 days", "7 day sus"],
                ["suspension 10 days", "10 day sus", "10 days"],
                ["suspension 14 days", "14 day suspension"],
                ["suspension 30 days", "3oday", "30 days"],
                ["suspension 45 days", "45"],
                ["suspension 60 days", "60 day suspension"],
                ["suspension 90 days", "90 suspension"],
                ["suspension"],
                ["6 months probation", "6months probation"],
                ["sensitivity training"],
                ["1 year no vehicle"],
                ["40 hours driving course", "40 hr. driving course"],
                ["resigned", "res.", "res"],
                ["justified"],
                ["retired"],
                ["deficiency", "def."],
                ["excessive force"],
                ["cubo"],
                ["eap"],
                ["disobey direct order"],
                ["terminated overturned by civil service"],
                ["demotion", "demontion"],
                ["performance log"],
                ["justified shooting"],
                ["counseling form for not using necessary force"],
                ["complaint withdrawn", "withdrawn"],
                ["bwc"],
                ["special evaluation", "special eval"],
                ["professional conduct", "prof conduct"],
                ["handling of evidence", "evidence"],
            ],
        )
        .pipe(split_rows_with_multiple_allegations)
        .pipe(split_action_from_disposition)
        .pipe(set_values, {"data_production_year": 2020, "agency": "lafayette-pd"})
        .pipe(
            clean_names,
            [
                "first_name",
                "last_name",
                "investigator_first_name",
                "investigator_last_name",
            ],
        )
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            ["agency", "investigator_first_name", "investigator_last_name"],
            "investigator_uid",
        )
        .pipe(
            gen_uid,
            ["agency", "tracking_id", "allegation", "uid"],
            "allegation_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )


def clean_tracking_id_14(df):
    df.loc[:, "tracking_id"] = (
        df.cc_number.str.lower()
        .str.strip()
        .str.replace(r"^-", "", regex=True)
        .str.replace(r"sl(\d{2})", r"sl \1", regex=True)
        .str.replace(r"\s+", "", regex=True)
    )
    return df.drop(columns="cc_number")


def clean_receive_date_14(df):
    df.loc[:, "receive_date"] = (
        df.date_received.str.replace("0517/2010", "05/17/2010", regex=False)
        .str.replace(r" \$", "", regex=True)
        .str.replace("20100", "2010", regex=False)
        .str.replace(r"^0/.+", "", regex=True)
    )  # discard dates with empty month
    return df.drop(columns="date_received")


def clean_complete_date_14(df):
    df.loc[:, "complete_date"] = (
        df.date_completed.str.replace("Arwood", "", regex=False)
        .str.replace(r" \-", "", regex=True)
        .str.replace(r" as", "", regex=True)
    )
    return df.drop(columns="date_completed")


def clean_and_split_investigator_14(df):
    df.loc[:, "investigator"] = (
        df.assigned_investigator.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^det\. ?", "detective ", regex=True)
        .str.replace(r"^lt\. ?", "lieutenant ", regex=True)
        .str.replace(r"^sgt\. ?", "sergeant ", regex=True)
        .str.replace(r"^capt\. ?", "captain ", regex=True)
        .str.replace(r"^cpl\.? ?", "corporal ", regex=True)
        .str.replace(r"(.+) terro", "detective shawn terro", regex=True)
        .str.replace(r"(.+) pattum$", "detective patrick pattum", regex=True)
        .str.replace(
            r"(.+)? ?(arm?wood|chastity)$", "detective chastity armwood", regex=True
        )
        .str.replace(r"det\. (.+) prevost", "detective joey prevost", regex=True)
        .str.replace(r"cpl\. (.+) prevost", "detective joey prevost", regex=True)
        .str.replace(r"(.+) gremillion", "sergeant keith gremillion", regex=True)
        .str.replace("none assigned", "", regex=False)
        .str.replace(r"p\. fontenot", "phil fontenot", regex=True)
        .str.replace(r"b\. bejsovec", "bert bejsovex", regex=True)
        .str.replace(r"(r\.)? czajkowski", " ron czajkowski", regex=True)
        .str.replace(r"l\. firmin", "levy firmin", regex=True)
        .str.replace(r"r\. miller", "randy miller", regex=True)
        .str.replace(r"l\. richard", "luranie richard", regex=True)
        .str.replace(r"d\. prejean", "dwayne prejean", regex=True)
        .str.replace("n/a", "", regex=False)
        .str.replace("force", "", regex=False)
        .str.replace(r" 3$", "", regex=True)
    )

    ranks = ["detective", "lieutenant", "sergeant", "captain", "major", "corporal"]
    parts = df.investigator.str.extract(
        r"^(?:(%s) )?(?:([^ ]+) )?([^ ]+)$" % "|".join(ranks)
    )
    df.loc[:, "investigator_rank"] = parts[0].fillna("")
    df.loc[:, "investigator_first_name"] = (
        parts[1].fillna("").str.replace(r"\.", "", regex=True)
    )
    df.loc[:, "investigator_last_name"] = parts[2].fillna("")
    return df.drop(columns=["investigator", "assigned_investigator"])


def extract_action_from_disposition_14(df):
    df.loc[:, "action"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"^sus?tained ?\/ ?(.+) ?", r"\1", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"lor", "letter of reprimand", regex=False)
        .str.replace(
            r"(not sustained|unfounded|withdrawn|unfounded on all officers|"
            r"\bsust\b|complaint|exonorated|sent to hr)",
            "",
            regex=True,
        )
        .str.replace(
            r"(officer resigned ?(prior)? ?(to)? ?(termination)?|resigned sts|"
            r"resigned under invest)",
            "resigned",
            regex=True,
        )
        .str.replace(
            r"(all focus officers listed received counseling forms|"
            r"counseli ?ng form)|loc",
            "letter of counseling",
            regex=True,
        )
        .str.replace("(retired under investigation)", "retired", regex=True)
        .str.replace(
            r" ?(\d+) days? ?(suspension)? ?", r"\1-day suspension ", regex=True
        )
        .str.replace(
            r"excessive force/ failure to complete report/sustained (2days)|^2days$",
            "2-day suspension",
            regex=True,
        )
    )
    actions = df.action.str.extract(
        r"((.+)suspension(.+)|(.+)terminated(.+)|(.+)resigned(.+)|"
        r"(.+)letter of counseling(.+)|(.+)letter of reprimand(.+))"
    )

    df.loc[:, "action"] = (
        actions[0]
        .fillna("")
        .str.replace(
            r"(.+)letter of counseling(.+)", "letter of counseling", regex=True
        )
        .str.replace(r"(\w+)3", "3", regex=True)
        .str.replace(r" \((\w+)\)$", "", regex=True)
        .str.replace("terminated  resigned/", "terminated; resigned", regex=False)
    )
    return df


disposition_14_lookup = [
    [
        "sustained; resigned",
        "sustained/nboc/resigned",
        "sustained/resigned prior to termination",
        "sustained/resigned",
        "sustained/resigned under invest.",
        "terminated sust. resigned/",
        "resigned sts.",
    ],
    ["unfounded", "/loc/sta rling- unfounded- crozier"],
    ["exonerated", "exonorated"],
    [
        "justified",
        "justified use of force",
        "justified use of",
        "justified/use of force",
    ],
    ["not sustained; sustained", "not sustained/anboc- sustained deficiency"],
    [
        "not sustained",
        "; not sustained- deroussel",
        "not sustained/ds- counseling form (ryan beard) unprofessional conduct",
    ],
    ["not sustained; resigned", "not sustained/resigned"],
    [
        "sustained",
        "sustained/lor",
        "sustained/loc",
        "sustained/training",
        "ned fowler- sustained- cubo/letter of repremaind gabe thompson- sustained- cubo/counseling form",
    ],
    ["withdrawn"],
    ["no violation", "no violation et", "noviolation/close"],
    ["resigned", "/resigned"],
    ["retired", "retired under investigation"],
]


def clean_disposition_14(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(
            r"sus?tained ?\/? ?((.+)(\d+)(.+)| ?letter(.+)| ?coun(.+)| ?training(.+)|"
            r" ?(.+)lor(.+)|performance(.+))",
            "sustained",
            regex=True,
        )
        .str.replace(r"\/ (\w+)", r"/\1", regex=True)
        .str.replace("officer ", "", regex=False)
    )
    return standardize_from_lookup_table(df, "disposition", disposition_14_lookup)


def clean_allegation_14(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"\(?(\w+)\)?\/ (\w+)", r"\1/\2", regex=True)
        .str.replace(r"unpo?r?o?fesso?i?o?nal", "unprofessional", regex=True)
        .str.replace(
            r"^officer involved ?\.? shooting", "officer involved shooting", regex=True
        )
        .str.replace("failed", "failure", regex=False)
        .str.replace(r"\((\w+)$", r"(\1)", regex=True)
        .str.replace("pd", "police department", regex=False)
        .str.replace("rude/unprofessional", "rude and unprofessional", regex=False)
        .str.replace(r" \b(her|his)\b ", "", regex=True)
        .str.replace(r"^ffde$", "", regex=True)
        .str.replace(r"\((\w+)\/(\w+)\)", r"(\1|\2)", regex=True)
        .str.replace("topatrol", "to patrol", regex=False)
        .str.replace("speeding", "safe speed violation", regex=False)
        .str.replace(r"^complete report$", "failure to complete report", regex=True)
        .str.replace(r"^rude and$", "rude and unprofessional", regex=True)
        .str.replace(r"duites", "duties", regex=False)
        .str.replace("onapplication", "on application", regex=False)
    )
    return df.drop(columns="complaint")


def split_rows_with_multiple_allegations_14(df):
    i = 0
    for idx in df[df.allegation.str.contains(r"/")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df


def split_rows_with_multiple_names_14(df):
    df.loc[:, "focus_officer_s"] = (
        df.focus_officer_s.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"\, ", "/", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace("unknown", "", regex=False)
        .str.replace(r"\((\w+)\/(\w+)\)", r"(\1|\2)", regex=True)
        .str.replace(r"\(?(\w+)\)? ?\/ (\w+)", r"\1/\2", regex=True)
        .str.replace(r"sgt\.?", "sergeant", regex=True)
        .str.replace(r"det\.?", "detective", regex=True)
        .str.replace(r"cpl\.?", "corporal", regex=True)
        .str.replace(r"capt\.?", "captain", regex=True)
        .str.replace(r"^lt\.?", "lieutenant", regex=True)
        .str.replace(r"officer \- (\w+)", r"officer \1", regex=True)
        .str.replace(r"\/and ", "/", regex=True)
        .str.replace(
            "officers jeremy dupuis (csu) and ross sonnier (1a)",
            "officer jeremy dupuis (csu)/officer ross sonnier (1a)",
            regex=False,
        )
        .str.replace(
            r"detective will white\/detective brian baumgardner\/captain ned fowler\/detective michael boutte\/"
            r"sergeant gabe thompson, \(all metro narcotics agents\) pat elliot \(da's office\)",
            "detective will white (metro narcotics)/detective brian baumgardner (metro narcotics)/"
            "captain ned fowler (metro narcotics)/detective michael boutte (metro narcotics)/"
            "sergeant gabe thompson (metro narcotics)/pat elliot (da's office)",
            regex=True,
        )
        .str.replace(
            r"officer calvin parker\(1d\) officer ross sonnier \(1d\)",
            "officer calvin parker 1d/officer ross sonnier 1d",
            regex=True,
        )
        .str.replace("officers", "officer", regex=False)
        .str.replace(r"(\w+)\((\w+)\)$", r"\1 \2", regex=True)
        .str.replace("1c", "1c", regex=False)
        .str.replace(r"\(k\- (\w+)\)?", r"(k-\1)", regex=True)
        .str.replace(r"(\w+) officer", r"\1/officer", regex=True)
        .str.replace(r"\b(\w{1})c$", r"(\1c)", regex=True)
        .str.replace(r"\b(\w{1})d$", r"(\1d)", regex=True)
        .str.replace(r"^lpd ", "", regex=True)
        .str.replace(r"and ?", "", regex=True)
        .str.replace("morvant 1c", "officer morvant (1c)")
        .str.replace("detectiveail", "detective", regex=False)
        .str.replace(r"(\((\w{2})\)) (.+)", r"\1/\2", regex=True)
        .str.replace(
            r"^greg cormier scott poiencot$", "greg cormier/scott poiencot", regex=True
        )
    )

    i = 0
    for idx in df[df.focus_officer_s.str.contains(r"/")].index:
        s = df.loc[idx + i, "focus_officer_s"]
        parts = re.split(r"\s*(?:/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "focus_officer_s"] = name
        i += len(parts) - 1
    return df


def split_names_14(df):
    df.loc[:, "focus_officer_s"] = (
        df.focus_officer_s.str.replace(
            r"^res lor$", "detective res lor (metro narcotics)", regex=True
        )
        .str.replace(r"^allred$", "sergeant walter allred", regex=True)
        .str.replace(r"^lange$", "lieutenant joseph lange (4b)", regex=True)
        .str.replace(r"action\)$", "", regex=True)
        .str.replace(r"^reserve$", "", regex=True)
        .str.replace(r"^\(k-9\)$", "", regex=True)
        .str.replace(r"\bsro\b", "school resources", regex=True)
        .str.replace(r"traffic\)$", "", regex=True)
        .str.replace(r"^4[db]$", "", regex=True)
        .str.replace(r"^2b$", "", regex=True)
        .str.replace("lpd- eeoc complain", "", regex=True)
        .str.replace(r"metro narc\)", "metro narcotics", regex=True)
        .str.replace(r"^officer\)$", "", regex=True)
        .str.replace(
            r"(officer)? ?(uletom) ?(hewitt)? ?(\(?1b\)?)?$",
            "officer uletom hewitt (1b)",
            regex=True,
        )
        .str.replace(
            r"\(all metro narcotics agents\) pat elliot \(da's office\)",
            "pat elliot (district attorney's office)",
            regex=True,
        )
        .str.replace(r"^metro narcotics", "", regex=True)
    )
    names = df.focus_officer_s.str.extract(
        r"(?:(officer|detective|sergeant|lieutenant|city marshall|major|recruit|"
        r"captain|corporal|reserve captain|dispatcher|drc|chief|park ranger))? "
        r"?(?:(\w+) )? ?(\w+) ?(.+)?"
    )
    df.loc[:, "rank_desc"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "last_name"] = (
        names[2].fillna("").str.replace(r"bertr$", "bertrand", regex=True)
    )
    df.loc[:, "department_desc"] = (
        names[3]
        .fillna("")
        .str.replace(r"\,", "", regex=True)
        .str.replace(r"\(\(?|\)\)?", "", regex=True)
        .str.replace("cid", "criminal investigations", regex=False)
    )
    return df.drop(columns=["focus_officer_s"])


def drop_rows_missing_allegation_disposition_and_action_14(df):
    return df[~((df.allegation == "") & (df.action == "") & (df.disposition == ""))]


def assign_correct_actions_14(df):
    df.loc[
        ((df.first_name == "brent") & (df.tracking_id == "2012-008")), "action"
    ] = "3-day suspension"
    df.loc[
        ((df.first_name == "devin") & (df.tracking_id == "2012-008")), "action"
    ] = "1-day suspension"
    df.loc[
        ((df.last_name == "firmin") & (df.tracking_id == "2009-002")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "starling") & (df.tracking_id == "sl 13-006")), "action"
    ] = "letter of counseling"
    df.loc[
        ((df.last_name == "hebert") & (df.tracking_id == "2011-012")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_id == "2011-006")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "hackworth") & (df.tracking_id == "2012-022")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "taylor") & (df.tracking_id == "ad 13-001")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "taylor") & (df.tracking_id == "ad 13-002")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "baumgardner") & (df.tracking_id == "2012-015")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "baumgardner") & (df.tracking_id == "2012-013")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "dangerfield") & (df.tracking_id == "2012-001")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "thompson") & (df.tracking_id == "2012-010")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "poiencot") & (df.tracking_id == "2012-010")), "action"
    ] = "terminated"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_id == "2011-004")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_id == "2011-001")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "bricker") & (df.tracking_id == "2011-007")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "carter") & (df.tracking_id == "2010-011")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "dartez") & (df.tracking_id == "2010-012")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "howard") & (df.tracking_id == "2009-008")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "winjum") & (df.tracking_id == "2010-014")), "action"
    ] = "resigned"
    df.loc[
        (
            (df.last_name == "bertrand")
            & (df.tracking_id == "2012-003")
            & (df.allegation == "insubordination")
        ),
        "action",
    ] = ""
    return df


def assign_correct_disposition_14(df):
    df.loc[
        ((df.last_name == "crozier") & (df.tracking_id == "sl 13-006")),
        "disposition",
    ] = "unfounded"
    df.loc[
        ((df.last_name == "starling") & (df.tracking_id == "sl 13-006")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "sonnier") & (df.tracking_id == "sl 13-006")),
        "disposition",
    ] = ""
    df.loc[
        ((df.last_name == "firmin") & (df.tracking_id == "2009-002")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "hebert") & (df.tracking_id == "2011-012")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_id == "2011-006")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "hackworth") & (df.tracking_id == "2012-022")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "taylor") & (df.tracking_id == "ad 13-001")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "taylor") & (df.tracking_id == "ad 13-002")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "baumgardner") & (df.tracking_id == "2012-015")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "baumgardner") & (df.tracking_id == "2012-013")),
        "disposition",
    ] = "not sustained"
    df.loc[
        ((df.last_name == "dangerfield") & (df.tracking_id == "2012-001")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "thompson") & (df.tracking_id == "2012-010")),
        "disposition",
    ] = ""
    df.loc[
        ((df.last_name == "poiencot") & (df.tracking_id == "2012-010")),
        "disposition",
    ] = ""
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_id == "2011-004")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_id == "2011-001")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "bricker") & (df.tracking_id == "2011-007")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "carter") & (df.tracking_id == "2010-011")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "dartez") & (df.tracking_id == "2010-012")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "howard") & (df.tracking_id == "2009-008")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "winjum") & (df.tracking_id == "2010-014")), "disposition"
    ] = "sustained"
    df.loc[
        (
            (df.last_name == "bertrand")
            & (df.tracking_id == "2012-003")
            & (df.allegation == "insubordination")
        ),
        "disposition",
    ] = "unfounded"
    df.loc[
        (
            (df.last_name == "bertrand")
            & (df.tracking_id == "2012-003")
            & (df.allegation == "rude and unprofessional")
        ),
        "disposition",
    ] = "sustained"
    return df


def clean_cprr_14():
    df = (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_cprr_2009_2014.csv"))
        .pipe(clean_column_names)
        .drop(columns=["complainant"])
        .pipe(clean_receive_date_14)
        .pipe(clean_complete_date_14)
        .pipe(clean_dates, ["receive_date", "complete_date"])
        .pipe(clean_tracking_id_14)
        .pipe(clean_and_split_investigator_14)
        .pipe(extract_action_from_disposition_14)
        .pipe(clean_disposition_14)
        .pipe(clean_allegation_14)
        .pipe(split_rows_with_multiple_allegations_14)
        .pipe(split_rows_with_multiple_names_14)
        .pipe(split_names_14)
        .pipe(assign_correct_actions_14)
        .pipe(assign_correct_disposition_14)
        .pipe(drop_rows_missing_allegation_disposition_and_action_14)
        .pipe(set_values, {"agency": "lafayette-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["agency", "investigator_first_name", "investigator_last_name"],
            "investigator_uid",
        )
        .pipe(
            gen_uid,
            ["uid", "allegation", "action", "tracking_id", "disposition"],
            "allegation_uid",
        )
        .pipe(drop_rows_missing_names)
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df

import re

def clean_tracking_id_25(df: pd.DataFrame, col: str = "tracking_id_og") -> pd.DataFrame:
    def _clean(val: str) -> str:
        if pd.isna(val):
            return val
        val = str(val).strip().lstrip("'\\").rstrip()

        # remove stray commas, periods, multiple spaces
        val = re.sub(r"[ ,\.]+", "-", val)

        # fix common OCR/typo errors
        val = re.sub(r"^5L", "SL", val, flags=re.IGNORECASE)
        val = re.sub(r"^S1", "SL", val, flags=re.IGNORECASE)
        val = re.sub(r"^S0", "SO", val, flags=re.IGNORECASE)
        val = re.sub(r"^A0", "AD", val, flags=re.IGNORECASE)
        val = re.sub(r"^SC0", "SC", val, flags=re.IGNORECASE)
        val = re.sub(r"^SLO", "SL0", val, flags=re.IGNORECASE)
        val = re.sub(r"^SCD", "SC", val, flags=re.IGNORECASE)

        # collapse multiple dashes/spaces
        val = re.sub(r"[-\s]+", "-", val)

        # all lowercase
        val = val.lower()

        return val

    df = df.copy()
    df["tracking_id_og"] = df[col].apply(_clean)
    return df

def clean_complaint(df: pd.DataFrame, col: str = "complaint") -> pd.DataFrame:
    def _clean(val: str) -> str:
        if pd.isna(val):
            return val
        val = str(val).strip().lstrip("'").rstrip()

        # normalize spacing and punctuation
        val = re.sub(r"[\.]+", " ", val)          # remove stray periods
        val = re.sub(r"\s{2,}", " ", val)         # collapse multiple spaces
        val = val.strip().lower()

        # standardize common variations / typos
        replacements = {
            "prof conduct": "professional conduct",
            "prof cond": "professional conduct",
            "prof": "professional conduct",
            "poot conduct": "professional conduct",
            "general conduct professional conduct": "professional conduct",
            "professional conduct and responsibilities": "professional conduct",
            "professional conduct/insubordination": "professional conduct / insubordination",
            "professional conduct / insubordination": "professional conduct / insubordination",
            "professional conduct/owi": "professional conduct / owi",
            "bwc": "body worn camera",
            "uof": "use of force",
            "ois": "officer involved shooting",
            "o.i.s": "officer involved shooting",
            "improper search": "improper search",
            "improper supervision": "improper supervision",
            "rude and unprof": "rude and unprofessional",
            "unauthoried absence": "unauthorized absence",
            "absent w/o approved leave": "unauthorized absence",
            "left stift e.rly": "left shift early",
            "left shat early": "left shift early",
            "misuse of sick leave": "sick leave misuse",
            "abuse of sick leave": "sick leave misuse",
            "drug screen policy": "drug screening policy",
            "cubo": "conduct unbecoming an officer",
            "payroll fraud": "payroll fraud",
            "retail theft": "theft",
            "sexual harassment": "sexual harassment",
            "left stift e rly": "left shift early",
        }
        for k, v in replacements.items():
            if val == k:
                val = v

        return val

    df = df.copy()
    df["allegation"] = df[col].apply(_clean)
    return df.drop(columns=[col])

def clean_allegation_from_complainant(df: pd.DataFrame, col: str = "complainant") -> pd.DataFrame:
    def _clean(val: str) -> str:
        if pd.isna(val):
            return val
        val = str(val).strip().lstrip("'").rstrip()

        # drop if it looks like a date
        if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", val):
            return None

        val = val.lower()
        val = re.sub(r"\s{2,}", " ", val)

        # standardize common variations
        replacements = {
            "prof conduct": "professional conduct",
            "prof. conduct": "professional conduct",
            "prof. conduct & responsibilities": "professional conduct",
            "prof conduct/arrest": "professional conduct / arrest",
            "prof conduct/social media": "professional conduct / social media",
            "prof conduct/handling of evidence": "professional conduct / handling of evidence",
            "cubo": "conduct unbecoming an officer",
            "ois": "officer involved shooting",
            "response to resistance / use of force": "use of force",
            "discharge of firearm / use of force": "use of force",
            "excessive force": "excessive force",
            "att. to duty": "attention to duty",
            "attention to duty operation of police vehicles emergency vehicles; exemptions": "attention to duty",
        }
        for k, v in replacements.items():
            if val == k:
                val = v

        return val

    df = df.copy()
    df["allegation_desc"] = df[col].apply(_clean)
    return df.drop(columns=[col])

def clean_disposition_simple(df: pd.DataFrame, col: str = "disposition") -> pd.DataFrame:
    """
    Standard text cleaning for disposition values (single output column).
    - lowercase
    - strip leading quotes
    - fix common typos/abbrevs
    - normalize separators to '; '
    - normalize suspension forms to: 'suspension (N days)'
    - trim extra whitespace
    writes: 'disposition_std'
    """
    df = df.copy()

    # compile reusable regex
    ws_multi = re.compile(r"\s+")
    leading_quotes = re.compile(r"^'+")
    paren_names = re.compile(r"\([^)]*\)")  # drops parenthetical name lists, if present

    # pairs of (pattern, replacement) applied in order
    replacements = [
        # normalize separators first
        (r"[|/,]+", ";"),
        (r";{2,}", ";"),
        (r"\s*;\s*", "; "),

        # strip obvious noise
        (r"\b\(all\)\b|\b\(s\)\b", ""),
        (r"\buse of force\b", "use of force"),  # keep if present
        (r"\bois\b", "officer involved shooting"),
        (r"\bjustified uof\b", "justified"),
        (r"\bexonerated\s+justified(?:\s+use of force)?\b", "exonerated; justified"),

        # findings
        (r"\bnon[ -]?sustained\b", "not sustained"),
        (r"\bnot\s*sust\w*\b", "not sustained"),
        (r"\bsustainet\b|\bsustened\b|\bsustamed\b|\bsastasned\b", "sustained"),
        (r"\bnet sustained\b|\bout sustained\b|\bout sustand\b", "not sustained"),
        (r"\bexonerated\b", "exonerated"),
        (r"\bunfounded\b", "unfounded"),
        (r"\bpending\b", "pending"),
        (r"\bjustified\b", "justified"),

        # actions / documents
        (r"\blor\b|\bl\.?o\.?r\.?\b|\bletter of reprimand\b", "letter of reprimand"),
        (r"\bloc\b", "letter of counseling"),
        (r"\bcf\b|\bc\/?f\b", "counseling form"),
        (r"\bcouns\w*\b|\bcours?il\w*\b", "counseling form"),
        (r"\bperformance\s*log\b|\bpent\.?\s*log\b|\bpert\.?\s*cog\b|\bperf\.?\s*log\b|\bpent\.?\s*log\b", "performance log"),
        (r"\bdefic\w*\b", "deficiency"),
        (r"\bno further action\b", "no further action"),
        (r"\beap\b", "eap counseling"),
        (r"\bspecial eval\b", "special evaluation"),
        (r"\bloss of extra duty\b", "loss of extra duty"),
        (r"\bdemontion\b", "demotion"),

        # employment outcomes
        (r"\bterminated\b", "termination"),
        (r"\btermination\b", "termination"),
        (r"\bresigned\s*\(in lieu of termination\)\b", "resigned (in lieu of termination)"),
        (r"\bresigned\s*under investigation\b", "resigned (under investigation)"),
        (r"\bresigned\b", "resigned"),

        # suspension normalization
        (r"\bsus\.?\b|\bsuspen\w*\b", "suspension"),
        (r"\b(\d+)\s*day[s]?\b", r"\1 days"),
        (r"\b(\d+)\s*days\s*suspension\b", r"suspension (\1 days)"),
        (r"suspension\s*\((\d+)\s*days?\)", r"suspension (\1 days)"),
        (r"suspension\s*(\d+)\b", r"suspension (\1 days)"),
        (r"\b(\d+)\s*days\b(?=.*suspension)", r"\1 days"),  # keep days if suspension elsewhere
    ]

    def _clean_one(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).lower().strip()
        s = leading_quotes.sub("", s)
        s = paren_names.sub("", s)

        # unify separators early to make patterns simpler
        s = re.sub(r"[–—-]/", ";", s)
        s = re.sub(r"[–—-]", " - ", s)  # normalize weird dashes to spaces

        # apply replacements
        for pat, repl in replacements:
            s = re.sub(pat, repl, s)

        # collapse multiple separators/spaces
        s = re.sub(r"\s*;\s*", "; ", s)
        s = re.sub(r";\s*;", "; ", s)
        s = ws_multi.sub(" ", s).strip(" ;")

        return s or pd.NA

    df["disposition_std"] = df[col].apply(_clean_one)
    return df

import pandas as pd
import re

def clean_disposition_inplace(df: pd.DataFrame, col: str = "disposition") -> pd.DataFrame:
    """
    Clean and standardize a free-text disposition column IN PLACE.
    - lowercase
    - strip leading quotes
    - normalize separators to '; '
    - fix common typos/abbreviations
    - normalize suspension syntax to 'suspension (N days)'
    - deduplicate tokens while preserving order
    Returns the modified DataFrame with the SAME `col` updated.
    """
    df = df.copy()

    # ordered (pattern -> replacement)
    replacements = [
        # normalize separators early
        (r"[|/,]+", ";"),
        (r"\s*-\s*", "; "),            # 'sustained - deficiency' -> 'sustained; deficiency'
        (r";{2,}", ";"),
        (r"\s*;\s*", "; "),

        # strip common noise
        (r"^'+", ""),                  # leading quotes
        (r"\b\(all\)\b|\b\(s\)\b", ""),# "(All)", "(S)"
        (r"\s+", " "),                 # collapse spaces

        # findings
        (r"\bnon[ -]?sustained\b", "not sustained"),
        (r"\bnot\s*sust\w*\b", "not sustained"),
        (r"\bnet sustained\b|\bout sustained\b|\bout sustand\b", "not sustained"),
        (r"\bsustainet\b|\bsustened\b|\bsustamed\b|\bsastasned\b", "sustained"),
        (r"\bexonerated\s+justified(?:\s+use of force)?\b", "exonerated; justified"),
        (r"\bjustified uof\b", "justified"),

        # actions / docs
        (r"\blor\b|\bl\.?o\.?r\.?\b|\bletter of reprimand\b", "letter of reprimand"),
        (r"\bloc\b", "letter of counseling"),
        (r"\bcf\b|\bc\/?f\b", "counseling form"),
        (r"\bcouns\w*\b|\bcours?il\w*\b", "counseling form"),
        (r"\bperformance\s*log\b|\bpent\.?\s*log\b|\bpert\.?\s*cog\b|\bperf\.?\s*log\b", "performance log"),
        (r"\bdefic\w*\b", "deficiency"),
        (r"\bno further action\b", "no further action"),
        (r"\beap\b", "eap counseling"),
        (r"\bspecial eval\b", "special evaluation"),
        (r"\bloss of extra duty\b", "loss of extra duty"),
        (r"\bdemontion\b", "demotion"),

        # employment outcomes
        (r"\bterminated\b", "termination"),
        (r"\btermination\b", "termination"),
        (r"\bresigned\s*\(in lieu of termination\)\b", "resigned (in lieu of termination)"),
        (r"\bresigned\s*under investigation\b", "resigned (under investigation)"),
        (r"\bresigned\b", "resigned"),

        # incident shorthand
        (r"\bois\b", "officer involved shooting"),

        # suspension normalization
        (r"\bsus\.?\b|\bsuspen\w*\b", "suspension"),
        (r"\b(\d+)\s*day[s]?\b", r"\1 days"),                     # '14 day' -> '14 days'
        (r"\b(\d+)\s*days\s*suspension\b", r"suspension (\1 days)"),
        (r"suspension\s*\((\d+)\s*days?\)", r"suspension (\1 days)"),
        (r"suspension\s*(\d+)\b", r"suspension (\1 days)"),
        (r"(\d+)\s*days\b(?=.*suspension)", r"\1 days"),          # keep 'N days' if suspension elsewhere
    ]

    def _clean_one(x):
        if pd.isna(x):
            return pd.NA
        s = str(x).lower().strip()

        # apply replacements in order
        for pat, repl in replacements:
            s = re.sub(pat, repl, s)

        # tidy separators/spaces
        s = re.sub(r"\s*;\s*", "; ", s)
        s = re.sub(r";\s*;", "; ", s)
        s = s.strip(" ;")

        # de-duplicate tokens while preserving order
        if ";" in s:
            parts = [p.strip() for p in s.split(";") if p.strip()]
            seen = set()
            uniq = []
            for p in parts:
                if p not in seen:
                    seen.add(p)
                    uniq.append(p)
            s = "; ".join(uniq)

        return s or pd.NA

    df[col] = df[col].apply(_clean_one)
    return df

import re
import pandas as pd
from datetime import datetime

def clean_and_split_dates(df,
                          receive_col="receive_date",
                          complete_col="complete_date",
                          year_min=2000,
                          year_max=2035,
                          zero_pad=False):
    """
    Clean two messy date columns and create split Y/M/D columns as *strings*.
    Invalid/unparseable parts become '' (empty string).
    Outputs:
      receive_year, receive_month, receive_day,
      complete_year, complete_month, complete_day
    All are pandas 'string' dtype (not Int64).
    """

    df = df.copy()

    alpha_re = re.compile(r"[A-Za-z]")
    iso_re   = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")

    def _parse_one(x):
        # Return (year_str, month_str, day_str); '' for any invalid date
        if pd.isna(x):
            return ("", "", "")
        s = str(x).strip()
        if not s:
            return ("", "", "")

        # ISO 'YYYY-MM-DD'
        if iso_re.match(s):
            dt = pd.to_datetime(s, errors="coerce")
            if pd.notna(dt) and year_min <= dt.year <= year_max:
                y, m, d = dt.year, dt.month, dt.day
                return (str(y), str(m), str(d))
            return ("", "", "")

        # Month-name strings: 'June 3,2022', 'July 7, 2022'
        if alpha_re.search(s):
            dt = pd.to_datetime(s, errors="coerce")
            if pd.notna(dt) and year_min <= dt.year <= year_max:
                y, m, d = dt.year, dt.month, dt.day
                return (str(y), str(m), str(d))
            return ("", "", "")

        # Normalize separators to '/', keep digits + '/'
        s = re.sub(r"[.\-–—]", "/", s)
        s = re.sub(r"/{2,}", "/", s).strip("/ ")
        s = re.sub(r"[^0-9/]", "", s)  # drop letters/garbage like 'eece/5e/e1'
        s = re.sub(r"/{2,}", "/", s).strip("/ ")

        parts = s.split("/")
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            return ("", "", "")

        mth, day, yr = parts

        # 2-digit year → 2000–year_max (e.g., '03'->2003, '27'->2027). else invalid.
        if len(yr) == 2:
            yy = int(yr)
            cap = year_max - 2000
            if 0 <= yy <= cap:
                yr = str(2000 + yy)
            else:
                return ("", "", "")

        try:
            mi, di, yi = int(mth), int(day), int(yr)
        except ValueError:
            return ("", "", "")

        # Swap if looks like D/M/Y (first >12 and second ≤12)
        if mi > 12 and di <= 12:
            mi, di = di, mi

        # Bounds + calendar validity
        if not (1 <= mi <= 12 and 1 <= di <= 31 and year_min <= yi <= year_max):
            return ("", "", "")
        try:
            _ = datetime(yi, mi, di)  # raises if invalid date (e.g., Feb 30)
        except ValueError:
            return ("", "", "")

        return (str(yi), str(mi), str(di))

    def _post(s):
        # optional zero-padding
        if not zero_pad:
            return s
        y, m, d = s
        y = y.zfill(4) if y else ""
        m = m.zfill(2) if m else ""
        d = d.zfill(2) if d else ""
        return (y, m, d)

    def _add_split_cols(src_col, prefix):
        triplets = df[src_col].apply(lambda v: _post(_parse_one(v)))
        ys, ms, ds = zip(*triplets)
        df[f"{prefix}_year"]  = pd.Series(ys, dtype="string")
        df[f"{prefix}_month"] = pd.Series(ms, dtype="string")
        df[f"{prefix}_day"]   = pd.Series(ds, dtype="string")

    _add_split_cols(receive_col, "receive")
    _add_split_cols(complete_col, "complete")

    return df

import pandas as pd
import re

def clean_investigator(df: pd.DataFrame, col: str = "investigator") -> pd.DataFrame:
    """
    Lowercase investigator names and coerce empty-ish values to ''.
    - Keeps the same column name.
    - Output dtype: pandas 'string' (not NaN; empty -> '').
    """
    df = df.copy()

    compact_empty = {"", "na", "none", "null", "nil", "unknown"}

    def _clean(x):
        if pd.isna(x):
            return ""
        s = str(x).strip()
        if not s:
            return ""
        # treat standalone dashes as empty
        if re.fullmatch(r"[-–—]+", s):
            return ""
        s_low = s.lower()
        # collapse repeated whitespace
        s_low = re.sub(r"\s+", " ", s_low).strip()
        # normalize and check variants like "N.A.", "n/a", "N / A"
        compact = s_low.replace(".", "").replace("/", "").replace("\\", "").replace(" ", "")
        if compact in compact_empty:
            return ""
        return s_low

    df[col] = df[col].apply(_clean).astype("string")
    return df

def split_officers_to_rows(df: pd.DataFrame, col: str = "officer") -> pd.DataFrame:
    """
    From a messy 'officer' column, produce per-officer rows with:
      - first_name (lowercase)
      - last_name  (lowercase)
    and drop the original officer column.

    Handles:
      - Ranks/titles (sgt., lt., capt., major, cpl., det., officer, mr/mrs/ms, etc.)
      - Multiple separators: '/', ',', ';', '&', ' and '
      - ISO garbage and stray punctuation
      - Runs of names without separators (pair them: First Last First Last ...)
      - Initials like 'J. Henry', 'H. Bradford'
      - Last-name-only tokens -> first_name = '', last_name = token

    If nothing parseable is found for a row, keeps one row with empty first/last.
    """

    df = df.copy()

    # Patterns & helpers
    RANK_RE = re.compile(
        r"^\s*(sgt|sergeant|lt|lieutenant|capt|captain|cpt|major|maj|cpl|corporal|officer|det|detective|mr|mrs|ms|chief)\.?\s+",
        flags=re.I,
    )
    # tokens we consider "unknown" and skip
    UNKNOWN_RE = re.compile(r"^(unk(?:nown)?|fireman|none|na|n/?a)$", flags=re.I)

    SEP_REPLACER = re.compile(r"\s+(?:and|&)\s+|[;,]|[|]|\\")
    MULTISPACE = re.compile(r"\s+")
    DASH_RUN = re.compile(r"[-–—]+")

    def _strip_ranks(s: str) -> str:
        prev = None
        # remove rank/titles repeatedly if chained (e.g., "Lt. Sgt. John Doe")
        while prev != s:
            prev = s
            s = RANK_RE.sub("", s)
        return s

    def _normalize_piece(s: str) -> str:
        s = s.strip()
        if not s:
            return ""
        s = _strip_ranks(s)
        s = s.strip(" .,/;-")
        s = MULTISPACE.sub(" ", s)
        return s

    def _split_by_separators(s: str):
        # unify separators to '/'
        s = SEP_REPLACER.sub(" / ", s)
        s = DASH_RUN.sub(" ", s)           # long dashes -> space
        s = MULTISPACE.sub(" ", s)
        parts = [p.strip() for p in s.split("/") if p.strip()]
        return parts

    def _pair_run_if_needed(s: str):
        """
        If no explicit separators and we have a run like 'First Last First Last ...',
        pair tokens into names two-by-two.
        """
        if "/" in s or "," in s or ";" in s:
            return None  # will be handled elsewhere
        tokens = s.split()
        if len(tokens) < 3:
            return None
        # if even token count, assume first/last pairs
        if len(tokens) % 2 == 0:
            pairs = []
            for i in range(0, len(tokens), 2):
                pairs.append(tokens[i] + " " + tokens[i+1])
            return pairs
        # odd token count: pair as much as possible; leave last as last-name-only
        pairs = []
        i = 0
        while i + 1 < len(tokens):
            pairs.append(tokens[i] + " " + tokens[i+1])
            i += 2
        if i < len(tokens):
            pairs.append(tokens[i])  # last lone token, likely a surname
        return pairs

    def _parse_name(piece: str):
        """
        Return (first, last) in lowercase.
        Rules:
          - 1 token -> ('', token)
          - 2+ tokens -> (first token, last token) [middle dropped]
        """
        piece = _normalize_piece(piece)
        if not piece or UNKNOWN_RE.match(piece):
            return None

        toks = piece.split()
        if len(toks) == 1:
            first, last = "", toks[0]
        else:
            first, last = toks[0], toks[-1]

        # normalize case: keep initials like 'J.' but lowercase everything
        first = first.lower()
        last = last.lower()

        # cleanup trailing punctuation on initial
        first = first.strip(".,")
        last = last.strip(".,")
        return (first, last)

    def _extract_people(raw: str):
        if pd.isna(raw):
            return []

        s = str(raw).strip()
        if not s:
            return []

        # normalize obvious separators; then try pairing if none present
        parts = _split_by_separators(s)
        if not parts:
            # maybe it's a run of names without separators
            paired = _pair_run_if_needed(s)
            parts = paired if paired else [s]

        # final clean pieces -> parse
        people = []
        for p in parts:
            p = _normalize_piece(p)
            if not p:
                continue
            # a piece might still contain multiple words without separator (e.g., "Will White Chris Beasley")
            # attempt one more pass: if > 3 words AND even, pair within the piece
            toks = p.split()
            if len(toks) >= 4 and len(toks) % 2 == 0:
                for i in range(0, len(toks), 2):
                    sub = toks[i] + " " + toks[i+1]
                    nm = _parse_name(sub)
                    if nm:
                        people.append(nm)
                continue
            nm = _parse_name(p)
            if nm:
                people.append(nm)

        # if nothing parsed, keep one empty person to preserve the row
        return people if people else [("", "")]

    # Build list column of (first,last) tuples
    name_lists = df[col].apply(_extract_people)

    # Explode to one row per officer
    out = df.explode(name_lists.name, ignore_index=True)  # trick to keep index? We'll do directly:
    # The above line won't work as-is; explode requires a column. So:
    df["_officers_list"] = name_lists
    df = df.explode("_officers_list", ignore_index=True)

    # Split tuple into columns
    df[["first_name", "last_name"]] = pd.DataFrame(
        df["_officers_list"].tolist(), index=df.index
    )

    # Ensure string dtype and lowercase (already lower, but enforce)
    df["first_name"] = df["first_name"].astype("string").fillna("")
    df["last_name"]  = df["last_name"].astype("string").fillna("")

    # Drop helper + original
    df = df.drop(columns=[col, "_officers_list"])

    return df

def split_action_from_disposition_25(
    df: pd.DataFrame,
    col: str = "disposition",
    action_col: str = "action",
    findings: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Split disposition into:
      - disposition (finding only)
      - action (everything after the finding separator)
    Keeps empty strings for missing pieces (no NaNs).
    """
    if findings is None:
        findings = ["sustained", "not sustained", "exonerated", "unfounded", "pending", "justified"]

    df = df.copy()
    s = df[col].astype("string").fillna("").str.strip().str.lower()

    # Light normalization for odd leftovers
    def _norm(x: str) -> str:
        x = re.sub(r"\s+", " ", x).strip(" ;.")
        x = x.replace("non; sustained", "not sustained").replace("non sustained", "not sustained")
        x = re.sub(r"\bnot not\s*sustained\b", "not sustained", x)
        x = x.replace("n; a", "")  # n/a
        # unify separators , | / -> ;
        x = re.sub(r"[,\|/]+", "; ", x)
        x = re.sub(r"\s*;\s*", "; ", x).strip(" ;.")
        return x

    s = s.map(_norm)

    # ^(finding)(optional sep + rest) OR (no finding -> whole string)
    findings_re = r"^(" + "|".join(map(re.escape, findings)) + r")\b(?:[:\-]|\s*;)?\s*(.*)$"
    parts = s.str.extract(findings_re)

    disp = parts[0].fillna("")  # the finding
    act  = parts[1].fillna("")  # the remainder after the finding

    # For rows with no finding matched, action should be the whole value
    mask_no_find = disp.eq("")
    act = act.where(~mask_no_find, s)

    # Drop parenthetical name lists from the action and tidy
    act = act.str.replace(r"\([^)]*\)", "", regex=True).str.strip(" ;.")

    df[col] = disp.astype("string")
    df[action_col] = act.astype("string")
    return df

import numpy as np

_ALIASES = {
    r"\buof\b": "use of force",
    r"\bbwc\b": "body worn camera",
    r"\bbody worn cameras\b": "body worn camera",
    r"\bois\b": "officer involved shooting",
    r"\bo i s\b": "officer involved shooting",
    r"\bowi\b": "operating while intoxicated",
    r"\bprof(?:\.?|essional)?\s*cond(?:uct)?\b": "professional conduct",
    r"\bcubo\b": "conduct unbecoming an officer",
}

_SEP_REGEX = re.compile(r"\s*(?:/|,|;|\band\b|\s-\s|\|{2})\s*", flags=re.I)

def _normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip(" .;:-")

def _apply_aliases(token: str) -> str:
    out = token
    for pat, repl in _ALIASES.items():
        out = re.sub(pat, repl, out)
    return re.sub(r"\s+", " ", out).strip(" .;:-")

def _split_tokens(s: str) -> list[str]:
    return [p.strip() for p in _SEP_REGEX.split(s) if p.strip()]

def combine_allegations(df: pd.DataFrame,
                        col: str = "allegation",
                        desc: str = "allegation_desc",
                        out: str = "allegation") -> pd.DataFrame:
    """Combine allegation + allegation_desc into one cleaned column."""
    def merge_row(a, d):
        a = "" if pd.isna(a) else str(a)
        d = "" if pd.isna(d) else str(d)
        both = " ; ".join(x for x in (a, d) if x)
        if not both:
            return pd.NA
        both = _normalize_text(both)

        tokens, seen = [], set()
        for t in _split_tokens(both):
            t = _apply_aliases(_normalize_text(t))
            if t and t not in seen:
                seen.add(t)
                tokens.append(t)
        return "; ".join(tokens) if tokens else pd.NA

    df = df.copy()
    df[out] = [merge_row(a, d) for a, d in zip(df[col], df[desc])]
    return df

_RANK_PREFIXES = [
    (r"^det\.?\s*", "detective "),
    (r"^lt\.?\s*", "lieutenant "),
    (r"^sgt\.?\s*", "sergeant "),
    (r"^capt\.?\s*", "captain "),
    (r"^cpt\.?\s*", "captain "),
    (r"^maj\.?\s*", "major "),
]

# allowed rank vocabulary for extraction (match your prior cleaned outputs)
_RANK_WORDS = ["detective", "lieutenant", "sergeant", "captain", "major"]

# split tokens on space, keep hyphens/apostrophes/periods within a token
_NAME_TOKEN = r"[A-Za-z][A-Za-z\-\.'`]*"

def _prep_investigator_series(s: pd.Series) -> pd.Series:
    s = s.astype("string").fillna("").str.strip().str.lower()
    # collapse multiple spaces
    s = s.str.replace(r"\s+", " ", regex=True)
    # standardize dangles like " , " or " ."
    s = s.str.strip(" ,.;:-")
    # expand rank abbreviations at the start
    for pat, repl in _RANK_PREFIXES:
        s = s.str.replace(pat, repl, regex=True)
    return s

def split_investigator(
    df: pd.DataFrame,
    col: str = "investigator",
    out_rank: str = "investigator_rank",
    out_first: str = "investigator_first_name",
    out_last: str = "investigator_last_name",
) -> pd.DataFrame:
    """
    Normalize investigator string and split into rank / first / last.
    Assumes one investigator per row (like prior scripts).
    Pattern mirrors previous behavior:
      optional <rank> + optional <first (may include middle/initials)> + <last>

    Returns a copy of df with normalized `col` and the three new columns.
    """
    df = df.copy()

    # normalize the main column (lowercase, expand rank prefixes, trim)
    norm = _prep_investigator_series(df[col] if col in df.columns else pd.Series("", index=df.index))

    # Extract:
    #   ^(?:(rank) )?(rest_of_name)$
    # then split rest_of_name into tokens; last token -> last_name; the rest -> first_name
    ranks_alt = "|".join(map(re.escape, _RANK_WORDS))
    m = norm.str.extract(fr"^(?:(?P<rank>{ranks_alt})\s+)?(?P<name>.+)$")

    # If no name captured (empty string), make it empty
    name_series = m["name"].fillna("").str.strip()

    def split_first_last(name: str) -> tuple[str, str]:
        if not name:
            return ("", "")
        # keep tokens that look like name-ish pieces; drop stray punctuation
        toks = re.findall(_NAME_TOKEN, name)
        if not toks:
            return ("", "")
        if len(toks) == 1:
            # single token → treat as last name to match older 2-token expectation
            return ("", toks[0])
        # more than one token: last token is last name; remaining tokens are first (incl. middle/initials)
        first = " ".join(toks[:-1])
        last = toks[-1]
        return (first, last)

    split = name_series.apply(lambda x: pd.Series(split_first_last(x), index=["first", "last"]))

    # assign outputs
    df[out_rank] = m["rank"].where(m["rank"].notna(), "")
    df[out_first] = split["first"]
    df[out_last]  = split["last"]

    # write back a cleaned investigator string:
    # "<rank> <first> <last>" but omit empties cleanly
    def _join_parts(r, f, l):
        parts = [p for p in [r, f, l] if p]
        return " ".join(parts) if parts else ""

    df[col] = [
        _join_parts(r, f, l)
        for r, f, l in zip(df[out_rank], df[out_first], df[out_last])
    ]

    # empty strings -> NA for downstream cleanliness
    for c in (col, out_rank, out_first, out_last):
        df.loc[df[c].astype("string") == "", c] = pd.NA

    return df

def clean_cprr_25():
    df = (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_cprr_2020_2025.csv"))
        .pipe(clean_column_names)
        .rename(columns={
            "sld_number": "tracking_id_og",
            "focus_officer": "officer",
            "date_received": "receive_date",
            "date_completed": "complete_date",
            "assigned_investigator": "investigator",
        })
        .pipe(clean_tracking_id_25, "tracking_id_og")
        .pipe(clean_complaint, "complaint")
        .pipe(clean_allegation_from_complainant, "complainant")
        .pipe(clean_disposition_inplace, "disposition")
        .pipe(strip_leading_comma)
        .pipe(clean_and_split_dates, "receive_date", "complete_date")
        .drop(columns=["receive_date", "complete_date"])
        .pipe(clean_investigator, "investigator")
        .pipe(split_officers_to_rows, "officer")
        .pipe(split_action_from_disposition_25, "disposition", "action")
        .pipe(set_values, {"agency": "lafayette-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "tracking_id_og", "disposition"], "allegation_uid")
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(combine_allegations, col="allegation", desc="allegation_desc", out="allegation")
        .drop(columns=["allegation_desc"])
        .pipe(split_investigator, col="investigator")
        .drop(columns=["investigator"])
        .pipe(clean_names, ["first_name", "last_name", "investigator_first_name", "investigator_last_name"])
        .pipe(gen_uid,["agency", "investigator_first_name", "investigator_last_name"],"investigator_uid")
)
    return df

def clean_race_addit(df: pd.DataFrame, col: str = "race") -> pd.DataFrame:
    mapping = {
        "aa": "black",
        "blk": "black",
        "wht": "white",
        "asn": "asian / pacific islander",
        "his": "hispanic",
        "lat": "hispanic",
        "two": "mixed",
    }
    out = df.copy()
    out.loc[:, col] = (
        out[col]
        .str.strip()
        .str.lower()
        .map(mapping)
        .fillna("")
    )
    return out

def clean_pprr_20():
    df = (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_pprr_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns=["department"])
        .rename(
            columns={
                "gender": "sex",
                "eeo_class": "race",
                "full_name": "name",
                "position_description": "rank_desc",
                "term_date": "left_date",
                "date_hired": "hire_date",
                "base_salary": "salary",
                "emp_status": "employment_status",
            }
        )
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_race_addit, col="race")
        .pipe(clean_salaries, ["salary"])
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "data_production_year": "2020",
                "agency": "lafayette-pd",
            },
        )
        .pipe(standardize_rank)
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
    )
    return df 

def split_names_lafayette(df: pd.DataFrame, name_col: str = "name") -> pd.DataFrame:
    """
    Split 'LAST[, SUFFIX], FIRST [MIDDLE]' into lower-case first_name, middle_name (single initial), last_name.
    Handles quirks like 'A MATHEW' (initial-first swap) and multi-word last names.
    """

    def parse(n):
        if pd.isna(n) or not str(n).strip():
            return pd.Series({"first_name": pd.NA, "middle_name": pd.NA, "last_name": pd.NA})

        s = re.sub(r"\s+", " ", str(n).strip())

        # split only on the first comma
        if "," in s:
            last, given = s.split(",", 1)
            last = last.strip().lower()
            given = given.strip()
        else:
            # fallback: if no comma, assume last token is last name
            toks = s.split(" ")
            last = toks[-1].lower()
            given = " ".join(toks[:-1])

        # tokens after the comma; remove periods for initial detection
        toks = [t for t in re.split(r"\s+", given) if t]
        toks_clean = [t.replace(".", "") for t in toks]

        def is_initial(t: str) -> bool:
            return len(t) == 1

        first = pd.NA
        middle = pd.NA

        if len(toks_clean) == 1:
            # e.g., "JOHNSON, TIMOTHY"
            first = toks_clean[0].lower()

        elif len(toks_clean) >= 2:
            t0, t1 = toks_clean[0], toks_clean[1]
            # Handle swapped "initial first" like "A MATHEW" or "G JACE"
            if is_initial(t0) and len(t1) > 1:
                first = t1.lower()
                middle = t0.lower()
            else:
                # Normal case: "JOHN W" or "JOHN WILLIAM"
                first = t0.lower()
                middle = t1[0].lower() if len(t1) >= 1 else pd.NA

        return pd.Series({"first_name": first, "middle_name": middle, "last_name": last})

    parts = df[name_col].apply(parse)
    parts = parts.astype({"first_name": "string", "middle_name": "string", "last_name": "string"})
    return df.assign(
        first_name=parts["first_name"],
        middle_name=parts["middle_name"],
        last_name=parts["last_name"],
    )

def clean_pprr_20():
    df20 = (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_pprr_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns=["department"])
        .rename(
            columns={
                "gender": "sex",
                "eeo_class": "race",
                "full_name": "name",
                "position_description": "rank_desc",
                "term_date": "left_date",
                "date_hired": "hire_date",
                "base_salary": "salary",
                "emp_status": "employment_status",
            }
        )
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_race_addit, col="race")
        .pipe(clean_salaries, ["overtime"])
        .pipe(clean_salaries, ["gross_annual"])
        .pipe(clean_salaries, ["salary"])
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "data_production_year": "2020",
                "agency": "lafayette-pd",
            },
        )
        .pipe(standardize_rank)
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(split_names_lafayette, "name")
        .drop(columns=["name"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
    )
    return df20 


def clean_pprr_21():
    df21 = (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_pprr_2021.csv"))
        .pipe(clean_column_names)
        .drop(columns=["department"])
        .rename(
            columns={
                "gender": "sex",
                "eeo_class": "race",
                "full_name": "name",
                "position_description": "rank_desc",
                "term_date": "left_date",
                "date_hired": "hire_date",
                "base_salary": "salary",
                "emp_status": "employment_status",
            }
        )
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_race_addit, col="race")
        .pipe(clean_salaries, ["overtime"])
        .pipe(clean_salaries, ["gross_annual"])
        .pipe(clean_salaries, ["salary"])
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "data_production_year": "2021",
                "agency": "lafayette-pd",
            },
        )
        .pipe(standardize_rank)
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(split_names_lafayette, "name")
        .drop(columns=["name"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
    )
    return df21 


def clean_pprr_22():
    df22 = (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_pprr_2022.csv"))
        .pipe(clean_column_names)
        .drop(columns=["department"])
        .rename(
            columns={
                "gender": "sex",
                "eeo_class": "race",
                "full_name": "name",
                "position_description": "rank_desc",
                "term_date": "termination_date",
                "date_hired": "hire_date",
                "base_salary": "salary",
                "emp_status": "employment_status",
            }
        )
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_race_addit, col="race")
        .pipe(clean_salaries, ["overtime"])
        .pipe(clean_salaries, ["gross_annual"])
        .pipe(clean_salaries, ["salary"])
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "data_production_year": "2022",
                "agency": "lafayette-pd",
            },
        )
        .pipe(standardize_rank)
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(split_names_lafayette, "name")
        .drop(columns=["name"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
    )
    return df22 


def clean_pprr_23():
    df23 = (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_pprr_2023.csv"))
        .pipe(clean_column_names)
        .drop(columns=["department"])
        .rename(
            columns={
                "gender": "sex",
                "eeo_class": "race",
                "full_name": "name",
                "position_description": "rank_desc",
                "term_date": "termination_date",
                "date_hired": "hire_date",
                "base_salary": "salary",
                "emp_status": "employment_status",
            }
        )
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_race_addit, col="race")
        .pipe(clean_salaries, ["overtime"])
        .pipe(clean_salaries, ["gross_annual"])
        .pipe(clean_salaries, ["salary"])
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "data_production_year": "2023",
                "agency": "lafayette-pd",
            },
        )
        .pipe(standardize_rank)
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(split_names_lafayette, "name")
        .drop(columns=["name"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
    )
    return df23 

def clean_pprr_24():
    df24 = (
        pd.read_csv(deba.data("raw/lafayette_pd/lafayette_pd_pprr_2024.csv"))
        .pipe(clean_column_names)
        .drop(columns=["department"])
        .rename(
            columns={
                "gender": "sex",
                "eeo_class": "race",
                "full_name": "name",
                "position_description": "rank_desc",
                "term_date": "left_date",
                "date_hired": "hire_date",
                "base_salary": "salary",
                "emp_status": "employment_status",
            }
        )
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_race_addit, col="race")
        .pipe(clean_salaries, ["overtime"])
        .pipe(clean_salaries, ["gross_annual"])
        .pipe(clean_salaries, ["salary"])
        .pipe(
            set_values,
            {
                "salary_freq": salary.YEARLY,
                "data_production_year": "2024",
                "agency": "lafayette-pd",
            },
        )
        .pipe(standardize_rank)
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(split_names_lafayette, "name")
        .drop(columns=["name"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
    )
    return df24 

def combine_pprrs(df20, df21, df22, df23, df24):
    # Concatenate all dfs together
    df = pd.concat([df20, df21, df22, df23, df24], ignore_index=True)

    # Ensure data_production_year is numeric for sorting
    df["data_production_year"] = df["data_production_year"].astype(int)

    # Sort by uid and data_production_year (later year comes last)
    df = df.sort_values(["uid", "data_production_year"])

    # Drop duplicates, keeping the later year
    df = df.drop_duplicates(subset=["uid"], keep="last")

    # Reset index
    return df.reset_index(drop=True)

def concat_union_dedup_uid(*dfs: pd.DataFrame, fill: str = "") -> pd.DataFrame:
    """
    Concatenate heterogeneous PPRR dataframes while preserving the union of columns.
    - Missing columns are created and filled with `fill` (default: empty string).
    - Deduplicate by `uid`, keeping the row with the latest data_production_year.
    """
    if not dfs:
        return pd.DataFrame()

    # union of all columns across inputs
    all_cols = sorted(set().union(*(df.columns for df in dfs)))

    # ensure every df has all columns (fill missing with empty string), then concat
    aligned = []
    for df in dfs:
        df = df.copy()
        for c in all_cols:
            if c not in df.columns:
                df[c] = fill
        aligned.append(df.reindex(columns=all_cols))

    out = pd.concat(aligned, ignore_index=True)

    # make sure data_production_year is numeric for proper ordering
    out["data_production_year"] = pd.to_numeric(out["data_production_year"], errors="coerce")

    # sort so the latest year per uid is last, then drop dupes keeping the last
    out = (
        out.sort_values(["uid", "data_production_year"], kind="mergesort")
           .drop_duplicates(subset=["uid"], keep="last")
           .reset_index(drop=True)
    )
    return out

if __name__ == "__main__":
    pprr = clean_pprr()
    pprr20 = clean_pprr_20()
    pprr21 = clean_pprr_21()
    pprr22 = clean_pprr_22()
    pprr23 = clean_pprr_23()
    pprr24 = clean_pprr_24()
    pprr20_24 = combine_pprrs(pprr20, pprr21, pprr22, pprr23, pprr24)
    pprr2010_2024 = concat_union_dedup_uid(pprr, pprr20_24)
    cprr_20 = clean_cprr_20()
    cprr_14 = clean_cprr_14()
    cprr_25 = clean_cprr_25()
    cprr_25.to_csv(deba.data("clean/cprr_lafayette_pd_2020_2025.csv"), index=False)
    cprr_20.to_csv(deba.data("clean/cprr_lafayette_pd_2015_2020.csv"), index=False)
    cprr_14.to_csv(deba.data("clean/cprr_lafayette_pd_2009_2014.csv"), index=False)
    pprr.to_csv(deba.data("clean/pprr_lafayette_pd_2010_2021.csv"), index=False)
    #pprr20.to_csv(deba.data("clean/pprr_lafayette_pd_2020.csv"), index=False)
    pprr20_24.to_csv(deba.data("clean/pprr_lafayette_pd_2020_2024.csv"), index=False)
    pprr2010_2024.to_csv(deba.data("clean/pprr_lafayette_pd_2010_2024.csv"), index=False)

