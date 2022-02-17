import pandas as pd

from lib.columns import clean_column_names, set_values
import dirk
from lib.clean import (
    clean_names,
    clean_salaries,
    clean_sexes,
    clean_races,
    float_to_int_str,
    clean_dates,
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
    df.loc[:, "middle_initial"] = names[2]
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
    )
    return df


def clean_pprr():
    return (
        pd.read_csv(dirk.data("raw/lafayette_pd/lafayette_pd_pprr_2010_2021.csv"))
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
                "agency": "Lafayette PD",
            },
        )
        .pipe(float_to_int_str, ["birth_year"])
        .pipe(standardize_rank)
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name", "middle_initial"])
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_initial"])
    )


def clean_tracking_number(df):
    df.loc[:, "tracking_number"] = (
        df.tracking_number.str.strip()
        .str.replace(" ", "", regex=False)
        .str.replace(r"(\w+)(\d+)", r"\1 \2", regex=True)
    )
    return df[df.tracking_number != "\x1a"].reset_index(drop=True)


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
        .sort_values("tracking_number")
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
        "pco",
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
        (df.tracking_number == "AD2015-005") & (df.last_name == "benoit"),
        "disposition",
    ] = "sustained-3 days"
    df.loc[
        (df.tracking_number == "AD2015-005") & (df.last_name != "benoit"),
        "disposition",
    ] = "sustained-letter of reprimand"

    # splitting for 'AD2015-015': 'sustained- bajat- loc; allred- counseling form'
    df.loc[
        (df.tracking_number == "AD2015-015") & (df.last_name == "bajat"),
        "disposition",
    ] = "sustained-loc"
    df.loc[
        (df.tracking_number == "AD2015-015") & (df.last_name != "allred"),
        "disposition",
    ] = "sustained-counseling form"

    # 'AD2019-004': 'excessive force - not sustained att. to duty - sustained/deficiency'
    df.loc[
        (df.tracking_number == "AD2019-004") & (df.allegation == "excessive force"),
        "disposition",
    ] = "not sustained"
    df.loc[
        (df.tracking_number == "AD2019-004") & (df.allegation != "attention to duty"),
        "disposition",
    ] = "sustained/deficiency"

    # 'AD2019-005': 'terminated - trent (overturned - 10 days) kyle 3 days susp.'
    df.loc[
        (df.tracking_number == "AD2019-005") & (df.first_name == "trent"),
        "disposition",
    ] = "10 days"
    df.loc[
        (df.tracking_number == "AD2019-005") & (df.first_name != "kyle"),
        "disposition",
    ] = "3 days susp"

    # 'CC1801': 'thibodeaux - counseling form; brasseaux - counseling form'
    df.loc[
        (df.tracking_number == "CC1801")
        & ((df.last_name == "thibodeaux") | (df.last_name == "brasseaux")),
        "disposition",
    ] = "counseling form"
    df.loc[
        (df.tracking_number == "CC1801")
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
        .sort_values("tracking_number")
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


def clean_cprr_20():
    return (
        pd.read_csv(dirk.data("raw/lafayette_pd/lafayette_pd_cprr_2015_2020.csv"))
        .pipe(clean_column_names)
        .dropna(how="all")
        .rename(
            columns={
                "cc_number": "tracking_number",
                "complaint": "allegation",
                "date_received": "receive_date",
                "date_completed": "complete_date",
                "assigned_investigator": "investigator",
                "focus_officer_s": "officer",
            }
        )
        .pipe(clean_tracking_number)
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
        .pipe(set_values, {"data_production_year": 2020, "agency": "Lafayette PD"})
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
            ["agency", "tracking_number", "allegation", "uid"],
            "allegation_uid",
        )
    )


def clean_tracking_number_14(df):
    df.loc[:, "tracking_number"] = (
        df.cc_number.str.lower()
        .str.strip()
        .str.replace(r"^-", "", regex=True)
        .str.replace(r"^(ad)(\d+)-", r"\1 \2-", regex=True)
        .str.replace(r"sl(\d{2})", r"sl \1", regex=True)
    )
    return df.drop(columns="cc_number")


def clean_complainant(df):
    df.loc[:, "complainant"] = (
        df.complainant.str.lower()
        .str.strip()
        .str.replace(r"(\/\/|ry|lt\.)", "", regex=True)
    )
    return df


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


def clean_charges_14(df):
    df.loc[:, "charges"] = (
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


def split_rows_with_multiple_charges_14(df):
    i = 0
    for idx in df[df.charges.str.contains(r"/")].index:
        s = df.loc[idx + i, "charges"]
        parts = re.split(r"\s*(?:/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "charges"] = name
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


def drop_rows_missing_first_and_last_name_14(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def drop_rows_missing_charges_disposition_and_action_14(df):
    return df[~((df.charges == "") & (df.action == "") & (df.disposition == ""))]


def assign_correct_actions_14(df):
    df.loc[
        ((df.first_name == "brent") & (df.tracking_number == "2012-008")), "action"
    ] = "3-day suspension"
    df.loc[
        ((df.first_name == "devin") & (df.tracking_number == "2012-008")), "action"
    ] = "1-day suspension"
    df.loc[
        ((df.last_name == "firmin") & (df.tracking_number == "2009-002")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "starling") & (df.tracking_number == "sl 13-006")), "action"
    ] = "letter of counseling"
    df.loc[
        ((df.last_name == "hebert") & (df.tracking_number == "2011-012")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_number == "2011-006")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "hackworth") & (df.tracking_number == "2012-022")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "taylor") & (df.tracking_number == "ad 13-001")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "taylor") & (df.tracking_number == "ad 13-002")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "baumgardner") & (df.tracking_number == "2012-015")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "baumgardner") & (df.tracking_number == "2012-013")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "dangerfield") & (df.tracking_number == "2012-001")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "thompson") & (df.tracking_number == "2012-010")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "poiencot") & (df.tracking_number == "2012-010")), "action"
    ] = "terminated"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_number == "2011-004")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_number == "2011-001")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "bricker") & (df.tracking_number == "2011-007")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "carter") & (df.tracking_number == "2010-011")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "dartez") & (df.tracking_number == "2010-012")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "howard") & (df.tracking_number == "2009-008")), "action"
    ] = "resigned"
    df.loc[
        ((df.last_name == "winjum") & (df.tracking_number == "2010-014")), "action"
    ] = "resigned"
    df.loc[
        (
            (df.last_name == "bertrand")
            & (df.tracking_number == "2012-003")
            & (df.charges == "insubordination")
        ),
        "action",
    ] = ""
    return df


def assign_correct_disposition_14(df):
    df.loc[
        ((df.last_name == "crozier") & (df.tracking_number == "sl 13-006")),
        "disposition",
    ] = "unfounded"
    df.loc[
        ((df.last_name == "starling") & (df.tracking_number == "sl 13-006")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "sonnier") & (df.tracking_number == "sl 13-006")),
        "disposition",
    ] = ""
    df.loc[
        ((df.last_name == "firmin") & (df.tracking_number == "2009-002")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "hebert") & (df.tracking_number == "2011-012")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_number == "2011-006")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "hackworth") & (df.tracking_number == "2012-022")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "taylor") & (df.tracking_number == "ad 13-001")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "taylor") & (df.tracking_number == "ad 13-002")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "baumgardner") & (df.tracking_number == "2012-015")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "baumgardner") & (df.tracking_number == "2012-013")),
        "disposition",
    ] = "not sustained"
    df.loc[
        ((df.last_name == "dangerfield") & (df.tracking_number == "2012-001")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "thompson") & (df.tracking_number == "2012-010")),
        "disposition",
    ] = ""
    df.loc[
        ((df.last_name == "poiencot") & (df.tracking_number == "2012-010")),
        "disposition",
    ] = ""
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_number == "2011-004")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "roberts") & (df.tracking_number == "2011-001")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "bricker") & (df.tracking_number == "2011-007")),
        "disposition",
    ] = "sustained"
    df.loc[
        ((df.last_name == "carter") & (df.tracking_number == "2010-011")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "dartez") & (df.tracking_number == "2010-012")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "howard") & (df.tracking_number == "2009-008")), "disposition"
    ] = "sustained"
    df.loc[
        ((df.last_name == "winjum") & (df.tracking_number == "2010-014")), "disposition"
    ] = "sustained"
    df.loc[
        (
            (df.last_name == "bertrand")
            & (df.tracking_number == "2012-003")
            & (df.charges == "insubordination")
        ),
        "disposition",
    ] = "unfounded"
    df.loc[
        (
            (df.last_name == "bertrand")
            & (df.tracking_number == "2012-003")
            & (df.charges == "rude and unprofessional")
        ),
        "disposition",
    ] = "sustained"
    return df


def clean_cprr_14():
    df = (
        pd.read_csv(dirk.data("raw/lafayette_pd/lafayette_pd_cprr_2009_2014.csv"))
        .pipe(clean_column_names)
        .pipe(clean_receive_date_14)
        .pipe(clean_complete_date_14)
        .pipe(clean_dates, ["receive_date", "complete_date"])
        .pipe(clean_tracking_number_14)
        .pipe(clean_complainant)
        .pipe(clean_and_split_investigator_14)
        .pipe(extract_action_from_disposition_14)
        .pipe(clean_disposition_14)
        .pipe(clean_charges_14)
        .pipe(split_rows_with_multiple_charges_14)
        .pipe(split_rows_with_multiple_names_14)
        .pipe(split_names_14)
        .pipe(drop_rows_missing_first_and_last_name_14)
        .pipe(assign_correct_actions_14)
        .pipe(assign_correct_disposition_14)
        .pipe(drop_rows_missing_charges_disposition_and_action_14)
        .pipe(set_values, {"agency": "Lafayette PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["agency", "investigator_first_name", "investigator_last_name"],
            "investigator_uid",
        )
        .pipe(
            gen_uid,
            ["uid", "charges", "action", "tracking_number", "disposition"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    pprr = clean_pprr()
    cprr_20 = clean_cprr_20()
    cprr_14 = clean_cprr_14()
    cprr_20.to_csv(dirk.data("clean/cprr_lafayette_pd_2015_2020.csv"), index=False)
    cprr_14.to_csv(dirk.data("clean/cprr_lafayette_pd_2009_2014.csv"), index=False)
    pprr.to_csv(dirk.data("clean/pprr_lafayette_pd_2010_2021.csv"), index=False)
