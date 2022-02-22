import pandas as pd
from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_names, float_to_int_str, standardize_desc_cols
from lib.uid import gen_uid
from lib.standardize import standardize_from_lookup_table


action_lookup = [
    ["arrested/terminated on 04/20/2020 on case d-003-2020"],
    ["case withdrawn by colonel colvin"],
    ["currently awaiting fit criminal report"],
    ["currently awaiting fit report"],
    ["currently out awaiting covid testing"],
    [
        "currently out due to covid downgraded to a write-up",
        "currently out due to covid/downgraded to a dm-1",
    ],
    [
        "currently out due to covid/fmla terminated case k-052-20 on 12/02/2020 by human resources",
        "currently out due to covid/fmla terminated on case k-052-20 on 12/02/2020 by hr",
    ],
    ["currently out on fmla during no call no show"],
    ["currently under investigation", "currently under investigaiton"],
    ["currently under investigation with tdc"],
    [
        "deputy rixner apologized to lieutenant armwood prior to write-up",
        "dep rixner apologized to lt armwood prior to writeup",
    ],
    ["downgraded to a write-up", "downgraded to dm-1"],
    ["downgraded to a write-up by chief bruno", "downgraded to dm-1 per chief bruno"],
    [
        "downgraded to verbal counsel by chief bruno",
        "downgraded to verbal councel per chief bruno",
    ],
    ["dropped from payroll on 03/26/2021"],
    ["non-sustained"],
    [
        "orleans justice center conducting investigation haven't received case yet",
        "ojc conducting investiagtion haven t received case yet",
    ],
    [
        "orleans justice center conducting investigation haven't "
        "received case yet/resigned under investigation on 03/17/2021",
        "ojc conducting investiagtion haven t received case yet "
        "resigned under investigation on 03/17/2021",
    ],
    [
        "currently out due to covid-19/downgraded to a write-up",
        "currently out due to covid downgraded to a dm-1",
        "out due to covid-19 downgraded to dm-1",
    ],
    [
        "currently out due to covid-19/resigned under investigation on 07/22/2020",
        "out due to covid-19/resigned under investigation 07/22/2020",
    ],
    [
        "currently out due to failure to take covid vaccination",
        "out due to failure to take covid vaccination",
    ],
    ["resigned under investigation"],
    ["resigned on 11/11/2019"],
    [
        "resigned under investigation on 01/25/2020",
        "resigned under investigation 01/25/2020",
    ],
    [
        "resigned under investigation 04/24/2020 prior to going to disciplinary review board"
    ],
    [
        "resigned under investigation on 11/27/2020",
        "resigned under investigation 11/27/2020",
    ],
    ["resigned under investigation on 03/25/2020"],
    ["resigned under investigation on 03/27/2020"],
    ["resigned under investigation on 11/11/19"],
    ["resigned under investigation on 5/27/2020"],
    ["suspended on 01/06/2020/terminated by disciplinary review board on 01/23/2020"],
    [
        "suspended on 01/28/2020/terminated by human resources",
        "suspended on 01/28/2020/terminated by hr",
    ],
    [
        "suspended on 02/05/2020/returned to duty on 03/05/2020",
        "suspended on 02/05/2020/return to duty on 03/05/2020",
    ],
    ["suspended on 02/05/2020/terminated by disciplinary review board on 03/03/2020"],
    [
        "suspended on 03/17/2020/terminated by human resources on 03/18/2020",
        "suspended on 03/17/2020/terminated by hr on 03/18/2020",
    ],
    ["suspended on 03/18/2020/terminated on 04/21/2020"],
    [
        "suspended on 03/24/2020/terminated by disciplinary review board on 04/22/2020",
        "suspended on 03/24/2020/terminaed by disciplinary review board on 04/22/2020",
    ],
    [
        "suspended on 04/02/2020/terminated by disciplinary review board on 04/21/2020",
        "suspended on 04/02/2020/terminated by disciplinary review board 04/21/2020",
    ],
    ["suspended on 04/20/2020/terminated by disciplinary review board on 06/03/2020"],
    ["suspended on 04/24/2020"],
    [
        "suspended on 04/24/2020/resigned under investigation on 04/24/2020",
        "suspended on 02/04/2020/resigned under investigation 04/24/2020",
    ],
    [
        "suspended on 05/08/2020/awaiting criminal investigation",
        "suspended on 05/08/2020/awaiting criminal s investigation",
    ],
    [
        "suspended on 06/02/2020/terminated by human resources on 06/17/2020",
        "suspended on 06/02/2020/terminated by hr on 06/17/2020",
    ],
    [
        "suspended on 07/20/2020/case currently pending in criminal court",
        "suspended on 07/20/2020 case currently pending in criminal court",
    ],
    ["suspended on 07/30/2020", "suspended on 07/30/2020/returned to duty on"],
    ["suspended on 08/04/2020/terminated by disciplinary review board on 09/02/2020"],
    ["suspended on 08/19/2020", "suspended on 08/19/2020/returned to duty on"],
    [
        "suspended on 09/14/2020 terminated on 10/01/2020/termination was rescinded on 10/1/2020",
        "suspended on 09/14/2020 terminated on 10/01/2020 termination was rescinded on 10/1/2020",
    ],
    [
        "suspended on 09/16/2020/terminated on 10/01/2020/termination was rescinded on 10/1/2020",
        "suspended on 09/16/2020 terminated on 10/01/2020 termination was rescinded on 10/1/2020",
    ],
    [
        "suspended 09/30/2020-10/25/2020/returned to duty 10/28/2020",
        "suspended on 09/30/2020-10/25/2020 return to duty 10/28/2020",
    ],
    [
        "suspended 10/01/2020-10/25/2020/returned to duty on 10/28/2020",
        "suspended on 10/01/2020-10/25/2020 return to dudty on 10/28/2020",
    ],
    [
        "suspended 10/21/20-10/30/20/returned to duty on 10/31/20",
        "suspended on 10/21/20-10/30/20 return to duty on 10/31/20",
    ],
    ["suspended on 10/21/2020/terminated by disciplinary review board on 11/02/2020"],
    [
        "suspended on 10/21/2020/terminated by human resources on 10/26/2020",
        "suspended on 10/21/2020/terminated by hr on 10/26/2020",
    ],
    [
        "suspended on 10/21/2020/terminated by human resources on 10/26/2020 case j-052-20",
        "suspended on 10/21/2020/terminated by hr on 10/26/2020 on case j-052-20",
    ],
    [
        "suspended 10/25/20-11/5/20/returned to duty on 11/8/20",
        "suspended on 10/25/20-11/5/20 return to duty on 11/8/20",
    ],
    [
        "suspended on 10/27/20-11/9/20/returned to duty on 11/10/20",
        "suspended on 10/27/20-11/9/20 return to duty on 11/10/20",
    ],
    ["suspended on 11/16/2020 due to arrest/pending court case"],
    ["suspended on 11/23/2020/returned to duty on 12/01/2020"],
    [
        "suspended on 11/25/2020/terminated by human resources on 12/02/2020",
        "suspended on 11/25/2020/terminated by hr on 12/02/2020",
    ],
    ["suspended on 12/04/2020"],
    ["suspended on 12/04/2020 on case l-011-20"],
    [
        "suspended on 12/04/2020/terminated on 01/14/2021",
        "suspended on 12/04/2020 terminated on 01/14/2021",
    ],
    ["suspended on 12/15/2020"],
    ["suspended on 12/21/2020"],
    ["suspended on 12/21/2020 on case l-039-2020"],
    ["suspended on 12/22/2020"],
    ["suspended on 12/31/2020/resigned under investigation on 01/04/2020"],
    ["suspended on 12/8/2020"],
    [
        "suspended 1-6-21-1/8/21/returned to duty on 1/11/21",
        "suspended on 1-6-21 end date 1/8/21 return to duty on 1/11/21",
    ],
    [
        "suspended on 2/10/21/returned on 2/11/2021",
        "suspended on 2/10/21 return on 2/11/2021",
    ],
    [
        "suspended 2/10/21-2/11/2021/returned to duty on 2/15/2021",
        "suspended on 2/10/21-2/11/2021 return to duty on 2/15/2021",
    ],
    [
        "suspended 2/15/2021-02/16/2021/returned to duty on 2/19/2021",
        "suspended on 2/15/2021-02/16/2021 return to duty on 2/19/2021",
    ],
    ["suspended on 6/15/2020/terminated by disciplinary review board on 07/29/2020"],
    ["suspended on 7/23/2020/resigned under investigation on 07/31/2020"],
    [
        "suspended on 8/24/2020/terminated on 10/1/2020",
        "suspended on 8/24/2020 terminated on 10/1/2020",
    ],
    [
        "suspended on 8/25/2020/terminated on 9/17/2020",
        "suspended on 8/25/2020 terminated on 9/17/2020",
    ],
    [
        "suspended 9/14/2020-9/17/2020/returned to duty on 9/18/2020",
        "suspended on 9/14/2020-9/17/2020 return to duty on 9/18/2020",
    ],
    [
        "suspended on 9/30/20-10/5/20/returned to duty on 10/6/2020",
        "suspended on 9/30/20-10/5/20 return to duty on 10/6/2020",
    ],
    [
        "suspended 9/4/2020-9/6/2020/returned to duty on 9/9/2020",
        "suspended on 9/4/2020-9/6/2020 return to duty on 9/9/2020",
    ],
    [
        "suspended 9/9/2020-9/10/2020/returned to duty on 9/11/2020",
        "suspended on 9/9/2020-9/10/2020 return to duty on 9/11/2020",
    ],
    [
        "suspended/arrested on 04/13/2020/terminated on 04/20/2020",
        "suspended/arrested on 04/13/2020 terminaed on 04/20/2020",
    ],
    ["suspended/arrested on 07/01/2020/resigned under investigation on 07/13/2020"],
    ["suspension overturned by sheriff marlin gusman on 07/10/2020"],
    [
        "suspended 3/18/21/returned to duty on 3/21/2021",
        "susupended 3/18/21 returned on 3/21/2021",
    ],
    ["terminated on 05/20/2019", "terminated 05/20/2019"],
    ["terminated by human resources on 03/17/2020", "terminated by hr 03/17/2020"],
    ["terminated by human resources on 06/11/2020", "terminated by hr 06/11/2020"],
    ["terminated by human resources on 05/11/2020", "terminated by hr on 05/11/2020"],
    ["terminated by human resources on 06/10/2020", "terminated by hr on 06/10/2020"],
    ["terminated by human resources on 07/31/2020", "terminated by hr on 07/31/2020"],
    ["terminated on case h-060-19 prior to receving case l-032-2020"],
    ["unfounded all charges"],
    [
        "suspended on 09/16/2020/terminated on 10/01/2020/termination was rescinded on 10/1/2020"
    ],
]


def remove_header_rows(df):
    return df[df.date_received.str.strip() != "Date\rReceived"].reset_index(drop=True)


def remove_carriage_return(df, cols):
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.replace(r"-\r", r"-", regex=True)
            .str.replace(r"\r", r" ", regex=True)
        )
    return df


def split_name_19(df):
    series = df.name_of_accused.fillna("").str.strip()
    for col, pat in [
        ("first_name", r"^([\w'-]+)(.*)$"),
        ("middle_name", r"^(\w\.) (.*)$"),
    ]:
        names = series[series.str.match(pat)].str.extract(pat)
        df.loc[series.str.match(pat), col] = names[0].str.replace(
            "various", "", regex=False
        )
        series = series.str.replace(pat, r"\2", regex=True).str.strip()
    df.loc[:, "last_name"] = series
    return df.drop(columns=["name_of_accused"]).dropna(
        subset=["first_name", "last_name"]
    )


def clean_action_19(df):
    df.loc[:, "action"] = (
        df.action.str.lower()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace("rui", "resigned under investigation", regex=False)
        .str.replace("rtd", "Return to duty", regex=False)
        .str.replace("drb", "Disciplinary review board", regex=False)
        .str.replace("iad", "Internal affairs department", regex=False)
        .str.replace(r"\bhr\b", "human resources", regex=True)
        .str.replace("unfouned all charges", "unfouded", regex=False)
        .str.replace(r"^n$", "", regex=True)
        .str.replace(" and ", "/", regex=False)
        .str.replace(
            "inmate was on special diet/deputy was given "
            "permission to give inmate his special diet",
            "",
            regex=False,
        )
        .str.replace(
            "accused of spreading a rumor of a sexual "
            "relationship between shaka milton",
            "",
            regex=False,
        )
        .str.replace("board non sustained charges", "", regex=False)
        .str.replace("no charges were filed by the nopd", "", regex=False)
        .str.replace("h.r", "human resources", regex=False)
        .str.replace(r"^under investigation$", "", regex=True)
    )
    return df


def split_rows_with_multiple_alllegations_19(df):
    df.loc[:, "allegation"] = (
        df.charges.str.lower()
        .str.strip()
        .str.replace(r"  +", " ", regex=True)
        .str.replace(r"(\w+)\, ? ?", r"\1/", regex=True)
        .str.replace(r" ?and ? ?", "/", regex=True)
        .str.replace(r"\/ ? ?\/", "/", regex=True)
        .str.replace(r"\"(\w+)\" \/ \"(\w+)\"", r"\1-\2", regex=True)
        .str.replace(r"\.", "", regex=True)
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
    return df.drop(columns="charges")


def clean_allegations_19(df):
    df.loc[:, "allegation"] = (
        df.allegation.fillna("")
        .str.replace(r"\"(\w+)\"", "", regex=True)
        .str.replace("responsibilities", "responsibility", regex=False)
        .str.replace(r"porfessionalism", "professionalism", regex=True)
        .str.replace(r"duty (\w+)", r"duty-\1", regex=True)
        .str.replace(r"\(|\)", "", regex=True)
        .str.replace("fo", "of", regex=False)
        .str.replace("proceudres", "procedures", regex=False)
        .str.replace("beofre", "before", regex=False)
        .str.replace("perofrm", "perform", regex=False)
        .str.replace(r"\- (\w+)", r"-\1", regex=True)
        .str.replace("neglct", "neglect", regex=False)
        .str.replace("supervisory", "supervisor", regex=False)
    )
    return df


def process_disposition(df):
    df.loc[:, "disposition"] = df.disposition.str.strip()
    df.loc[:, "allegation"] = df.allegation.str.strip().str.replace(
        r",$", r"", regex=True
    )
    for _, row in df.iterrows():
        if pd.isnull(row.disposition) or pd.isnull(row.allegation):
            continue
        allegation = row.allegation.lower()
        if row.disposition.lower().startswith(allegation):
            row.disposition = row.disposition[len(allegation) :]
    df.loc[:, "disposition"] = df.disposition.str.strip().str.replace(
        r"^[- ]+", "", regex=True
    )
    return df


def clean_department_desc(df):
    df.loc[:, "department_desc"] = (
        df.department_desc.str.lower()
        .str.strip()
        .str.replace("mechani c", "mechanic", regex=False)
        .str.replace("plannin g", "planning", regex=False)
        .str.replace("complai nce", "complaince", regex=False)
        .str.replace(r"mainten( ance)?$", "maintenance", regex=True)
        .str.replace("grievnce", "grievance", regex=False)
        .str.replace(r"h\.?r\.?", "human resources", regex=True)
        .str.replace("tdc", "temporary detention center", regex=False)
        .str.replace("ojc", "orleans justice center", regex=False)
        .str.replace(r"\bsani\b", "sanitation", regex=True)
        .str.replace("isb", "investigative services", regex=False)
        .str.replace("facility  mgmt", "facility management", regex=False)
        .str.replace("ipc", "intake and processing center", regex=False)
        .str.replace(" department", "", regex=False)
        .str.replace("iad-crim", "internal affiars", regex=False)
    )
    return df


def set_empty_uid_for_anonymous_officers(df):
    df.loc[df.employee_id.isna() & (df.first_name == ""), "uid"] = ""
    return df


def fix_date_typos(df, cols):
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.strip()
            .str.replace("//", "/", regex=False)
            .str.replace(r"^(\d{2})(\d{2})", r"\1/\2", regex=True)
            .str.replace(r"2011(\d)$", r"201\1", regex=True)
        )
    return df


def split_investigating_supervisor(df):
    df.loc[:, "investigating_supervisor"] = (
        df.investigating_supervisor.fillna("")
        .str.strip()
        .str.lower()
        .str.replace(
            r"serrgeant|ssergeant|sergenat|sergant|sergent|sergeat",
            "sergeant",
            regex=True,
        )
        .str.replace(
            r"lt\.?|lieutenatn|lieutenat|lieuttenant|lieuteant",
            "lieutenant",
            regex=True,
        )
        .str.replace("ms", "", regex=False)
        .str.replace("augusuts", "augustus", regex=False)
        .str.replace("karengant", "karen gant", regex=False)
        .str.replace(r"^(\w+)  (\w+)", r"\1 \2", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^sergeant$", "", regex=True)
        .str.replace(r"^colvin", "colonel", regex=True)
        .str.replace(r"dwi (shana)? ", "dwishana ", regex=True)
        .str.replace("unassigned", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace("captaiin", "captain", regex=False)
    )
    ranks = set(
        [
            "major",
            "agent",
            "sergeant",
            "captain",
            "lieutenant",
            "colonel",
            "chief",
            "director",
            "admin",
        ]
    )
    for idx, s in df.investigating_supervisor.items():
        if " " not in s:
            continue
        first_word = s[: s.index(" ")]
        if first_word in ranks:
            df.loc[idx, "supervisor_rank"] = first_word
            df.loc[idx, "supervisor_name"] = s[s.index(" ") + 1 :]
        else:
            df.loc[idx, "supervisor_name"] = s
    df.loc[:, "supervisor_name"] = df.supervisor_name.str.replace(
        " -", "-", regex=False
    )
    names = df.supervisor_name.fillna("").str.extract(r"^([^ ]+) (.+)")
    df.loc[:, "supervisor_first_name"] = names[0]
    df.loc[:, "supervisor_last_name"] = names[1]
    return df.drop(columns=["investigating_supervisor", "supervisor_name"])


def clean_receive_date_20(df):
    df.loc[:, "receive_date"] = (
        df.date_received.fillna("")
        .str.replace(r"(\d+)/(\d+)//(\d+)", r"\1/\2/\3", regex=True)
        .str.replace("!2", "12", regex=False)
        .str.replace("9/142020", "9/14/2020", regex=False)
    )
    return df.drop(columns="date_received")


def clean_investigation_start_date_20(df):
    df.loc[:, "investigation_start_date"] = df.date_started.fillna("").str.replace(
        r"(\d+)//(\d+)$", r"\1/\2", regex=True
    )
    return df.drop(columns="date_started")


def clean_investigation_complete_date_20(df):
    df.loc[:, "investigation_complete_date"] = (
        df.date_completed.fillna("")
        .str.replace(r"^(\d+)//(\d+)", r"\1/\2", regex=True)
        .str.replace("031/2/2020", "3/12/2020", regex=False)
        .str.replace("3/42021", "3/4/2021", regex=False)
    )
    return df.drop(columns="date_completed")


def clean_action_20(df):
    df.loc[:, "action"] = (
        df.action.str.lower()
        .str.strip()
        .str.replace(r" ^(\w+)", r"\1", regex=True)
        .str.replace(".", "", regex=False)
        .str.replace(r"on  (\d+)", r"on \1", regex=True)
        .str.replace(" and ", "/", regex=False)
        .str.replace(r"board|drb|dbr", "disciplinary review board", regex=True)
        .str.replace(r"(\w+)  (\w+)", r"\1 \2", regex=True)
        .str.replace("rui", "resigned under investigation", regex=False)
        .str.replace(r"(\d+) (\d+) (\d+)", r"\1/\2/\3", regex=True)
    )
    return df


def standardize_action_20(df):
    df.loc[:, "action"] = df.action
    return standardize_from_lookup_table(df, "action", action_lookup)


def clean_rank_desc_20(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"recu?c?r?u?it", "recruit", regex=True)
        .str.replace(r"deo?u?p?u?i?ti?e?s?y?", "deputy", regex=True)
        .str.replace("lieuteant", "lieutenant", regex=False)
        .str.replace("various", "", regex=False)
        .str.replace("cmt", "certified medical technician", regex=False)
        .str.replace("policies", "policy", regex=False)
    )
    return df


def clean_allegations_20(df):
    df.loc[:, "allegation"] = (
        df.charges.str.strip()
        .str.replace("and", "", regex=False)
        .str.replace(r", ?", "/", regex=True)
        .str.replace("dtuy", "duty", regex=False)
        .str.replace("#", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace('"', "", regex=False)
        .str.replace(r"(\d+)", "", regex=True)
        .str.replace("&", "", regex=False)
        .str.replace(r"\blaw and professionalism\b", "law/professionalism", regex=True)
        .str.replace(r" ?/$", "", regex=True)
        .str.replace(
            r"\bresp(onsibi?u?l?i?t?i?e?s?y?)?\b", "responsibilities", regex=True
        )
        .str.replace(r"\bcomp\b", "compensation", regex=True)
        .str.replace("proferssionalism", "professionalism", regex=False)
        .str.replace(r"^ ? ?(\w+)", r"\1", regex=True)
        .str.replace(",", "", regex=False)
        .str.replace("beofore", "before", regex=False)
        .str.replace("tyour", "tour", regex=False)
        .str.replace(r"poli?ciy", "policy", regex=True)
        .str.replace(r"cease?s?ing", "ceasing", regex=True)
        .str.replace(r"nege?le?c?e?t", "neglect", regex=True)
        .str.replace("procudures", "procedures", regex=False)
        .str.replace("higer", "higher", regex=False)
        .str.replace("; ", "/", regex=False)
        .str.replace("foece", "force", regex=False)
        .str.replace("hiigher", "higher", regex=False)
        .str.replace("froma", "from a", regex=False)
        .str.replace("professionalism- ", "professionalism/", regex=False)
        .str.replace("foe", "for", regex=False)
        .str.replace(r"ree?por?ti[tim]?n?g", "reporting", regex=True)
        .str.replace(r"(\w+)/ ?//?(\w+)", r"\1/\2", regex=True)
        .str.replace("a a", "a", regex=False)
        .str.replace(r"duty-? ?supervisos?ry", "duty/supervisory", regex=True)
        .str.replace(r"duty -?failure", "duty-failure", regex=True)
        .str.replace("interferring", "interfering", regex=False)
        .str.replace("dutyceasing", "duty/ceasing", regex=False)
        .str.replace(r"duty ?/?general", "duty-general", regex=True)
        .str.replace(r"(\w+)  ? ?/(\w+)", r"\1/\2", regex=True)
        .str.replace(r"(\w+)   ? ? ? ? ? ?(\w+)", r"\1 \2", regex=True)
        .str.replace(
            "neglect of duty/neglect of duty-failure to act "
            "neglect of duty-failure to act/neglect of duty-failure to act",
            "neglect of duty/neglect of duty-failure to act",
            regex=False,
        )
        .str.replace(r"act ? ?ceasing", "act/ceasing", regex=True)
        .str.replace(r"act (\w+)", r"act/\1", regex=True)
        .str.replace(
            r"/? ?(knowledge)? ?(of)? ?(opso)? ?policy?i?e?s?  ?procedures",
            " knowledge of orleans parish sheriff's office policy procedures",
            regex=True,
        )
        .str.replace(r"(\w+) knowledge", r"\1/knowledge", regex=True)
        .str.replace(r"(\w+)  ?false", r"\1/false", regex=True)
        .str.replace(r"(\w+) supervisory", r"\1/supervisory", regex=True)
        .str.replace(r"(\w+) ceasing", r"\1/ceasing", regex=True)
        .str.replace(r"(\w+) professionalism", r"\1/professionalism", regex=True)
        .str.replace(r" ?(\w+) ?leaving", r"/leaving", regex=True)
        .str.replace(r"(\w+) use", r"\1/use", regex=True)
        .str.replace(r"(\w+) cooperation", r"\1/cooperation", regex=True)
        .str.replace(r"(\w+) neatness", r"\1/neatness", regex=True)
        .str.replace(r" (\w+) interfering", r"/interfering", regex=True)
        .str.replace(r" (\w+) possesse?s?ion", r"\1/possession", regex=True)
        .str.replace(
            r"^professionalism/professionalism professionalism/",
            "professionalism/",
            regex=True,
        )
        .str.replace(r"/ ? ? ?(\w+)", r"/\1", regex=True)
        .str.replace(r"(\w+) neglect", r"\1/neglect", regex=True)
        .str.replace(r"(\w+) devoting", r"\1/devoting", regex=True)
        .str.replace(r"worker ?s", "workers'", regex=True)
        .str.replace(r"/?neglect of/", "neglect of duty", regex=True)
        .str.replace("member devoting", "member/devoting", regex=False)
        .str.replace("ofduty", "of duty", regex=False)
        .str.replace("toduty", "to duty", regex=False)
        .str.replace(r"(\w+) unauthorized", r"\1/unauthorized", regex=True)
        .str.replace(r"(\w+) truthfulness", r"\1/truthfulness", regex=True)
        .str.replace(r"end of ?(tour)? ?(of)? ?(duty)?", "end of tour", regex=True)
        .str.replace("from authoritative", "from authority", regex=False)
        .str.replace(r"(\w+)neglect of duty(\w+)", r"\1/neglect of duty/\2", regex=True)
        .str.replace("memberleaving", "member/leaving", regex=False)
        .str.replace(
            r"ins?str?s?uctions? ?(from)? ?(a)? ?(sus?pp?erv?i?s?i?os?r?)? ?(of)? "
            "?(or)? ?(higher)? ?(ranke?d?)? ?(sr?at?ff)? ?(mm?em?ber)?",
            "instructions from a supervisor or higher ranked staff member/",
            regex=True,
        )
        .str.replace(r"(\w+)  ?instructions", r"\1/instructions", regex=True)
        .str.replace(r"-?/?/", "/", regex=True)
        .str.replace("dutyleaving", "duty/leaving", regex=False)
        .str.replace(r"(\w+) reporting", r"\1/reporting", regex=True)
        .str.replace(r"(\w+) associations", r"\1/associations", regex=True)
        .str.replace(r"(\w+) interfering", r"\1/interfering", regex=True)
        .str.replace(r"/a?uthority/", "", regex=True)
        .str.replace("memberleaving", "member/leaving", regex=False)
        .str.replace(r"/(staff)? ?(mm?em?ber)?/?", "/", regex=True)
        .str.replace("/s/", "/", regex=False)
        .str.replace(r" injury?i?n?g?", "", regex=True)
        .str.replace("resopnsibility", "responsibilities", regex=False)
        .str.replace(r"(\w+) prohibited", r"\1/prohibited", regex=True)
        .str.replace(r"(\w+) courage", r"\1/courage", regex=True)
        .str.replace(r"(\w+) sick", r"\1/sick", regex=True)
        .str.replace("truthfulness/truthfulness/", "truthfulness/", regex=False)
        .str.replace(r"phs?ys?c?ic?al", "physical", regex=True)
        .str.replace(r"(\w+) physical", r"\1/physical", regex=True)
        .str.replace("courtesy intimidation", "courtesy/intimidation", regex=False)
        .str.replace("unfounded", "", regex=False)
        .str.replace(r"/$", "", regex=True)
        .str.replace(r"  +", "", regex=True)
    )
    return df.drop(columns=["charges"])


def split_rows_with_multiple_allegations_20(df):
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
    return df


def split_name_20(df):
    df.loc[:, "name_of_accused"] = (
        df.name_of_accused.str.lower()
        .str.strip()
        .str.replace("la shanda ezidor", "lashanda ezidor", regex=False)
        .str.replace("lieutenant latoya armwood", "latoya armwood", regex=False)
        .str.replace("various", "", regex=False)
        .str.replace("lise", "lisa", regex=False)
    )
    names = (
        df.name_of_accused.str.lower().str.strip().str.extract(r"^(\w+) (\w+ )? ?(.+)$")
    )
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "middle_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns=["name_of_accused"]).fillna("")


def drop_rows_missing_name_20(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean_employee_id_20(df):
    df.loc[:, "employee_id"] = (
        df.emp_no.str.lower()
        .str.strip()
        .str.replace("608199n", "608199", regex=False)
        .str.replace("various", "", regex=False)
    )
    return df.drop(columns="emp_no")


def fix_rank_desc_20(df):
    df.loc[
        (df.rank_desc == "recruit") & (df.last_name == "armwood"), "rank_desc"
    ] = "lieutenant"
    return df


def clean_allegation_desc(df):
    df.loc[:, "allegation_desc"] = (
        df.allegation_desc.str.lower()
        .str.strip()
        .str.replace(r"\brui\b", "resigned under investigation", regex=True)
        .str.replace("ojc", "orleans justice center", regex=False)
        .str.replace(r"lt\.?", "lieutenant", regex=True)
        .str.replace(r"(\w+)   ? ?(\w+)", r"\1 \2", regex=True)
        .str.replace(r"(\w+)/ (\w+)", r"\1/\2", regex=True)
        .str.replace("faiked", "faked", regex=False)
        .str.replace("mgmt", "management", regex=False)
        .str.replace("fialed", "failed", regex=False)
        .str.replace(r"(\w+)$", r"\1.", regex=True)
        .str.replace(r"sgt\.?", "sergeant", regex=True)
        .str.replace("iad", "internal affairs")
    )
    return df


def clean_disposition_20(df):
    df.loc[:, "disposition"] = df.disposition.str.lower().str.strip()

    names = df.disposition.str.extract(
        r"(non-? ?sustaine?d?|sustaine?d?|exonerated|unfounded|founded)"
    )
    df.loc[:, "disposition"] = names[0].str.replace(
        r"non ?sustained", "non-sustained", regex=True
    )
    return df


def extract_suspension_start_date(df):
    df.loc[:, "suspension_start_date"] = (
        df.action.str.replace(r"suspended (\d+)", r"suspeneded on \1", regex=True)
        .str.replace("suspeneded", "suspended", regex=False)
        .str.replace(r"^suspended/arrested on", "suspended on", regex=True)
        .str.replace("1-6-21-1/8/21", "1/6/2021", regex=False)
    )
    dates = df.suspension_start_date.str.extract(r"(suspended on (\d+)/(\d+)/(\d+)/)")
    df.loc[:, "suspension_start_date"] = (
        dates[0]
        .str.replace("suspended on ", "", regex=False)
        .str.replace(r"/$", "", regex=True)
    )
    return df


def extract_suspension_end_date(df):
    df.loc[:, "suspension_end_date"] = (
        df.action.str.lower()
        .str.strip()
        .str.replace(r"\breturn\b", "returned", regex=True)
    )
    dates = df.suspension_end_date.str.extract(r"(returned.+)")
    df.loc[:, "suspension_end_date"] = dates[0].str.replace(
        r"returned ?(back)? ?(to)? ?(duty)? ?(on)? ?", "", regex=True
    )
    return df


def extract_arrest_date(df):
    df.loc[:, "arrest_date"] = df.action
    dates = df.action.str.extract(r"(arrested.+)")
    df.loc[:, "arrest_date"] = (
        dates[0]
        .str.replace("/resigned under investigation on 07/13/2020", "", regex=False)
        .str.replace(r" (12/04/2019)$", "", regex=True)
        .str.replace(r"(/terminated on 04/20/2020)$", "", regex=True)
        .str.replace(
            r"/terminated on ?| on case d-003-2020|arrested ?(on)? ?", "", regex=True
        )
    )
    return df


def extract_resignation_date(df):
    df.loc[:, "resignation_date"] = df.action
    dates = df.action.str.extract(r"(resigned.+)")
    df.loc[:, "resignation_date"] = (
        dates[0]
        .str.replace(r"resigned (under)? ?(investigation)? ?(on)? ?", "", regex=True)
        .str.replace(r" ?prior to going to disciplinary review board ?", "", regex=True)
        .str.replace(
            r" ?at 0930 hours| under Internal affairs department case# d-025 19|"
            r" ?under case number h-049-19 ?|in |from opso on |"
            r" ?prior to start of investigation ",
            "",
            regex=True,
        )
    )
    return df


def extract_termination_date(df):
    df.loc[:, "termination_date"] = df.action
    dates = df.action.str.extract(r"(terminated.+)")
    df.loc[:, "termination_date"] = (
        dates[0]
        .str.lower()
        .str.strip()
        .str.replace(
            r" ?terminated | ?(by)? ?human resources ?(on)? ?|\.|"
            r" ?(by)? ?disciplinary review board ?(on)? ?| ?case k-052-20 on |"
            r" j-052-20 ?|on case h-060-19 prior to receving case l-032-2020|"
            r"/termination was rescinded on 10/1/2020| ?case d-003-2020|"
            r" ?on ?| d-003-2020 ?| under internal affairs department control no. b-006-19|"
            r"per wellpath |/? ?(overturned)? ?by director hodge ?|"
            r"case # b 005-19 ?|06/17/2019",
            "",
            regex=True,
        )
        .str.replace(r"(\d+)/(\d+)/(\d+) (\w+)", r"\1/\2/\3", regex=True)
    )
    return df


def add_left_reason_column(df):
    df.loc[df.resignation_date.notna(), "resignation_left_reason"] = "resignation"
    df.loc[df.termination_date.notna(), "termination_left_reason"] = "termination"
    df.loc[df.arrest_date.notna(), "arrest_left_reason"] = "arrest"

    cols = ["resignation_left_reason", "termination_left_reason", "arrest_left_reason"]

    df.loc[:, "left_reason"] = (
        df[cols]
        .apply(lambda row: "|".join(row.values.astype(str)), axis=1)
        .str.replace("nan", "", regex=False)
        .str.replace(r"\|+", "|", regex=True)
        .str.replace(r"\|$", "", regex=True)
        .str.replace(r"^\|", "", regex=True)
    )
    return df.drop(
        columns={
            "resignation_left_reason",
            "termination_left_reason",
            "arrest_left_reason",
        }
    )


def clean19():
    df = pd.read_csv(
        deba.data("raw/new_orleans_so/new_orleans_so_cprr_2019_tabula.csv")
    )
    df = clean_column_names(df)
    df = (
        df.pipe(remove_header_rows)
        .drop(
            columns=[
                "month",
                "quarter",
                "numb_er_of_cases",
                "related_item_number",
                "a_i",
                "intial_action",
                "inmate_grievance",
                "referred_by",
                "date_of_board",
            ]
        )
        .rename(
            columns={
                "date_received": "receive_date",
                "case_number": "tracking_number",
                "job_title": "rank_desc",
                "charge_disposition": "disposition",
                "location_or_facility": "department_desc",
                "assigned_agent": "investigating_supervisor",
                "emp_no": "employee_id",
                "date_started": "investigation_start_date",
                "date_completed": "investigation_complete_date",
                "terminated_resigned": "action",
                "summary": "allegation_desc",
            }
        )
        .pipe(split_rows_with_multiple_alllegations_19)
        .pipe(clean_allegations_19)
        .pipe(
            remove_carriage_return,
            [
                "name_of_accused",
                "disposition",
                "allegation",
                "allegation_desc",
                "investigating_supervisor",
                "action",
                "department_desc",
                "rank_desc",
            ],
        )
        .pipe(clean_department_desc)
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(split_name_19)
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(clean_action_19)
        .pipe(set_values, {"agency": "New Orleans SO", "data_production_year": "2019"})
        .pipe(process_disposition)
        .pipe(
            fix_date_typos,
            ["receive_date", "investigation_start_date", "investigation_complete_date"],
        )
        .pipe(
            gen_uid,
            ["agency", "employee_id", "first_name", "last_name", "middle_name"],
        )
        .pipe(set_empty_uid_for_anonymous_officers)
        .pipe(gen_uid, ["agency", "tracking_number", "uid"], "allegation_uid")
        .sort_values(["tracking_number", "investigation_complete_date"])
        .drop_duplicates(subset=["allegation_uid"], keep="last", ignore_index=True)
        .pipe(split_investigating_supervisor)
        .pipe(clean_allegation_desc)
        .pipe(extract_arrest_date)
        .pipe(extract_resignation_date)
        .pipe(extract_suspension_start_date)
        .pipe(extract_suspension_end_date)
        .pipe(extract_termination_date)
        .pipe(add_left_reason_column)
    )
    return df


def clean20():
    df = pd.read_csv(
        deba.data("raw/new_orleans_so/new_orleans_so_cprr_2020.csv")
    ).dropna(how="all")
    df = clean_column_names(df)
    df = (
        df.drop(
            columns=[
                "month",
                "quarter",
                "intial_action",
                "number_of_cases",
                "date_of_board",
                "a_i",
                "inmate_grievance",
                "related_item_number",
            ]
        )
        .drop(columns=['referred_by'])
        .rename(
            columns={
                "case_number": "tracking_number",
                "job_title": "rank_desc",
                "charge_disposition": "disposition",
                "location_or_facility": "department_desc",
                "assigned_agent": "investigating_supervisor",
                "terminated_resigned": "action",
                "summary": "allegation_desc",
            }
        )
        .pipe(
            clean_names,
            [
                "investigating_supervisor",
                "name_of_accused",
                "charges",
                "action",
            ],
        )
        .pipe(clean_receive_date_20)
        .pipe(clean_investigation_start_date_20)
        .pipe(clean_investigation_complete_date_20)
        .pipe(clean_action_20)
        .pipe(standardize_action_20)
        .pipe(clean_rank_desc_20)
        .pipe(clean_allegations_20)
        .pipe(split_rows_with_multiple_allegations_20)
        .pipe(split_name_20)
        .pipe(drop_rows_missing_name_20)
        .pipe(clean_employee_id_20)
        .pipe(fix_rank_desc_20)
        .pipe(clean_allegation_desc)
        .pipe(clean_disposition_20)
        .pipe(extract_suspension_start_date)
        .pipe(extract_suspension_end_date)
        .pipe(extract_resignation_date)
        .pipe(extract_arrest_date)
        .pipe(extract_termination_date)
        .pipe(standardize_desc_cols, ["action", "allegation_desc"])
        .pipe(split_investigating_supervisor)
        .pipe(process_disposition)
        .pipe(clean_department_desc)
        .pipe(set_values, {"agency": "New Orleans SO", "data_production_year": "2020"})
        .pipe(float_to_int_str, ["employee_id"])
        .pipe(
            gen_uid, ["agency", "first_name", "middle_name", "last_name", "employee_id"]
        )
        .pipe(
            gen_uid,
            ["tracking_number", "allegation", "action", "employee_id"],
            "allegation_uid",
        )
        .pipe(add_left_reason_column)
    )
    return df


if __name__ == "__main__":
    df19 = clean19()
    df20 = clean20()
    df19.to_csv(deba.data("clean/cprr_new_orleans_so_2019.csv"), index=False)
    df20.to_csv(deba.data("clean/cprr_new_orleans_so_2020.csv"), index=False)
