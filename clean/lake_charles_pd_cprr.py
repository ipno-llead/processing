import pandas as pd
import deba
from lib.clean import float_to_int_str
from lib.columns import clean_column_names
from lib.rows import duplicate_row
from lib.uid import gen_uid
from lib.standardize import standardize_from_lookup_table
import re


disposition_lookup = [
    ["exonerated", "closeexhor", "close/exhan"],
    [
        "sustained; resigned",
        "sust/r",
        "sust//r",
        "sustr/r",
        "sust /r",
        "sustr",
        "bust//r",
        "sustr the",
        "sustresign",
        "sust/",
        "sust//",
        "bust/",
        "singe/resign",
        "sustained; resigned",
    ],
    [
        "resigned",
        "name /resigne",
        "resigne",
        "susp/resign",
        "clogul resigne",
        "kesigne",
        "/r",
        ". term resign",
        "close resisne",
    ],
    ["no investigation merited", "no invest,", "no. invest."],
    [
        "sustained",
        "syst",
        "ast/",
        "sust /",
        "syst/",
        "sast /",
        "sust tyr",
        "elfet sust/this an",
        "sust",
        "susth",
        "sust/aya",
        "sustflo",
        "sust//eap",
        "sustl",
        "sast./2days",
        "sast. /w",
    ],
    ["sustained/exonerated", "sust exoa", "erhon/yst"],
    ["unfounded", "unfoune"],
    [
        "exonerated",
        "examprate",
        "exomerate",
        "exopplate",
        "exhoreate",
        "exhonerate",
        "exhonesete",
        "exhone rate",
        "excenerate",
        "experate",
        "estay",
        "eranente",
        "examerate",
        "exomenate",
        "exonepte",
        "exokerate",
        "exon / unformal",
    ],
    ["not justified", "notjustified", "not just."],
    [
        "justified",
        "justified!",
        "justified",
        "just. 12day",
        "just, (2day",
        "just.,lld",
        "just wr",
        "just./vr",
        "just! vr",
        "3ust//3day",
        "just /wr",
        "just/ vr",
        "just/wr",
        "just/7day",
        "just ",
        "just. bday",
    ],
    [
        "justified/resigned",
        "justified/r",
        "justifiedb",
        "justifiedll",
        "justifiedresigne",
    ],
    ["no fault", "no fnest"],
    ["not sustained", "not sust", "not sast"],
]


allegations_lookup = [
    [
        "fleet crash",
        "fleet crash crash",
        "fleet crash gash",
        "gash",
        "fleet crash cash",
        "fleet crash mash",
        "crash",
        "fileet ",
        "/gast",
        " cigsh",
    ],
    ["adherence to law", "adherenceency to law ments", "adhan to law"],
    [
        "unsatisfactory performance",
        "performance",
        "unzatisfactory performance",
        "unsatisfantary performance",
        "unsetisfactory performance",
        "unsetofec tay performance",
        "unitisfactory fe form",
        "unsptar performance",
        "ungat performance",
        "unsatisfactory",
        "unsatisfectory performance",
        "unsatistactory performance",
        "unsatperformance",
        "unsatisfactory ferf",
        "unsatisfactory restory",
        "unsada performance",
        "unsatisfactory uasat performance",
        "unsatperform",
        "unsatfertorm",
    ],
    [
        "unauthorized force",
        "unauthogized force",
        "unanthorized force",
        "unauthf force",
        "unauth force",
        "unsatfertorm",
        "unauth",
    ],
    [
        "neglect of duty",
        "neglect of duty of buty",
        "neglect of duty of duty",
        "neglect of duty of wuty",
        "wednefof daty",
    ],
    ["theft/false arrest", "theft /fase arrest"],
    [
        "false or inaccurate statement",
        "falze ordnaccumate state",
        "false on fracanta",
        "false or strace statement",
    ],
    ["sexual harrassment", "sexual has massment", "harassment"],
    [
        "use of department equipment",
        "use of deptequip",
        "use f lept equipment",
        "of rept equipment use",
        "department vehicles",
    ],
    ["two counts of insubordination"],
    [
        "insubordination",
        "ansabordination",
        "ansubord linbering",
        "disaboridation",
        "dispordingtion",
        "huspbodization",
    ],
    ["insubordination; conduct unbecoming", "ansub /conduct unbecon"],
    ["unauthorized force", "lignauth force", "unaathorized force"],
    ["adherence to law", "adner to", "adherenceence to law", "to law", "addre to ("],
    ["conduct unbecoming", "/conduct"],
    ["chain of command", "chaind com"],
    ["discrimination/bias", "discrimination isias"],
    [
        "unsatisfactory performance/insubordination",
        "unsatient ansupendination",
        "ansub",
    ],
    ["tardiness", "tandiness"],
    ["officer involved shooting", "officer", "office involved streeting f"],
    ["assocations", "associations"],
    ["abuse of position", "abuse of "],
    ["workplace violence"],
    ["employee harrassment", "employee contact"],
]


def clean_allegations_20(df):
    df.loc[:, "allegation"] = (
        df.nature_of_complaint.str.lower()
        .str.strip()
        .str.replace(r"\. ?", " ", regex=True)
        .str.replace(",", "", regex=False)
        .str.replace(r"(\d+) ", "", regex=True)
        .str.replace("unsat perform", "unsatisfactory performance", regex=False)
        .str.replace("neg of duty", "neglect of duty", regex=False)
        .str.replace(r"cond unbecom(ing)?", "conduct unbecoming", regex=True)
        .str.replace("unauth force", "unauthorized force", regex=False)
        .str.replace("use of dept equip", "use of department equipment", regex=False)
        .str.replace("unknown", "", regex=False)
    )
    return df.drop(columns="nature_of_complaint")


def split_rows_with_multiple_allegations_20(df):
    i = 0
    for idx in df[df.allegation.str.contains(r"/")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df


def clean_action_20(df):
    df.loc[:, "action"] = (
        df.action_taken.str.lower()
        .str.strip()
        .str.replace(r"no action ?(tak[eo]n)?", "", regex=True)
    )
    return df.drop(columns=("action_taken"))


def consolidate_action_and_disposition_20(df):
    df.loc[:, "action"] = (
        df.action.str.cat(df.disposition, sep="|")
        .str.replace(
            r"((not)? ?sustained|exonerated|unfounded|invalid complaint) ?",
            "",
            regex=True,
        )
        .str.replace(r"^\|", "", regex=True)
        .str.replace(r"\|$", "", regex=True)
        .str.replace(r"(\d+) (\w+)", r"\1-\2", regex=True)
    )
    return df


def clean_disposition_20(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace("invalid complaint", "", regex=False)
        .str.replace("suspended", "", regex=False)
    )
    return df


def split_rows_with_multiple_officers_20(df):
    i = 0
    for idx in df[df.name.str.contains(r"/|,")].index:
        s = df.loc[idx + i, "name"]
        parts = re.split(r"\s*(?:/|,)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "name"] = name
        i += len(parts) - 1
    return df


def drop_rows_missing_disp_allegations_and_action_20(df):
    return df[~((df.disposition == "") & (df.allegation == "") & (df.action == ""))]


def assign_empty_first_name_column_20(df):
    df.loc[:, "first_name"] = ""
    return df


def assign_first_names_from_post_20(df):
    df.loc[:, "name"] = (
        df.name.str.lower()
        .str.strip()
        .str.replace("torres", "torres paul", regex=False)
        .str.replace("redd", "redd jeffrey", regex=False)
        .str.replace("romero", "romero mayo", regex=False)
        .str.replace("nevels", "nevels harold", regex=False)
        .str.replace("manuel", "manuel carlos", regex=False)
        .str.replace("morrow", "morrow errel", regex=False)
        .str.replace("mccloskey", "mccloskey john", regex=False)
        .str.replace("myers", "myers hannah", regex=False)
        .str.replace("mccue", "mccue eddie", regex=False)
        .str.replace("mills", "mills logan", regex=False)
        .str.replace("dougay", "dougay bennon", regex=False)
        .str.replace("saunier", "saunier john", regex=False)
        .str.replace("ewing", "ewing joshua", regex=False)
        .str.replace("johnson", "johnson martin", regex=False)
        .str.replace("jackson", "jackson princeton", regex=False)
        .str.replace("baccigalopi", "baccigalopi dakota", regex=False)
        .str.replace("breaux", "breaux keithen", regex=False)
        .str.replace("falcon", "falcon bendy", regex=False)
        .str.replace("ford", "ford raymond", regex=False)
        .str.replace("perkins", "perkins carlton", regex=False)
        .str.replace("ponthieaux", "ponthieaux wilbert", regex=False)
        .str.replace("markham", "markham alan", regex=False)
    )
    names = df.name.str.extract(r"(\w+) ?(.+)?")
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1].fillna("")
    return df.drop(columns="name")


def assign_agency(df):
    df.loc[:, "agency"] = "lake-charles-pd"
    return df


def clean_tracking_id_19(df):
    df.loc[:, "tracking_id"] = (
        df.iad_file.str.replace(" ", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace(r":|\.", "-", regex=True)
        .str.replace(r"(\d{1})(\d{1})(\d{1})", r"\1\2-\3", regex=True)
        .str.replace("14-68", "16-68", regex=False)
    )
    return df.drop(columns="iad_file")


def clean_complainant_19(df):
    df.loc[:, "complainant"] = (
        df.complainant_s.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(
            r"(rcpb|ucps|ups|^\/|kepb|admini station lcpa)", "lcpd", regex=True
        )
        .str.replace(
            r"^l(.+)", "lake charles police department or sheriffs office", regex=True
        )
        .str.replace(r"^(?!lake).*", "", regex=True)
    )
    return df.drop(columns="complainant_s")


def extract_rank_from_name_19(df):
    df.loc[:, "rank_desc"] = (
        df.officer_s_accused.str.lower()
        .str.strip()
        .str.replace(r"(,|\.)", "", regex=True)
        .str.replace("brister", "cpl brister", regex=False)
        .str.replace(r"c?g?pl?6?\.? ", "corporal ", regex=True)
        .str.replace(r"^p\.?d?o?\.?,?\.?i?\.? ", "parole officer ", regex=True)
        .str.replace(r"^e\.?d?\.?p?6?\.?o?\.?s?\.? ", "evidence officer ", regex=True)
        .str.replace(r"^5?s?gt\.? ", "sergeant ", regex=True)
    )
    ranks = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.extract(r"(corporal|parole officer|evidence officer|sergeant)")
    )
    df.loc[:, "rank_desc"] = ranks[0].fillna("")
    return df


def clean_names_19(df):
    df.loc[:, "officer_s_accused"] = (
        df.officer_s_accused.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(",", "", regex=False)
        .str.replace(r"c?g?pl?6?\.? ", "", regex=True)
        .str.replace(r"^p\.?d?o?\.?,?\.?i?\.? ", "", regex=True)
        .str.replace(r"^e\.?d?\.?p?6?\.?o?\.?s?\.? ", "", regex=True)
        .str.replace(r"^5?s?gt\.? ", "", regex=True)
        .str.replace("stickell stickell", "stickell", regex=False)
        .str.replace(
            r"riveraalicea|rivesa alecie|riveraalecia|rivera - alicea",
            "rivera-alecia",
            regex=True,
        )
        .str.replace(r"unf?k?nown??l?y?", "", regex=True)
        .str.replace(r"^(\w{1})\.(\w+)", r"\1. \2", regex=True)
        .str.replace(r"^k\.$", "k. holiday", regex=True)
        .str.replace(" haliday", " holiday", regex=False)
        .str.replace(r"(^holiday$|l. registration)", "k. holiday", regex=True)
        .str.replace(r"^(\w+) (\w{1})\.? ", r"\2. ", regex=True)
        .str.replace(r"^5\. clouse$", "s. clouse", regex=True)
        .str.replace(r"^n$", "", regex=True)
        .str.replace(r"^gruspier$", "g. gruspier", regex=True)
        .str.replace(r"^k$", "k. washington", regex=True)
        .str.replace(r"^m\.$", "m. montgomery", regex=True)
        .str.replace(r"^b\.$", "b. ewing", regex=True)
        .str.replace(r"^a\.$", "", regex=True)
        .str.replace(r"^k. mixon to", "r. mixon", regex=True)
        .str.replace(r"^byb? agillory", "branden guillory", regex=True)
        .str.replace(r"^an\b ", "a. ", regex=True)
        .str.replace(r"^/ saunigg", "john saunier", regex=True)
        .str.replace(r"^4. smith", "joseph smith", regex=True)
        .str.replace(r"^f simien 1", "josh simien", regex=True)
        .str.replace(r"^5. doughary", "scott dougerty", regex=True)
        .str.replace(r"^c\.$", "c. young", regex=True)
        .str.replace(r"^officer\(s\) accused$", "", regex=True)
        .str.replace(r"^1. hebert b. it neeley$", "a. williams", regex=True)
        .str.replace(r"^lajessika jalk$", "lajessika jack", regex=True)
        .str.replace(r"^h. nevels 1.4. miller$", "h. nevels/a. miller", regex=True)
        .str.replace(r" f[ao]nten[op]t$", " fontenot", regex=True)
        .str.replace(r"^l.$", "l. jack", regex=True)
        .str.replace(r"^r$", "", regex=True)
        .str.replace(r"^j\.$", "", regex=True)
        .str.replace(r"^j. ex littletony$", "j. littleton", regex=True)
        .str.replace(r"^5. kingsley$", "s. kingsley", regex=True)
        .str.replace(r"^d.$", "", regex=True)
        .str.replace(r"^f$", "", regex=True)
        .str.replace(r"^to wofford$", "j. walford", regex=True)
        .str.replace(
            r"(mg kaughenbaugh|c\. daughen baugh)", "marie daughenbaugh", regex=True
        )
        .str.replace(r"j. eving", "j. ewing/marie daughenbaugh", regex=True)
        .str.replace(r"^2. harrell", "r. harrell", regex=True)
        .str.replace(r"i?\.? ?stickell", "j. stickell", regex=True)
        .str.replace(r"lig janice", "robert janice", regex=True)
        .str.replace(r"^p\.$", "", regex=True)
        .str.replace(r"(\w{1})\.? holiday", r"k. holiday", regex=True)
        .str.replace(r"s\.? m ?'?[dbp]aniel?/?", "s. mcdaniel", regex=True)
        .str.replace(r"c\. booth", "chad booth", regex=True)
        .str.replace(r"(\w+) /(\w+)", r" \1/\2", regex=True)
        .str.replace(r"^j. antonistration$", "j. redd/n. stratton", regex=True)
        .str.replace(r"t\.? bup[fl]echan", "t. suplechan", regex=True)
        .str.replace(" flurcy/p.", "michael flurry/c. breland", regex=True)
        .str.replace(r"b?\.? ?randolph", "benjamin randolph", regex=True)
        .str.replace(" comville", " courville", regex=False)
        .str.replace(" veiller", " veillon", regex=False)
        .str.replace(r"c\.  ?manu?el?", "carlos manuel", regex=True)
        .str.replace(r"^a.  ake/c.boudin$", "a. ake/c. boudin", regex=True)
        .str.replace("cbbeland", "c. breland", regex=False)
        .str.replace("hammee", "hammer", regex=False)
        .str.replace(" manual", " manuel", regex=False)
        .str.replace(r"^ryjjennis$", "russell dennis", regex=True)
        .str.replace(" \bdenyis\b", " dennis", regex=True)
        .str.replace(" williays", " williams", regex=False)
        .str.replace(r"(\w+) \b(\w{1})\b", r"\2 \1", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"^(\w+)$", r" \1", regex=True)
        .str.replace(r"^ i$", "jonathan landrum", regex=True)
        .str.replace(r"^j n", "j", regex=True)
    )
    return df


def assign_missing_names_19(df):
    df.loc[(df.tracking_id == "16-7"), "officer_s_accused"] = "a malveaux"
    df.loc[(df.tracking_id == "19-54"), "officer_s_accused"] = "a aeheb"
    df.loc[(df.tracking_id == "17-36"), "officer_s_accused"] = "john saunier"
    df.loc[(df.tracking_id == "17-38"), "officer_s_accused"] = "j littleton"
    return df


def split_rows_with_multiple_officers_19(df):
    i = 0
    for idx in df[df.officer_s_accused.str.contains("/")].index:
        s = df.loc[idx + i, "officer_s_accused"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer_s_accused"] = name
        i += len(parts) - 1
    return df


def assign_first_names_from_post_19(df):
    df.loc[:, "officer_s_accused"] = (
        df.officer_s_accused.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("s clouse", "samuel clouse", regex=False)
        .str.replace("j courville", "julia courville", regex=False)
        .str.replace("r rainwater", "robert rainwater", regex=False)
        .str.replace("j redd", "jeffrey redd", regex=False)
        .str.replace("s kingsley", "samuel kingsley", regex=False)
        .str.replace("j savoie", "joe savoie", regex=False)
        .str.replace("j saunier", "john saunier", regex=False)
        .str.replace("h nevels", "harold nevels", regex=False)
        .str.replace("b dommert", "bret dommert", regex=False)
        .str.replace("t duplechan", "timothy duplechan", regex=False)
        .str.replace("s dougherty", "scott dougherty", regex=False)
        .str.replace("l mills", "logan mills", regex=False)
        .str.replace("j wall", "judith wall", regex=False)
        .str.replace("m wilson", "michael wilson", regex=False)
        .str.replace("whitfield", "michael whitfield", regex=False)
        .str.replace("k washington", "kalon washington", regex=False)
        .str.replace("g geheb", "gary geheb", regex=False)
        .str.replace("k hoover", "kevin hoover", regex=False)
        .str.replace("t toten", "travis toten", regex=False)
        .str.replace("m johnson", "martin johnson", regex=False)
        .str.replace("c johnson", "christopher johnson", regex=False)
        .str.replace("h rivera-alicea", "hector rivera-alicea", regex=False)
        .str.replace("r milled", "rickey miller", regex=False)
        .str.replace("gardener", "phillip gardiner", regex=False)
        .str.replace("r rathbenn", "ross rathburn", regex=False)
        .str.replace("m treadery", "michael treadway", regex=False)
        .str.replace("t scheoner", "thadius shecher", regex=False)
        .str.replace("t masee", "tony magee", regex=False)
        .str.replace("j mccopley", "john mccloskey", regex=False)
        .str.replace("m bentrand", "mandy bertrand", regex=False)
    )
    return df


def split_names_19(df):
    names = df.officer_s_accused.fillna("").str.extract(r"(?:(\w+) )? ?(.+)")
    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "last_name"] = names[1].fillna("")
    return df.drop(columns="officer_s_accused")


def drop_rows_missing_name_19(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def assign_allegations_19(df):
    df.loc[(df.tracking_id == "19-2"), "allegation"] = "fleet crash"
    df.loc[
        (df.tracking_id == "17-14"), "allegation"
    ] = "unauthorized force/unsatisfactory performance"
    return df


def clean_investigation_start_date_19(df):
    df.loc[:, "investigation_start_date"] = (
        df.date.str.replace("-", "/", regex=False)
        .str.replace(".", "/", regex=False)
        .str.replace("9817", "", regex=False)
        .str.replace("412", "4/12", regex=False)
        .str.replace("48/19", "4/8/2019", regex=False)
        .str.replace("3716", "3/17/2016", regex=False)
        .str.replace("5/416", "5/4/2016", regex=False)
        .str.replace(r"(\d+) $", r"\1", regex=True)
        .str.replace("Apr", "04", regex=False)
        .str.replace("5/417", "5/04/2017", regex=False)
        .str.replace("10/24116", "10/24/2016", regex=False)
        .str.replace("04/17", "", regex=False)
        .str.replace("4/62/18", "4/26/2018", regex=False)
    )
    return df.drop(columns="date")


def clean_allegations_19(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("  ", " ", regex=False)
        .str.replace("if", "of", regex=False)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r", ?", "", regex=True)
        .str.replace(r"un?g?h?ss?ae?l?f?t?i?y?\b", "unsatisfactory", regex=True)
        .str.replace("viplence", "violance", regex=False)
        .str.replace("chaindcom", "chain of command", regex=False)
        .str.replace("ha passment", "harrassment", regex=False)
        .str.replace(r"destequip|equit", "department equipment", regex=True)
        .str.replace(r"profes?s?i?o?n?a?l?i?s?t?y?", "professionalism", regex=True)
        .str.replace(
            r"insubordination \(2cts\)", "two counts of insubordination", regex=True
        )
        .str.replace("uncuthorized", "unauthorized", regex=False)
        .str.replace(r"d?w?uty", "duty", regex=False)
        .str.replace("olinace", "online", regex=False)
        .str.replace("adher", "adherence", regex=False)
        .str.replace(r"\bp(\w+)", "performance", regex=True)
        .str.replace(r"^co(.+)", "conduct unbecoming", regex=True)
        .str.replace(r"^fl(\w+)", "fleet crash", regex=True)
        .str.replace(r"^n(\w+)", "neglect of duty", regex=True)
    )
    return standardize_from_lookup_table(df, "allegation", allegations_lookup).drop(
        columns="complaint"
    )


def extract_actions_from_disposition_19(df):
    actions = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace("/", "", regex=False)
        .str.replace(r"re[dp]", "reprimand", regex=True)
        .str.replace(r"verbal$|g?s?f?iverbal reprimand", "verbal reprimand", regex=True)
        .str.replace("1 d day", "1day")
        .str.extract(r"( ?\d+ ?|susp|wr|vr|verbal reprimand|term|whitten bust rep)")
    )
    df.loc[:, "action"] = (
        actions[0]
        .fillna("")
        .str.replace("/", "", regex=False)
        .str.replace(r"^ | $", "", regex=True)
        .str.replace(r"(\d+) ?[wd]a?y?s?", r"\1-day suspension", regex=True)
        .str.replace(r"(\d+)$", r"\1-day suspension", regex=True)
        .str.replace(r"(wr|whitten bust rep)", "written reprimand", regex=True)
        .str.replace("vr", "verbal reprimand", regex=False)
        .str.replace("term", "termination", regex=False)
        .str.replace(r"\bsusp\b", "suspenion", regex=True)
    )
    return df


def clean_disposition_19(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("  ", " ", regex=False)
    )
    return standardize_from_lookup_table(df, "disposition", disposition_lookup)


def clean_and_split_investigator_19(df):
    df.loc[:, "investigator"] = (
        df.investigator.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("refferred", "", regex=False)
        .str.replace(r",|< |-|\.", "", regex=True)
        .str.replace(" correct", "", regex=False)
        .str.replace(r"^(\w{1})\.? ?", "", regex=True)
        .str.replace(r"^(\w+) (\w+)", r"\1\2", regex=True)
        .str.replace(
            r"^(a|h|t|k|i|e|s|n|d)(\w+)", "lieutenant richard harrell", regex=True
        )
        .str.replace(r"^(c|f)(\w+)", "deputy kirk carroll", regex=True)
        .str.replace(r"^g(\w+)", "dustin gaudet", regex=True)
    )

    names = df.investigator.str.extract(r"(lieutenant|deputy) (\w+) (\w+)")
    df.loc[:, "investigator_rank_desc"] = names[0]
    df.loc[:, "investigator_first_name"] = names[1]
    df.loc[:, "investigator_last_name"] = names[2]

    return df.drop(columns="investigator")


def clean_20():
    df = pd.read_csv(deba.data("raw/lake_charles_pd/lake_charles_pd_cprr_2020.csv"))
    df = (
        df.pipe(clean_column_names)
        .rename(columns={"date_of_investigation": "investigation_start_date"})
        .pipe(clean_allegations_20)
        .pipe(split_rows_with_multiple_allegations_20)
        .pipe(clean_action_20)
        .pipe(consolidate_action_and_disposition_20)
        .pipe(clean_disposition_20)
        .pipe(split_rows_with_multiple_officers_20)
        .pipe(drop_rows_missing_disp_allegations_and_action_20)
        .pipe(assign_empty_first_name_column_20)
        .pipe(assign_first_names_from_post_20)
        .pipe(assign_agency)
        .pipe(float_to_int_str, ["investigation_start_date"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "first_name",
                "last_name",
                "investigation_start_date",
                "allegation",
                "action",
            ],
            "allegation_uid",
        )
    )
    return df


def clean_19():
    df = (
        pd.read_csv(deba.data("raw/lake_charles_pd/lake_charles_pd_cprr_2014_2019.csv"))
        .pipe(clean_column_names)
        .drop(columns=["shift"])
        .pipe(clean_investigation_start_date_19)
        .pipe(clean_tracking_id_19)
        .pipe(clean_complainant_19)
        .pipe(extract_rank_from_name_19)
        .pipe(clean_names_19)
        .pipe(assign_missing_names_19)
        .pipe(split_rows_with_multiple_officers_19)
        .pipe(assign_first_names_from_post_19)
        .pipe(split_names_19)
        .pipe(drop_rows_missing_name_19)
        .pipe(assign_allegations_19)
        .pipe(clean_allegations_19)
        .pipe(extract_actions_from_disposition_19)
        .pipe(clean_disposition_19)
        .pipe(clean_and_split_investigator_19)
        .pipe(assign_agency)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "first_name",
                "last_name",
                "tracking_id",
                "investigation_start_date",
                "allegation",
                "action",
            ],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df20 = clean_20()
    df19 = clean_19()
    df20.to_csv(deba.data("clean/cprr_lake_charles_pd_2020.csv"), index=False)
    df19.to_csv(deba.data("clean/cprr_lake_charles_pd_2014_2019.csv"), index=False)
