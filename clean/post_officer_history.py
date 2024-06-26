import deba
import pandas as pd
from lib.uid import gen_uid
from lib.clean import (
    names_to_title_case,
    clean_sexes,
    standardize_desc_cols,
    clean_dates,
)
from lib.columns import set_values
import numpy as np


def drop_rows_missing_names(df):
    return df[~((df.officer_name.fillna("") == ""))]


def split_names(df):
    names = (
        df.officer_name.str.replace(r"^\~", "", regex=True)
        .str.replace(r"\.[\.\,]?", ",", regex=True)
        .str.replace(r"^ROSALIET\, \(No$", "ROSALIET", regex=True)
        .str.replace(r"^6729/2012\,$", "", regex=True)
        .str.replace(r"Officer: ", "", regex=False)
        .str.replace(r"DJUANA7", "djuana", regex=False)
        .str.replace(r"DJUANATWIGGS", "DJUANA TWIGGS", regex=False)
        .str.replace(r"WARRENJ", "WARREN J", regex=False)
        .str.replace(r"MAIAAYANNA", "MAIA AYANNA")
        .str.replace(r"SHELBYTHERESA", "SHELBY THERESA", regex=False)
        .str.replace(r"MICHAELJ", "MICHAEL J", regex=False)
        .str.replace(r"RANDALLC", "RANDALL C", regex=False)
        .str.replace(r"JOSHUAANTHONY", "JOSHUA ANTHONY", regex=False)
        .str.replace(r"TREYJ", "TREY J", regex=False)
        .str.replace(r"LONNIEJ", "LONNIE J", regex=False)
        .str.replace(r"JOSHUAPAUL", "JOSHUA PAUL", regex=False)
        .str.replace(r"NICHOLASJ", "NICHOLAS J", regex=False)
        .str.replace(r"NATHANELJOSEPH", "NATHANEL JOSEPH", regex=False)
        .str.replace(r"DAMONJ", "DAMON J", regex=False)
        .str.replace(r"SHANDREIKAN", "SHANDREIKA N", regex=False)
        .str.replace(r"JOSHUAJERRELL", "JOSHUA JERRELL", regex=False)
        .str.replace(r"CHRISTOPHERJ", "CHRISTOPHER J", regex=False)
        .str.replace(r"WALTERJ", "WALTER J", regex=False)
        .str.replace(r"TERRYJ", "TERRY J", regex=False)
        .str.replace(r"ERICD", "ERIC D", regex=False)
        .str.replace(r"ERICAL", "ERICA L", regex=False)
        .str.replace(r"BRADLEYJ", "BRADLEY J", regex=False)
        .str.replace(r"BEAUJAMES", "BEAU JAMES", regex=False)
        .str.replace(r"AARONL", "AARON L", regex=False)
        .str.replace(r"BRANDONJ", "BRANDON J", regex=False)
        .str.replace(r"TROYJAMES", "TROY JAMES", regex=False)
        .str.replace(r"PATRICKH", "PATRICK H", regex=False)
        .str.replace(r"THOMASJ", "THOMAS J", regex=False)
        .str.replace(r"RICKYL", "RICKY L", regex=False)
        .str.replace(r"TROYJ", "TROY J", regex=False)
        .str.replace(r"RUSTYJ", "RUSTY J", regex=False)
        .str.replace(r"LANCEJ", "LANCE J", regex=False)
        .str.replace(r"KAEGANJOHN", "KAEGAN JOHN", regex=False)
        .str.replace(r"JARRODL", "JARROD L", regex=False)
        .str.replace(r"SHANNONJ", "SHANNON J", regex=False)
        .str.replace(r"RONALDJ", "RONALD J", regex=False)
        .str.replace(r"GEORGEI", "GEORGE I", regex=False)
        .str.replace(r"JEFFREYJ", "JEFFREY J", regex=False)
        .str.replace(r"JEFFREYT", "JEFFREY T", regex=False)
        .str.replace(r"ANGELIQUEI", "ANGELIQUE", regex=False)
        .str.replace(r"GABRIELJ", "GABRIEL J", regex=False)
        .str.replace(r"BRIANJ", "BRIAN J", regex=False)
        .str.replace(r"GREGORYJ", "GREGORY J", regex=False)
        .str.replace(r"JONATHANI", "JONATHAN", regex=False)
        .str.replace(r"CODIJ", "CODI J", regex=False)
        .str.replace(r"CRYSTALLEIGH", "CRYSTAL LEIGH")
        .str.replace(r"AARONJ", "AARON J", regex=False)
        .str.replace(r"JASMYNI", "JASMYN", regex=False)
        .str.replace(r"ROCHELLAAARONINSHA", "ROCHELLA", regex=False)
        .str.replace(r"ANDREWLYNN", "ANDREW LYNN", regex=False)
        .str.replace(r"ARICJAMES", "ARIC JAMES", regex=False)
        .str.replace(r"RANDALLFRANK", "RANDALL FRANK", regex=False)
        .str.replace(r"ERROLLJOSEPH", "ERROLL JOSEPH", regex=False)
        .str.replace(r"SAMANTHAJ", "SAMANTHA J", regex=False)
        .str.replace(r"KEITHANTHONY", "KEITH ANTHONY", regex=False)
        .str.replace(r"PAULLEONARD", "PAUL LEONARD", regex=False)
        .str.replace(r"MONICAANNLACOSTE", "MONICA", regex=False)
        .str.replace(r"ERICL", "ERIC L", regex=False)
        .str.replace(r"SAMUELJAY", "SAMUEL JAY", regex=False)
        .str.replace(r"MICHAELANTHONY", "MICHAEL ANTHONY", regex=False)
        .str.replace(r"GEORGEL", "GEORGE L", regex=False)
        .str.replace(r"MURPHYJ", "MURPHY J", regex=False)
        .str.replace(r"DLEISA", r"D'LEISA", regex=False)
        .str.replace(r"CHRISTOPHERP", "CHRISTOPHER P", regex=False)
        .str.replace(r"BRADJOSEPH", "BRAD JOSEPH", regex=False)
        .str.replace(r"MICHAELR", "MICHAEL R", regex=False)
        .str.replace(r"CARLJ", "CARL J", regex=False)
        .str.replace(r"BETTYJ", "BETTY J", regex=False)
        .str.replace(r"JOSEPHANDREW", "JOSEPH ANDREW", regex=False)
        .str.replace(r"BARNE$", "BARNES-LOVE", regex=False)
        .str.replace(r"JASONJ", "JASON J", regex=False)
        .str.replace(r"JORDANERIC", "JORDAN ERIC", regex=False)
        .str.replace(r"JUSTINPAUL", "JUSTIN PAUL", regex=False)
        .str.replace(r"JOSEPHALLISON", "JOSEPH ALLISON", regex=False)
        .str.replace(r"TERRANCEJ", "TERRANCE J", regex=False)
        .str.replace(r"CHADL", "CHAD L", regex=False)
        .str.replace(r"TMBRIOTIETRIYPAULJ", "", regex=False)
        .str.replace(r"^ST$", "", regex=True)
        .str.replace(r"AMESDENOND", "JAMES", regex=False)
        .str.replace(r"CAOCKETT", "CROCKETT", regex=False)
        .str.replace(r"DANIELT", "DANIEL T", regex=False)
        .str.replace(r"BRITTAINJOHN", "JOHN", regex=False)
        .str.replace(r"PAULJOSEPH", "PAUL JOSEPH", regex=True)
        .str.replace(r"JOSHUALOUIS", "JOSHUA LOUIS", regex=False)
        .str.replace(r"ERICJ", "ERIC J", regex=False)
        .str.replace(r"DANIELJ", "DANIEL J", regex=False)
        .str.replace(r"OSEPIANTHONY", "ANTHONY", regex=False)
        .str.replace(r"TRACYJOHN", "TRACY JOHN", regex=False)
        .str.replace(r"PAULJAMES", "PAUL JAMES", regex=False)
        .str.replace(r"JOHNI", "JOHN I", regex=False)
        .str.replace(r"EDWARDP", "EDWARD P", regex=False)
        .str.replace(r"SHEILAPAUL", "SHEILA PAUL", regex=False)
        .str.replace(r"ALLENJ", "ALLEN J", regex=False)
        .str.replace(r"WILLIAMJ", "WILLIAM J", regex=False)
        .str.replace(r"KEVINJ", "KEVIN J", regex=False)
        .str.replace(r"BARBARALYNN", "BARBARA LYNN", regex=False)
        .str.replace(r"RHONAMARIE", "RHONA MARIE", regex=False)
        .str.replace(r"KEILY", "", regex=False)
        .str.replace(r"EREMYLUKE", "", regex=False)
        .str.replace(r"BRANDONLUKE", "BRANDON LUKE", regex=False)
        .str.replace(r"RALPHC", "RALPH C", regex=False)
        .str.replace(r"OSBPHANTHONY", "ANTHONY", regex=False)
        .str.replace(r"ANTHONYJ", "ANTHONY J", regex=False)
        .str.replace(r"RAMONI", "RAMON I", regex=False)
        .str.replace(r"PAULANDREW", "PAUL ANDREW", regex=False)
        .str.strip()
        .str.extract(r"(\w+(?:'\w+)?),? ?(\w+)(?: (\w+-?\w+?))?")
    )

    df.loc[:, "last_name"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "middle_name"] = names[2].fillna("")
    df = df[df.agency.fillna("").str.contains("/")]

    df.loc[
        (df.first_name == "E") & (df.last_name == "SANCLEMENT"), "first_name"
    ] = "Anny"
    df.loc[
        (df.first_name == "S") & (df.last_name == "BARNES-LOVE"), "first_name"
    ] = "Kanesha"
    df.loc[(df.first_name == "LE") & (df.last_name == "HALL"), "first_name"] = "Lecour"
    df.loc[(df.first_name == "CLAIR") & (df.last_name == "ST"), "first_name"] = "Brian"
    df.loc[
        (df.first_name == "Brian") & (df.last_name == "ST"), "last_name"
    ] = "St. Clair"
    df.loc[
        (df.first_name == "ANNY") & (df.middle_name == "LORENA"), "last_name"
    ] = "Sanclemente-Haynes"
    return df.pipe(names_to_title_case, ["first_name", "middle_name", "last_name"])[
        ~((df.first_name == "") & (df.last_name == ""))
    ].drop(columns=["officer_name"])


def generate_history_id(df):
    # create a copy of the dataframe to avoid modifying the original one
    df_copy = df.copy()

    # generate a unique id for each distinct officer
    df_copy["history_id"] = pd.factorize(df_copy["officer_name"])[0]

    # Convert each 'agency' column into a list, if it's not already
    for column in [
        "agency",
        "agency_1",
        "agency_2",
        "agency_3",
        "agency_4",
        "agency_5",
        "agency_6",
        "agency_7",
        "agency_8",
        "agency_9",
    ]:
        df_copy[column] = df_copy[column].apply(
            lambda x: x if isinstance(x, list) else [x]
        )

    # merge all 'agency' columns into one
    df_copy["agency_all"] = df_copy[
        [
            "agency",
            "agency_1",
            "agency_2",
            "agency_3",
            "agency_4",
            "agency_5",
            "agency_6",
            "agency_7",
            "agency_8",
            "agency_9",
        ]
    ].values.tolist()

    # explode the 'agency_all' column to create a new row for each agency
    df_copy = df_copy.explode("agency_all")

    # drop the rows where 'agency_all' is [nan]
    df_copy = df_copy[df_copy.agency_all.apply(lambda x: x != [np.nan])]

    df_copy["agency_all"] = df_copy["agency_all"].apply(lambda x: ", ".join(x))

    # remove the 'agency' columns as they're not needed
    df_copy = df_copy.drop(
        columns=[
            "agency",
            "agency_1",
            "agency_2",
            "agency_3",
            "agency_4",
            "agency_5",
            "agency_6",
            "agency_7",
            "agency_8",
            "agency_9",
        ]
    )

    # rename 'agency_all' to 'agency'
    df_copy = df_copy.rename(columns={"agency_all": "agency"})

    df_copy = pd.DataFrame(df_copy)

    return df_copy


def clean_agency_pre_split(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
        .str.lower()
        .fillna("")
        .str.replace(r"(\[|\]|\'|\,)", "", regex=True)
    )
    agencies = df.agency.str.extract(r"(.+(time|retired|reserve|deceased).+)")

    df.loc[:, "agency"] = agencies[0]
    return df[~((df.agency.fillna("") == ""))]


def split_agency(df):
    terms = (
        df.agency.str.lower()
        .str.strip()
        .str.extract(
            r"(termination|i?n?voluntary resignation|resignation|other|deceased)"
        )
    )
    df.loc[:, "left_reason"] = (
        terms[0].fillna("").str.replace(r"(\w+) $", r"\1", regex=True)
    )

    dates = df.agency.str.extract(r"(\w+\/\w+\/?\w+) ?(\w+\/\w+\/?\w+)?")
    df.loc[:, "hire_date"] = (
        dates[0]
        .str.replace(r"^d(\w{1})", r"\1", regex=True)
        .str.replace(r"^0\/(.+)", "", regex=True)
        .str.replace(r"(.+)?7209(.+)?", "", regex=True)
        .str.replace(r"^2\/31(.+)", "", regex=True)
        .str.replace(r"^(\w{1,2})\/(\w{1,2})(\w{4})", r"\1/\2/\3", regex=True)
        .str.replace(r"^1/1/1900$", "", regex=True)
        .str.replace(r"(.+)?[a-z](.+)?", "", regex=True)
        .str.replace(r"(.+)?(_|\,|&|-)(.+)?", "", regex=True)
    )
    df.loc[:, "left_date"] = (
        dates[1]
        .str.replace(r"(.+)(_|\,|&|-)(.+)?", "", regex=True)
        .str.replace(r"^0\/(.+)", "", regex=True)
        .str.replace(r"^7/51/2020", "", regex=True)
        .str.replace(r"(.+)?[a-z](.+)?", "", regex=True)
    )

    emp_status = df.agency.str.lower().str.extract(
        r"( ?reserve ?| ?full-?time ?| ?part-?time ?| ?deceased ?| ?retired ?)"
    )
    df.loc[:, "employment_status"] = emp_status[0].str.replace(
        r"^decease$", "deceased", regex=True
    )

    agency = df.agency.str.extract(r"(.+) (\w+)\/(\w+)?")
    df.loc[:, "agency"] = (
        agency[0]
        .str.lower()
        .replace(r"(\w+)-(\w+)?.+", "", regex=True)
        .str.replace(r"( ?reserve| ?deceased| ?retired)", "", regex=True)
        .str.replace(r"(\w+)\/(\w+)\/(\w+)", "", regex=True)
        .str.replace(r"(\“|\(|\"| ?agency ?| ?name ?)", "", regex=True)
    )

    return df[~((df.agency.fillna("") == ""))]


def clean_agency(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
        .str.replace(r"pari?s?h?s?", "", regex=True)
        .str.replace(r"^orleans(.+)so$", "new orleans so", regex=True)
        .str.replace(r"(\w+)so$", r"\1 so", regex=True)
        .str.replace(r"^st(\w+)\b", r"st \1", regex=True)
        .str.replace(r"(:|\.|- |\)|!)", "", regex=True)
        .str.replace(r"1st", "first", regex=False)
        .str.replace(r"(morehous|independenc)b", r"\1", regex=True)
        .str.replace(r"morehous\b", "morehouse", regex=True)
        .str.replace(r"efeliciana", "east feliciana", regex=False)
        .str.replace(r"newc", "new", regex=False)
        .str.replace(r"(\w+)pd", r"\1 pd", regex=True)
        .str.replace(r"univ pd (.+)", r"\1-university-pd")
        .str.replace(r"delgado ?cc", "delgado-community-college", regex=True)
        .str.replace(r"ouachitai", "ouachita", regex=False)
        .str.replace(r"^univi? ?p?d?$", "", regex=True)
        .str.replace(r"^e\b", "east", regex=True)
        .str.replace(r"pearlriver", "pearl river", regex=False)
        .str.replace(r"^new orleans s so$", "new orleans so", regex=True)
        .str.replace(r"jeffersonlevee", "jefferson levee", regex=False)
        .str.replace(r"krotzsprings", "krotz springs", regex=False)
        .str.replace(r"(\w+) univ pd", r"\1-university-pd", regex=True)
        .str.replace(r"shrevbport", "shreveport", regex=False)
        .str.replace(
            r"^bossier  cc-university-pd$",
            "bossier-community-college-university-pd",
            regex=True,
        )
        .str.replace(
            r"(kenner|thibodaux|vincent|lockport|tickfawi|settlement|mandeville|orleans"
            r"|harahan|shreveport|jefferson|sunset|brusly|vidalia|iberville|merryville"
            r"|feliciana|zachary|houma|covington|causeway|gretna|welsh|tickfaw)[ei]",
            r"\1",
            regex=True,
        )
        .str.replace(r"(.+)(pd|so)(.+)(so|pd)(.+)?", "", regex=True)
        .str.replace(
            r"^lsuhsc -no-university-pd$",
            "lsuhsc-new-orleans-university-pd",
            regex=True,
        )
        .str.replace(
            r"^probation & parole adult$", "probation-parole-adult", regex=True
        )
        .str.replace(
            r"^medical center of la no$",
            "medical-center-of-louisiana-new-orleans-pd",
            regex=False,
        )
        .str.replace(r"st ate", "state", regex=False)
        .str.replace(r"--", "", regex=True)
        .str.replace(
            r"^office of youth dev dept of corrections$",
            "office-of-youth-development-department-of-corrections",
            regex=True,
        )
        .str.replace(r"of uvenilejustice", "of juvenile justice", regex=False)
        .str.replace(
            r"^livestock brand comm office of inspector general$", "", regex=True
        )
        .str.replace(r"outof", "out of", regex=False)
        .str.replace(r"lsuhsc no", "lsuhsc-new-orleans", regex=False)
        .str.replace(r"\bno\b", "new orleans", regex=True)
        .str.replace(r"^probation &$", "", regex=True)
        .str.replace(
            r"^medical center of ?(la new orleans)?",
            "medical-center-of-louisiana-new-orleans-pd",
            regex=True,
        )
        .str.replace(r"^orleans da office$", "orleans-da", regex=True)
        .str.replace(
            r"^baton rouge cc-university-pd$",
            "baton-rouge-community-college-university-pd",
            regex=True,
        )
        .str.replace(
            r"^(st ammanyparisf so|st tamimanyparsh so)$", "st tammany so", regex=True
        )
        .str.replace(r"& ", "", regex=False)
        .str.replace(r"^-i? ?", "", regex=True)
        .str.replace(r"ecarrollparise so", "east carroll so", regex=False)
        .str.replace(r"^lakeprovidence pd$", "lake-providence-pd", regex=True)
        .str.replace(r"^st i martinville pd$", "st martinville", regex=True)
        .str.replace(r"^wiestwego pd$", "westwego pd", regex=True)
        .str.replace(r"portallen", "port allen", regex=False)
        .str.replace(r"(caldwbllda|caldwell) ?(da)? ?office", "caldwell-da", regex=True)
        .str.replace(r"charity(.+)", "charity hospital pd", regex=True)
        .str.replace(r"crescentcity", "crescent city", regex=False)
        .str.replace(r"dept\b", "department", regex=True)
        .str.replace(r"harbor pdl$", "harbor pd", regex=True)
        .str.replace(r"independenc", "independence", regex=False)
        .str.replace(r"juvenile services br", "juvenile services bureau", regex=False)
        .str.replace(r"la military", "louisiana military", regex=False)
        .str.replace(r"lastate", "louisiana state", regex=False)
        .str.replace(r"latech", "louisiana tech", regex=False)
        .str.replace(r"lsu medical(.+)bossier(.+)", "", regex=True)
        .str.replace(r"jovenile", "juvenile", regex=False)
        .str.replace(r"westmonroe", "west monroe", regex=False)
        .str.replace(r"(\w+) +(\w+)", r"\1 \2", regex=True)
        .str.replace(r"\s+", "-", regex=True)
        .str.replace(r"b-baton", "east-baton", regex=False)
        .str.replace(r"attormn", "attorney", regex=False)
        .str.replace(r"avoyblles", "avoyelles", regex=False)
        .str.replace(
            r"miss-river-bridge-pd", "mississippi-river-bridge-pd", regex=False
        )
        .str.replace(r"naichitoches", "natchitoches", regex=False)
        .str.replace(
            r"^new-orleans-criminal-court$", "orleans-criminal-court", regex=True
        )
        .str.replace(
            r"(new-orleans-criminal-court-las-state-police|new-orleans-pd-orleans-da-office|"
            r"new-orleans-pd-out-of-state|new-orleans-pd-univ|newort-lans-pd)",
            "",
            regex=True,
        )
        .str.replace(r"portvincent", "port-vincent", regex=False)
        .str.replace(r"--", "-", regex=False)
        .str.replace(
            r"^(shreveport-city-marshal-bossier-city-pd|slidell-city-marshal-slidell-pd|"
            r"state-fire-marshal-kenner-pd|state-fire-marshal-univ|st-landry-so-la-state-police|"
            r"ting-vtoaparish-so|university-pd|west-monroe-pd-west-monroe-marshal)$",
            "",
            regex=True,
        )
        .str.replace(r"sldell", "slidell", regex=False)
        .str.replace(r"southbastern", "southeastern", regex=False)
        .str.replace(
            r"southern--?new-orleans-university-pd",
            "southern-university-pd",
            regex=True,
        )
        .str.replace(r"st-erlington-pd", "sterlington-pd", regex=False)
        .str.replace(r"st-i-mary-so", "st-mary-so", regex=False)
        .str.replace(r"st-martinville$", "st-martinville-pd", regex=True)
        .str.replace(r"-o$", "-so", regex=True)
        .str.replace(r"^orleans-so$", "new-orleans-so", regex=True)
        .str.replace(r"probation-ole-adult", "probation-parole-adult", regex=False)
    )
    return df[~((df.agency.fillna("") == ""))]


def clean_agency_2(df):
    df.loc[:, "agency"] = (
        df.agency.str.replace(
            r"ponch\b|ponciatoula\b|ponciia-lla|ponciiatoula|poncitoula",
            "ponchatoula",
            regex=True,
        )
        .str.replace(r"ia-pd", "pd", regex=False)
        .str.replace(r"detment\b", "department", regex=True)
        .str.replace(r"independencee", "independence", regex=False)
        .str.replace(
            r"(ting-vtoao|1-ngimtoai-so|mississippi-river-bridge-pd|red-river-so-natchitoches-so|"
            r"red-river-o-bossier-city-pd)",
            "",
            regex=True,
        )
        .str.replace(r"(st-ammanyf-so|st-tamimanyo)", "st-tammany-so", regex=True)
        .str.replace(r"orleansp-so", "new-orleans-so", regex=False)
        .str.replace(r"vidalpd", "vidalia-pd", regex=False)
        .str.replace(r"new-iberpd", "new-iberia-pd", regex=False)
        .str.replace(
            r"(ponchatoula-1a-pd|ponciia-ula-pd)", "ponchatoula-pd", regex=True
        )
        .str.replace(r"(.+)faw-pd", "tickfaw-pd", regex=True)
        .str.replace(r"^out-of-state-new-orleans-pd$", "", regex=True)
        .str.replace(r"\/", "-", regex=True)
        .str.replace(r"^ebaton(.+)", r"east-baton-rouge-so")
        .str.replace(r"^ecarr(.+)", r"east-carroll-so")
        .str.replace(r"lafavetteo", "lafayette-so", regex=False)
        .str.replace(r"orlbans", "orleans", regex=False)
        .str.replace(r"(.+)(certifications|training|resignation)(.+)", "", regex=True)
    )
    return df


def clean_employment_status(df):
    df.loc[:, "employment_status"] = (
        df.employment_status.str.replace(r" (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+)time", r"\1-time", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
    )
    return df


def drop_duplicates(df):
    df = df.drop_duplicates(subset=["uid"], keep="first")
    return df


def check_for_duplicate_uids(df):
    uids = df.groupby(["uid"])["history_id"].agg(list).reset_index()

    for row in uids["history_id"]:
        unique = all(element == row[0] for element in row)
        if unique:
            continue
        else:
            raise ValueError("uid found in multiple history ids")

    return df


def switched_job(df):
    df.loc[:, "switched_job"] = df.duplicated(subset=["history_id"], keep=False)
    return df


### add DB metadata and add to docs table


def filter_agencies(df):
    agencies = pd.read_csv(deba.data("raw/agency/agency_reference_list.csv"))
    agencies = agencies.agency_slug.tolist()
    df = df[df.agency.isin(agencies)]
    return df


def split_dates(df):
    dates = df.hire_date.str.extract(r"(\w+)\/(\w+)\/(\w+)")
    df.loc[:, "hire_month"] = dates[0]
    df.loc[:, "hire_day"] = dates[1]
    df.loc[:, "hire_year"] = dates[2]
    return df


def clean_parsed_dates(df):
    df.loc[:, "left_date"] = (
        df.left_date.str.replace(r"16\/2016", r"1/6/2016", regex=True)
        .str.replace(r"(\w+)\/(\w+)\/(\w+)\/(\w+)", "", regex=True)
        .str.replace(r"^(\w{2})\/(\w{4})$", "", regex=True)
        .str.replace(r"^$", "", regex=True)
    )
    df.loc[:, "hire_date"] = df.hire_date.str.replace(
        r"^(\w{2})\/(\w{4})$", "", regex=True
    )
    hire_dates = df.hire_date.str.extract(r"^(\w{1,2})\/(\w{1,2})\/(\w{4})")

    df.loc[:, "hire_month"] = hire_dates[0]
    df.loc[:, "hire_day"] = hire_dates[1]
    df.loc[:, "hire_year"] = hire_dates[2]

    left_dates = df.left_date.str.extract(r"^(\w{1,2})\/(\w{1,2})\/(\w{4})")
    df.loc[:, "left_month"] = left_dates[0]
    df.loc[:, "left_day"] = left_dates[1]
    df.loc[:, "left_year"] = left_dates[2]

    df = df[~((df.hire_month.fillna("") == ""))]
    df = df[~((df.hire_day.fillna("") == ""))]
    df = df[~((df.hire_year.fillna("") == ""))]
    return df


def extract_hire_dates_pprr(df):
    hire_dates = df.hire_date.str.extract(r"^(\w{1,2})\/(\w{1,2})\/(\w{4})")

    df.loc[:, "hire_month"] = hire_dates[0]
    df.loc[:, "hire_day"] = hire_dates[1]
    df.loc[:, "hire_year"] = hire_dates[2]
    return df


def clean_hire_dates_concat(df):
    df.loc[:, "hire_date"] = df.hire_date.str.replace(r"^nan/nan/nan$", "", regex=True)
    return df[~((df.hire_date.fillna("") == ""))]


def clean():
    dfa = pd.read_csv(deba.data("ner/advocate_post_officer_history_reports.csv"))
    dfb = pd.read_csv(deba.data("ner/post_officer_history_reports.csv"))
    dfc = pd.read_csv(deba.data("ner/post_officer_history_reports_2022.csv"))
    dfd = pd.read_csv(deba.data("ner/post_officer_history_reports_2022_rotated.csv"))
    dfe = pd.read_csv(deba.data("ner/post_officer_history_reports_2023.csv"))
    df = (
        pd.concat([dfa, dfb, dfc, dfd, dfe])
        .pipe(drop_rows_missing_names)
        .rename(
            columns={
                "officer_sex": "sex",
            }
        )
        .drop(
            columns=[
                "filesha1",
                "pageno",
                "paragraphs",
                "ocr_status",
                "md5",
                "filepath",
                "fileid",
                "filetype",
                "file_category",
            ]
        )
        .pipe(clean_sexes, ["sex"])
        .pipe(generate_history_id)
        .pipe(split_names)
        .pipe(clean_agency_pre_split)
        .pipe(split_agency)
        .pipe(clean_agency)
        .pipe(clean_agency_2)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(drop_duplicates)
        .pipe(check_for_duplicate_uids)
        .pipe(switched_job)
        .pipe(set_values, {"source_agency": "post"})
        .pipe(standardize_desc_cols, ["agency"])
        .pipe(filter_agencies)
        .pipe(
            standardize_desc_cols,
            [
                "employment_status",
                "left_reason",
                "hire_date",
                "left_date",
            ],
        )
        .pipe(clean_parsed_dates)
    )
    return df


def pprr_post():
    df = (
        pd.read_csv(deba.data("raw/post_council/pprr_post_2023_v_post_2020.csv"))
        .pipe(standardize_desc_cols, ["hire_date"])
        .pipe(extract_hire_dates_pprr)
    )
    return df


def concat_dfs(dfa, dfb):
    df = pd.concat([dfa, dfb])
    df = df.pipe(clean_hire_dates_concat).pipe(check_for_duplicate_uids)
    return df


if __name__ == "__main__":
    dfa = clean()
    dfb = pprr_post()
    df = concat_dfs(dfa, dfb)
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)
