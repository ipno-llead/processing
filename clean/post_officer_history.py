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


def drop_rows_missing_names(df):
    return df[~((df.officer_name.fillna("") == ""))]


def split_names(df):
    names = (
        df.officer_name.str.replace(r"^\~", "", regex=True)
        .str.replace(r"\.[\.\,]?", ",", regex=True)
        .str.replace(r"^ROSALIET\, \(No$", "ROSALIET", regex=True)
        .str.replace(r"^6729/2012\,$", "", regex=True)
        .str.replace(r"Officer: ", "", regex=False)
        .str.strip()
        .str.extract(r"(\w+(?:'\w+)?),? ?(\w+)(?: (\w+))?")
    )

    df.loc[:, "last_name"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "middle_name"] = names[2].fillna("")
    df = df[df.agency.fillna("").str.contains("/")]
    return df.pipe(names_to_title_case, ["first_name", "middle_name", "last_name"])[
        ~((df.first_name == "") & (df.last_name == ""))
    ].drop(columns=["officer_name"])


def generate_history_id(df):
    stacked_agency_sr = df[
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
    ].stack()

    stacked_agency_df = stacked_agency_sr.reset_index().iloc[:, [0, 2]]
    stacked_agency_df.columns = ["history_id", "agency"]

    names_df = df[
        [
            "officer_name",
        ]
    ].reset_index()
    names_df = names_df.rename(columns={"index": "history_id"})

    stacked_agency_df = stacked_agency_df.merge(names_df, on="history_id", how="right")

    return stacked_agency_df[~((stacked_agency_df.agency.fillna("") == ""))]


def clean_agency_pre_split(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
        .str.lower()
        .fillna("")
        .str.replace(r"(\[|\]|\'|\,)", "", regex=True)
    )
    agencies = df.agency.str.extract(r"(.+(time|retired|reserve|deceased).+)")

    df.loc[:, "agency"] = agencies[0]
    return df


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


def clean():
    dfa = pd.read_csv(deba.data("ner/advocate_post_officer_history_reports.csv"))
    dfb = pd.read_csv(deba.data("ner/post_officer_history_reports.csv"))
    dfc = pd.read_csv(deba.data("ner/post_officer_history_reports_2022.csv"))
    dfd = pd.read_csv(deba.data("ner/post_officer_history_reports_2022_rotated.csv"))
    dfe = pd.read_csv(deba.data("ner/post_officer_history_reports_2023.csv"))
    df = (
        pd.concat([dfa, dfb, dfc, dfd, dfe], axis=0, ignore_index=True)
        .pipe(drop_rows_missing_names)
        .rename(
            columns={
                "officer_sex": "sex",
            }
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


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)
