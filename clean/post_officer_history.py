import deba
import pandas as pd
from lib.uid import gen_uid
from lib.clean import names_to_title_case, clean_sexes, standardize_desc_cols
from lib.columns import set_values


def drop_rows_missing_names(df):
    return df[~((df.officer_name.fillna("") == ""))]


def split_names(df):
    names = (
        df.officer_name.str.replace(r"^\~", "", regex=True)
        .str.replace(r"\.[\.\,]?", ",", regex=True)
        .str.replace(r"^ROSALIET\, \(No$", "ROSALIET", regex=True)
        .str.replace(r"^6729/2012\,$", "", regex=True)
        .str.strip()
        .str.extract(r"(\w+(?:'\w+)?),? ?(\w+)(?: (\w+))?")
    )

    df.loc[:, "last_name"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "middle_name"] = names[2].fillna("")
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
            "agency_10",
            "agency_11",
            "agency_12",
            "agency_13",
            "agency_14",
            "agency_15",
            "agency_16",
            "agency_17",
            "agency_18",
            "agency_19",
            "agency_20",
            "agency_21",
            "agency_22",
            "agency_23",
            "agency_24",
            "agency_25",
            "agency_26",
            "agency_27",
            "agency_28",
            "agency_29",
            "agency_30",
            "agency_31",
            "agency_32",
            "agency_33",
            "agency_34",
            "agency_35",
            "agency_36",
            "agency_36",
            "agency_37",
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

    return stacked_agency_df


def clean_agency_pre_split(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
        .str.lower()
        .fillna("")
        .str.replace(r"time — (\w+)\/(\w+)\/(\w+)", r"time \1/\2/\3", regex=True)
        .str.replace(r"(\w)\/(\w{2})(\w{4})", r"\1/\2/\3", regex=True)
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office",
            "orleans coroners office",
            regex=True,
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--(\w+)", r"\1", regex=True)
        .str.replace(r"bull-time", "full-time", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" \_ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) \_ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.replace(r"-time(\w+\/\w+\/\w+)", r"-time \1", regex=True)
        .str.replace(r"\'", "", regex=True)
        .str.replace(r"_(\w+)", r"\1", regex=True)
        .str.replace(
            r"^(\w+? ?\w+? ? ?\w+? ?\w+? ? ?\w+?) ?(full-time|reserve|retired|part-time|deceased)$",
            "",
            regex=True,
        )
        .str.replace(r"^(.+)?(pd|so)$", "", regex=True)
        .str.replace(r"\—", "", regex=True)
        .str.replace(r"‘", "", regex=False)
        .str.replace(r"(.+)?(range|safety academy)(.+)?", "", regex=True)
        .str.replace(r"$", "", regex=True)
        .str.replace(r" Â«", " ", regex=True)
        .str.replace(r"~\-", "", regex=True)
        .str.replace(
            r"full\-time  1\/7\/2021 s\/192001",
            "full-time 1/7/2021 8/19/2021",
            regex=True,
        )
        .str.replace(
            r"full-time 1001\/1997 8\/25\/2016 term",
            "full-time 10/1/1997 8/25/2016 term",
            regex=True,
        )
        .str.replace(
            r"full-time 4202018 7\/672018 termination",
            "full-time 4/20/2018 7/6/2018 termination",
            regex=True,
        )
        .str.replace(r"(\w+)  +(\w+)\/", r"\1 \2", regex=True)
        .str.replace(r"\bpb\b", "pd", regex=True)
        .str.replace(
            r"45\/2021 1\/27\/2022 voluntary",
            "4/5/2021 1/27/2022 voluntary",
            regex=True,
        )
        .str.replace(
            r"full-time 6\/172019 77772021", "full-time 6/17/2019 7/7/2021", regex=True
        )
        .str.replace(
            r"full-time 103072016 7\/15\/2018",
            "full-time 10/30/2016 7/15/2018",
            regex=True,
        )
        .str.replace(
            r"full-time 923\/2019 3\/29\/2021",
            "full-time 9/23/2019 3/29/2021",
            regex=True,
        )
        .str.replace(
            r"full-time 1\/26\/2021 s\/1072021",
            "full-time 1/26/2021 8/10/2021",
            regex=True,
        )
        .str.replace(
            r"full-time 1144\/2021 41472022",
            "full-time 11/14/2021 4/14/2022",
            regex=True,
        )
        .str.replace(
            r"full-time \/1672021929\/2021voluntary",
            "full-time 8/16/2021 9/20/2021 voluntary",
            regex=True,
        )
        .str.replace(
            r"full-time 81\/2020 7\/12\/2022",
            "full-time 8/1/2020 7/12/2022",
            regex=True,
        )
        .str.replace(
            r"full-time 16\/2014 7\/31\/2022",
            "full-time 1/6/2014 7/31/2022",
            regex=True,
        )
        .str.replace(
            r"full-time 117202014 473072018",
            "full-time 11/20/2014 4/30/2018",
            regex=True,
        )
        .str.replace(
            r"full-time 4232018 1572022", "full-time 4/23/2018 8/5/2022", regex=False
        )
        .str.replace(
            r"full-time 6292015 11\/9015", "full-time 6/29/2015 11/9/2015", regex=True
        )
        .str.replace(
            r"full-time 12102017 202021", "full-time 12/10/2017 2/20/2021", regex=False
        )
        .str.replace(
            r"full-time 4\/102018 _\/1\/2019",
            "full-time 4/10/2018 1/1/2019",
            regex=True,
        )
        .str.replace(
            r"full-time 4722019 5162021", "full-time 4/2/2019 5/6/2021", regex=True
        )
        .str.replace(
            r"reserve 2\/13\/2018 12\/202018",
            "reserve 2/13/2018 12/20/2018",
            regex=True,
        )
        .str.replace(
            r"full-time 6\/15\/2020 29\/2021",
            "full-time 6/15/2020 1/29/2021",
            regex=True,
        )
        .str.replace(
            r"full-time 2\/4\/2021 4\/6\/2022-",
            "full-time 2/4/2021 4/6/2022 ",
            regex=True,
        )
        .str.replace(r"baton roug!", "baton rouge", regex=True)
        .str.replace(r"(\w+)pd", r"\1 pd", regex=True)
        .str.replace(r"univ pd- tulane", "univ pd-tulane", regex=True)
        .str.replace(
            r"stjohin so full-time 5262016 1272272017",
            "st john so full-time 5/26/2016",
            regex=False,
        )
    )
    return df[~((df.agency == ""))]


def split_agency(df):
    terms = df.agency.str.extract(
        r"( ?termination ?| ?resignation ?| ?involuntary resignation ?| ?volu[mn]tary resignation ?| ?other ?)"
    )
    df.loc[:, "left_reason"] = terms[0].fillna("")

    dates = df.agency.str.extract(
        r"(\w{1,2}\/\w{1,2}\/\w{4}) ?(\w{1,2}\/\w{1,2}\/\w{4})?"
    )
    df.loc[:, "hire_date"] = (
        dates[0]
        .fillna("")
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
        .fillna("")
        .str.replace(r"(.+)(_|\,|&|-)(.+)?", "", regex=True)
        .str.replace(r"^0\/(.+)", "", regex=True)
        .str.replace(r"^7/51/2020", "", regex=True)
        .str.replace(r"(.+)?[a-z](.+)?", "", regex=True)
    )

    bio = df.agency.str.extract(
        r"^(\w+? ?\w+?-? ? ?\w+?-? ?\w+?-? ? ?\w+?) (full-time|reserve|retired|part-time|deceased?|unknown)"
    )
    df.loc[:, "agency"] = bio[0]
    df.loc[:, "employment_status"] = bio[1].str.replace(
        r"^decease$", "deceased", regex=True
    )
    return df[~((df.hire_date.fillna("") == ""))]


def clean_left_reason(df):
    l_reasons = (
        df.left_reason.str.replace(r"volumtary", "voluntary", regex=False)
        .str.replace(r"(\||=|_)", "", regex=True)
        .str.extract(
            r"( ?termination ?| ?involuntary resignation ?| ?voluntary resignation ?| ?resignation ?)"
        )
    )

    df.loc[:, "left_reason"] = l_reasons[0].str.replace(r"^ ", "", regex=True)
    return df


def clean_agency(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
        .str.replace(r"So$", "SO", regex=True)
        .str.replace(r"Pd", "PD", regex=True)
        .str.replace(r"(\w+)SO$", r"\1 SO", regex=True)
        .str.replace(r"(\w+)PD$", r"\1 PD", regex=True)
        .str.replace(r"Stcharles", "St. Charles", regex=True)
        .str.replace(r"^St ?[mM]artin", "St. Martin", regex=True)
        .str.replace(r"^Stfrancisville", "St. Francisville", regex=True)
        .str.replace(r"^Stlandry", "St. Landry", regex=True)
        .str.replace(r"^Stbernard", "St. Bernard", regex=True)
        .str.replace(r"Sttammany", "St. Tammany", regex=True)
        .str.replace(r"Stjohn", "St. John", regex=True)
        .str.replace(r"Stmary", "St. Mary", regex=True)
        .str.replace(r"Slidellpd", "Slidell PD")
        .str.replace(
            r"^Probationparoleadult$", "Probation & Parole - Adult", regex=True
        )
        .str.replace(r"^Deptpublic Safety$", "Department Of Public Safety", regex=True)
        .str.replace(r"^W\b ", "West ", regex=True)
        .str.replace(r"^E\b ", "", regex=True)
        .str.replace(r"^Univ PDxavier", "Xavier University PD", regex=True)
        .str.replace(r"La State Police", "Louisiana State PD", regex=True)
        .str.replace(
            r"^Univ PDlsuhscno$", "LSUHSC - New Orleans University PD", regex=True
        )
        .str.replace(
            r"^Univ PDdelgado Cc$",
            "Delgado Community College University PD",
            regex=True,
        )
        .str.replace(r"Univ PDdillard", "Dillard University PD", regex=True)
        .str.replace(r"^Outstate", "Out of State", regex=True)
        .str.replace(r"^Medical Centerlano$", "Medical Center Of La - No", regex=True)
        .str.replace(r"^Univ PDloyola$", "Loyola University PD", regex=True)
        .str.replace(r"^Univ PDlsu$", "LSU University PD", regex=True)
        .str.replace(r"^Orleans", "New Orleans", regex=True)
        .str.replace(r"\bLsu\b", "LSU", regex=True)
        .str.replace(
            r"Univ PDsouthernno",
            "Southern University PD",
            regex=True,
        )
        .str.replace(r" Par ", "", regex=True)
        .str.replace(r"Alcoholtobacco Control", "Alcohol Tobacco Control", regex=True)
        .str.replace(
            r"^Housing Authorityno$",
            "Housing Authority of New Orleans",
            regex=True,
        )
        .str.replace(r"^Univ PD-?[Tt]ulane$", "Tulane University PD", regex=True)
        .str.replace(r"Univ PDuno", "UNO University PD")
        .str.replace(r"Univ PDla Tech", "Louisiana Tech University PD")
        .str.replace(r"\bLa\b", "Louisiana")
        .str.replace(r"^PlaqueminesSO$", "Plaquemines SO", regex=True)
        .str.replace(r"\bNo\b", "New Orleans PD", regex=True)
        .str.replace(r"^St\. Martin$", "St. Martin SO")
        .str.replace(r"^Tangipahoa$", "Tangipahoa SO", regex=True)
        .str.replace(r"^Lake Charles$", "Lake Charles PD")
        .str.replace(r"^St\. Tammany$", "St. Tammany SO", regex=True)
        .str.replace(r"^Hammond$", "Hammond PD", regex=True)
        .str.replace(r"^New Orleans$", "New Orleans PD")
        .str.replace(r"^Lafayette City$", "Lafayette City PD", regex=True)
        .str.replace(r"^Grand Isle$", "Grand Isle PD", regex=True)
        .str.replace(r"^New Orleans Levee$", "New Orleans Levee PD", regex=True)
        .str.replace(r"^Harbor PD$", "New Orleans Harbor PD", regex=True)
        .str.replace(r"^Lafayette$", "Lafayette PD", regex=True)
        .str.replace(r"^Louisiana State$", "Louisiana State PD", regex=True)
        .str.replace(r"^Caddo Parish$", "Caddo SO", regex=True)
        .str.replace(r"^22Nd", "22nd", regex=True)
        .str.replace(r" Decease$", "", regex=True)
        .str.replace(r" \bParish\b ", " ", regex=True)
        .str.replace(r"^Jefferson Levee PD$", "East Jefferson Levee PD", regex=True)
        .str.replace(r"Time(.+)", "", regex=True)
        .str.replace(r"^St\b ", "St.", regex=True)
        .str.replace(r"(.+)?Range(.+)?", "", regex=True)
        .str.replace(r"P_D$", "PD", regex=True)
        .str.replace(r"^Univ PDnicholls", "Nicholls University PD", regex=True)
        .str.replace(r"^Stjames SO$", "St. James SO", regex=True)
        .str.replace(r"^Wildlifefisheries$", "Wildlife & Fisheries", regex=True)
        .str.replace(r"^State Park(.+)", "", regex=True)
        .str.replace(r"^St\.(\w+)", r"St. \1", regex=True)
        .str.replace(r"^Lastate Police$", "Louisiana State PD", regex=True)
        .str.replace(r"_$", "", regex=True)
        .str.replace(r"^Houma ?[Pp]l?d", "Houma PD", regex=True)
        .str.replace(r"(Lapd(.+)|Sits(.+)|Poncr(.+)|Gional(.+))", "", regex=True)
        .str.replace(r"(\w+) So\b", r"\1 SO", regex=True)
        .str.replace(r"(PD|SO)? ?Unknown$", r"\1", regex=True)
        .str.replace(r"^Pearlriver", "Pearl River", regex=True)
        .str.replace(r"^Officeyouth Dev Deptcorrections$", "", regex=True)
        .str.replace(r"(.+)Reserve(.+)", "", regex=True)
        .str.replace(r"(.+)?Academy(.+)?", "", regex=True)
        .str.replace(r"Univ PDull", "", regex=True)
        .str.replace(r"(\w+)pd$", r"\1 PD", regex=True)
        .str.replace(r"^nd District Attorney$", "", regex=True)
        .str.replace(r"Univ PDsoutheastern", "Southeastern University PD", regex=True)
        .str.replace(r"^Univ PDmcneese$", "Mcneese University PD", regex=True)
        .str.replace(r"^Ladeptjustice$", "Louisiana Department Of Justice", regex=True)
        .str.replace(r"(^Tulane$|^Univ PD Tulane$)", "Tulane University PD", regex=True)
        .str.replace(r" \bUniv\b ", " University ", regex=True)
        .str.replace(r"^Charity Hospital Policeno", "Charity Hospital PD", regex=True)
        .str.replace(r"^Univ PDlsuhsc", "LSUHSC University PD", regex=True)
        .str.replace(r"^Univ PDsouthern$", "Southern University PD", regex=True)
        .str.replace(r"PDd$", "PD", regex=True)
        .str.replace(r"Tafourche SO", "Lafourche SO", regex=True)
        .str.replace(r"^Iefferson SO$", "Jefferson SO", regex=True)
        .str.replace(r"^Tangtpahoa SO$", "Tangipahoa SO", regex=True)
        .str.replace(r"^Dillard$", "Dillard University PD", regex=True)
        .str.replace(r"^Bunice PD", "Eunice PD", regex=True)
        .str.replace(r"^New Orleans Da Office$", "New Orleans DA", regex=True)
        .str.replace(r"Stbernard", r"St Bernard", regex=True)
        .str.replace(r"Univ PD- (\w+)", r"\1 University PD", regex=True)
    )
    return df


def convert_agency_to_slug(df):
    df.loc[:, "agency"] = (
        df.agency.str.lower()
        .str.strip()
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\s+", "-", regex=True)
        .str.replace(r"&", "", regex=True)
        .str.replace(r"\-+", "-", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\'", "", regex=True)
        .str.replace(r"^baton-rouge-so$", "east-baton-rouge-so", regex=True)
        .str.replace(
            r"^univ\-pdbaton\-rouge\-cc$",
            "baton-rouge-community-college-university-pd",
            regex=True,
        )
        .str.replace(r"^retired$", "", regex=True)
        .str.replace(r"jefferson-ist-court", "jefferson-first-court", regex=True)
        .str.replace(
            r"^dept-of-health-hospitals$", "department-of-health-hospitals", regex=True
        )
        .str.replace(r"-jdc-", "-judicial-district-court-", regex=True)
        .str.replace(r"\buniv\b", "university", regex=True)
        .str.replace(r"\b-cc-\b", "-community-college-", regex=True)
        .str.replace(r"\bdept\b", "department", regex=True)
        .str.replace(r"^w\-", "west-", regex=True)
        .str.replace(r"^e\-", "east-", regex=True)
        .str.replace(r"^delhl-pd$", "", regex=True)
        .str.replace(
            r"^(d?-?reserve|pox-la-pd|xc-stoula-pd|mandbville-pd|xavier|neveans-pd)$",
            "",
            regex=True,
        )
        .str.replace(r"^louisiana-tech$", "louisiana-tech-university-pd", regex=True)
        .str.replace(
            r"^(feliciana-so|hannon-pd|pearl-river-pd-deceased|houmap)$", "", regex=True
        )
        .str.replace(
            r"^(houma|river-bridge-pd|oaparish-so|nc-oula-pd)$", "", regex=True
        )
        .str.replace(r"^scort-pd$", "scott-pd", regex=True)
        .str.replace(
            r"^probationparoleadult-fulltime$", "probation-parole-adult", regex=True
        )
        .str.replace(r"^houma-pl$", "houma-pd", regex=True)
        .str.replace(r"^ponciatoula-pd$", "ponchatoula-pd", regex=True)
        .str.replace(r"^jeeferson-so$", "jefferson-so", regex=True)
        .str.replace(r"^loyola$", "loyola-university-pd", regex=True)
        .str.replace(r"^new-orleans-da$", "orleans-da", regex=True)
        .str.replace(r"^new-orleans-levee-pd$", "orleans-levee-pd", regex=True)
        .str.replace(
            r"^new-orleans-coroners-office$", "orleans-coroners-office", regex=True
        )
        .str.replace(r"^new-orleans-civil-so$", "orleans-civil-so", regex=True)
        .str.replace(
            r"^new-orleans-constable$", "new-orleans-constables-office", regex=True
        )
        .str.replace(r"^pearl-river-pd-deceased$", "pearl-river-pd", regex=True)
        .str.replace(r"^xavier$", "xavier-university-pd", regex=True)
        .str.replace(r"^i$", "", regex=True)
        .str.replace(r"^acadia\-o$", "acadia-so", regex=True)
        .str.replace(r"^agricultureforestry$", "agriculture-forestry", regex=True)
        .str.replace(r"broussard-fd$", "broussard-fire-department", regex=True)
        .str.replace(r"^my-shal$", "", regex=True)
        .str.replace(r"^gretnapld$", "gretna-pd", regex=True)
        .str.replace(r"^sthelena-so$", "st-helena-so", regex=True)
        .str.replace(r"^ng-oaparish-so$", "", regex=True)
        .str.replace(r"^vermilion-s?o$", "", regex=True)
        .str.replace(r"^(\w+)-district-attorney", r"\1-da", regex=True)
        .str.replace(r"medical-centerla-new-orleans-pd", "medical-center-of-louisiana-new-orleans-pd", regex=False)
        .str.replace(r"hama-pd", "", regex=False)
        .str.replace(r"university-pd-lsu", "lsu-university-pd", regex=False)
        .str.replace(r"^lsuhsc-university-pd-new-orleans-pd$", "lsuhsc-new-orleans-university-pd", regex=True)
        .str.replace(r"city-park-pd-new-orleans-pd", "new-orleans-city-park-pd", regex=False)
        .str.replace(r"university-pdbaton-rouge", "", regex=False)
        .str.replace(r"lasupreme-court", "louisiana-supreme-court", regex=False)
        .str.replace(r"crowley-pp", "crowley-pd", regex=False)
        .str.replace(r"university-pdnsu", "", regex=False)
        .str.replace(r"^new-orleans-p$", "new-orleans-pd", regex=True)
        .str.replace(r"lastate-museum-pd", "louisiana-state-museum-pd", regex=False)
        .str.replace(r"louisiana-state-museum-fd", "louisiana-state-museum-pd", regex=False)
        .str.replace(r"^university-pdlsusc-new-orleans-pd$", "lsuhsc-new-orleans-university-pd", regex=True)
        .str.replace(r"university-pddelgado-ce", "delgado-community-college-university-pd", regex=False)
        .str.replace(r"officeinspector-general", "louisiana-office-of-inspector-general", regex=False)
        .str.replace(r"university-pdsouthernbr", "southern-br-university-pd", regex=False)
        .str.replace(r"university-pdlsushreveport", "lsuhsc-shreveport-university-pd", regex=False)
        .str.replace(r"university-pdcentenary", "", regex=False)
        .str.replace(r"bossier-cc", "", regex=False)
        .str.replace(r"new-orleans-criminal-court", "orleans-criminal-court", regex=False)
        .str.replace(r"shreveport-city-marshal", "shreveport-city-marshall", regex=False)
        .str.replace(r"juvenile-servicesbr", "juvenile-services-br", regex=False)
        .str.replace(r"new-orleans-po", "", regex=False)
        .str.replace(r"stjoin-so", "st-john-so", regex=False)
        .str.replace(r"outstare", "out-of-state", regex=False)
        .str.replace(r"tepferson-so", "", regex=False)
        .str.replace(r"jbfferson-o", "jefferson-so", regex=False)
        .str.replace(r"washington-paristi-so", "washington-so", regex=False)
        .str.replace(r"st-martinso", "st-martin-so", regex=False)
        .str.replace(r"^tangipaho-s-so$", "tangipahoa-so", regex=True)
    )
    return df[~((df.agency.fillna("") == ""))]


def drop_duplicates(df):
    return df.drop_duplicates(subset="uid", keep="first")


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


def clean():
    dfa = pd.read_csv(deba.data("ner/advocate_post_officer_history_reports.csv"))
    dfb = pd.read_csv(deba.data("ner/post_officer_history_reports.csv"))
    dfc = pd.read_csv(deba.data("ner/post_officer_history_reports_9_16_2022.csv"))
    df = (
        pd.concat([dfa, dfb, dfc], axis=0, ignore_index=True)
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
        .pipe(clean_left_reason)
        .pipe(
            names_to_title_case,
            [
                "agency",
            ],
        )
        .pipe(clean_agency)
        .pipe(convert_agency_to_slug)
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency"])
        .pipe(drop_duplicates)
        .pipe(check_for_duplicate_uids)
        .pipe(switched_job)
        .pipe(set_values, {"source_agency": "post"})
        .pipe(standardize_desc_cols, ["agency"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)
