import deba
import pandas as pd
from lib.uid import gen_uid
from lib.clean import names_to_title_case, clean_sexes, standardize_desc_cols
from lib.columns import set_values
import sympy as sym



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
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
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
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) \_ (\w+)", r"\1 \2", regex=True)
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
        .str.replace(r"(\w+)  +(\w+)\/", r"\1 \2", regex=True)
        .str.replace(r"-", "", regex=False)
    )
    return df[~((df.agency == ""))]


def split_agency(df):
    terms = df.agency.str.lower().str.strip().str.extract(
        r"(termination| ?resignation ?| ?involuntary resignation ?| ?volu[mn]tary resignation ?| ?other ?)"
    )
    df.loc[:, "left_reason"] = terms[0].fillna("")

    dates = df.agency.str.extract(
        r"(\w+\/\w+\/?\w+) ?(\w+\/\w+\/?\w+)?"
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

    agency = df.agency.str.extract(r"(.+) (\w+)\/(\w+)?")
    df.loc[:, "agency"] = (agency[0].str.lower().replace(r"(\w+)-(\w+)?.+", "", regex=True)
        .str.replace(r"( ?reserve| ?deceased| ?retired)", "", regex=True)
        .str.replace(r"(\w+)\/(\w+)\/(\w+)", "", regex=True)
    )

    emp_status = df.agency.str.lower().str.extract(
        r"( ?reserve ?| ?full-?time ?| ?part-?time ?| ?deceased ?| ?retired ?)"
    )
    df.loc[:, "employment_status"] = emp_status[0].str.replace(
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
            .str.replace(r"(\/|\)|\||\\)", "", regex=True)
            .str.replace(r"^(part|full).+", "", regex=True)
            .str.replace(r"-", "", regex=False)
            .str.replace(r"unknown", "", regex=False)
            .str.replace(r" ([pf]ull|part).+", "", regex=True)
            .str.replace(r"(p[pd]|nsu|so|police|eastern) (.+)", r"pd", regex=True)
            .str.replace(r"^e\b", "east", regex=True)
            .str.replace(r"^w\b", "west", regex=True)
            .str.replace(r"(\w+) c[ec]", r"\1cc", regex=True)
            .str.replace(r" pari(sti|sh)", "", regex=True)
            .str.replace(r"stmartinso", "st martin so", regex=False)
            .str.replace(r"sttamimany", "sttammany", regex=False)
            .str.replace(r"join", "john", regex=False)
            .str.replace(r"outsta[tr]e", "out of state")
            .str.replace(r"jbfferson", "jefferson", regex=False)
            .str.replace(r"bossiercc", "univ pdbossiercc")
            .str.replace(r"ccnter", "center", regex=False)
            .str.replace(r"correctionalcenter", "correctional center", regex=False)
            .str.replace(r"(.+)?(academy|fire)(.+)?", "", regex=True)
            .str.replace(r" o$", " so", regex=True)
            .str.replace(r"police", "pd", regex=False)
            .str.replace(r"^(univ pd)$", "", regex=True)
            .str.replace(r" (pb|po)$", " pd", regex=True)
            .str.replace(r"univ pd(\w+)", r"\1 univ pd", regex=True)
            .str.replace(r"(\w+)cc\b", "community college", regex=False)
            .str.replace(r"servicesbr", "services bureau", regex=False)
            # .str.replace(r"la ?state police", "la state pd", regex=True)
            # .str.replace(r"pb", "pd", regex=False)
            # .str.replace(r"madi-sonville", "madisonville", regex=False)
            # .str.replace(r"fol-som", "folsom", regex=False)
            # .str.replace(r"jeffer-son", "jefferson", regex=False)
            # .str.replace(r"du-son", "duson", regex=False)
            # .str.replace(r"madi-son", "madison", regex=False)
            # .str.replace(r"dod\-son", "dodson", regex=True)
            # .str.replace(r"jack-son", "jackson", regex=False)
            # .str.replace(r"w\b", "west", regex=True)
            # .str.replace(r"e\b", "east", regex=True)
            # .str.replace(r"^st(\w+)", r"st-\1", regex=True)
            # .str.replace(r"east-(so|pd)", r"e-\1", regex=True)
            # .str.replace(r"st-ateparks", "state-parks", regex=False)
            # .str.replace(r" [pf]ull.+", "", regex=True)
            # .str.replace(r"orleans-so", "new-orleans-so", regex=False)
            # .str.replace(r"(\w)\.(\w)\.", r"\1\2", regex=True)
            # .str.replace(r" parish ", " ", regex=False)
            # .str.replace(r"unknown", "", regex=True)\
            # .str.replace(r"housing-authorityno", "housing-authority-of-new-orleans", regex=False)
            # .str.replace(r" (\w+)(200[56])", "", regex=True)
            # .str.replace(r"(\w+)so", r"\1-so", regex=True)
            # .str.replace(r"univ pd(\w+)", r"\1-university-pd", regex=True)
            # .str.replace(r"(\w+) (\w+)", r"\1-\2", regex=True)
            
    )
    return df[~((df.agency.fillna("") ==  ""))]


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
        .str.replace(
            r"medical-centerla-new-orleans-pd",
            "medical-center-of-louisiana-new-orleans-pd",
            regex=False,
        )
        .str.replace(r"hama-pd", "", regex=False)
        .str.replace(r"university-pd-lsu", "lsu-university-pd", regex=False)
        .str.replace(
            r"^lsuhsc-university-pd-new-orleans-pd$",
            "lsuhsc-new-orleans-university-pd",
            regex=True,
        )
        .str.replace(
            r"city-park-pd-new-orleans-pd", "new-orleans-city-park-pd", regex=False
        )
        .str.replace(r"university-pdbaton-rouge", "", regex=False)
        .str.replace(r"lasupreme-court", "louisiana-supreme-court", regex=False)
        .str.replace(r"crowley-pp", "crowley-pd", regex=False)
        .str.replace(r"university-pdnsu", "", regex=False)
        .str.replace(r"^new-orleans-p$", "new-orleans-pd", regex=True)
        .str.replace(r"lastate-museum-pd", "louisiana-state-museum-pd", regex=False)
        .str.replace(
            r"louisiana-state-museum-fd", "louisiana-state-museum-pd", regex=False
        )
        .str.replace(
            r"^university-pdlsusc-new-orleans-pd$",
            "lsuhsc-new-orleans-university-pd",
            regex=True,
        )
        .str.replace(
            r"university-pddelgado-ce",
            "delgado-community-college-university-pd",
            regex=False,
        )
        .str.replace(
            r"officeinspector-general",
            "louisiana-office-of-inspector-general",
            regex=False,
        )
        .str.replace(
            r"university-pdsouthernbr", "southern-br-university-pd", regex=False
        )
        .str.replace(
            r"university-pdlsushreveport",
            "lsuhsc-shreveport-university-pd",
            regex=False,
        )
        .str.replace(r"university-pdcentenary", "", regex=False)
        .str.replace(r"bossier-cc", "", regex=False)
        .str.replace(
            r"new-orleans-criminal-court", "orleans-criminal-court", regex=False
        )
        .str.replace(
            r"shreveport-city-marshal", "shreveport-city-marshall", regex=False
        )
        .str.replace(r"juvenile-servicesbr", "juvenile-services-br", regex=False)
        .str.replace(r"new-orleans-po", "", regex=False)
        .str.replace(r"stjoin-so", "st-john-so", regex=False)
        .str.replace(r"outstare", "out-of-state", regex=False)
        .str.replace(r"tepferson-so", "", regex=False)
        .str.replace(r"j[eb]fferson-o", "jefferson-so", regex=True)
        .str.replace(r"washington-paristi-so", "washington-so", regex=False)
        .str.replace(r"st-martinso", "st-martin-so", regex=False)
        .str.replace(r"^tangipaho-s-so$", "tangipahoa-so", regex=True)
        .str.replace(r"1-nc-oula-pd", "", regex=False)
        .str.replace(r"atio-generals-office", "attorney-generals-office", regex=False)
        .str.replace(r"bossier-parishso", "bossier-so", regex=False)
        .str.replace(
            r"lsu-university-pdhscno", "lsuhsc-new-orleans-university-pd", regex=False
        )
        .str.replace(r"p-d$", "pd", regex=True)
        .str.replace(r"ponc-louisiana-pd", "", regex=False)
        .str.replace(r"pox-louisiana-pd", "", regex=False)
        .str.replace(r"university-pd-uno", "uno-university-pd", regex=False)
        .str.replace(
            r"shreveport-city-marshall", "shreveport-city-marshal", regex=False
        )
        .str.replace(r"^jefferson$", "jefferson-so", regex=True)
        .str.replace(r"^sulphur-city-marshall$", "sulphur-city-marshal", regex=True)
        .str.replace(r"^natchitoches$", "natchitoches-so", regex=True)
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
    dfd = pd.read_csv(deba.data("ner/post_officer_history_reports_9_30_2022.csv"))
    df = (
        pd.concat([dfa, dfb, dfc, dfd], axis=0, ignore_index=True)
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
        .pipe(clean_agency)
        # .pipe(convert_agency_to_slug)
        # .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency"])
        # .pipe(drop_duplicates)
        # .pipe(check_for_duplicate_uids)
        # .pipe(switched_job)
        # .pipe(set_values, {"source_agency": "post"})
        # .pipe(standardize_desc_cols, ["agency"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)
