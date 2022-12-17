import deba
import pandas as pd
from lib.uid import gen_uid
from lib.clean import names_to_title_case, clean_sexes, standardize_desc_cols, clean_dates
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
        r"(term(\w+)|(\w+) resi(\w+)|other|deceased)"
        # r"(termination| ?resignation ?| ?involuntary resignation ?| ?volu[mn]tary resignation ?| ?other ?)"
    )
    df.loc[:, "left_reason"] = terms[0].fillna("").str.replace(r"(\w+) $", r"\1", regex=True)

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
        .str.replace(r"(\w+)\/(\w+)\/(\w+)", "", regex=True).str.replace(r"(\“|\(|\")", "", regex=True)
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


def clean_agency_1(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
            .str.replace(r"(\/|\)|\||\\)", "", regex=True)
            .str.replace(r"^(part|full).+", "", regex=True)
            .str.replace(r"-", "", regex=False)
            .str.replace(r"unknown", "", regex=False)
            .str.replace(r" ([pf]ull|part).+", "", regex=True)
            .str.replace(r"(p[pd]|nsu|so|police|eastern) (.+)", r"pd", regex=True)
            .str.replace(r"^(e\b|bast)", "east", regex=True)
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
            .str.replace(r" (p[bop]?|80)$", " pd", regex=True)
            .str.replace(r"univ pd(\w+)", r"\1 univ pd", regex=True)
            .str.replace(r"(\w+)cc\b", "community college", regex=False)
            .str.replace(r"servicesbr", "services bureau", regex=False)
            .str.replace(r"^city park pdno$", "new orleans city park pd", regex=True)
            .str.replace(r"^medicalcenterla ?no$", "medical-center-of-louisiana-new-orleans-pd", regex=True)
            .str.replace(r"^alcoholtobacco", "alcohol tobacco", regex=True)
            .str.replace(r"^southernno", "southern no", regex=True)
            .str.replace(r"^st(\w+)", r"st \1", regex=True)
            .str.replace(r"authorityno$", "authority of new orleans", regex=True)
            .str.replace(r"deptpublic safety", "department of public safety", regex=True)
            .str.replace(r"medicalcenter", "medical center", regex=True)
            .str.replace(r"^southpd univ pd$", "", regex=True)
            .str.replace(r"^st ate", "state", regex=True)
            .str.replace(r"baton univ pd rougecc$", "baton rouge community college university pd", regex=True)
            .str.replace(r"^probationparoleadult$", "probation parole adult", regex=True)
            .str.replace(r"22nd district attorney", "22nd da", regex=False)
            .str.replace(r"wildlifefisheries", "wildlife fisheries", regex=False)
            .str.replace(r"officeinspector general", "office of inspector general", regex=False)
            .str.replace(r"^lsushreveport", "lsu shreveport", regex=True)
            .str.replace(r"^southernbr", "southern br", regex=True)
            .str.replace(r"west baton rouge so", "", regex=True)
            .str.replace(r"^pd univ pd$", "", regex=True)
            .str.replace(r"lasupreme court", "la supreme court", regex=False)
            .str.replace(r"(\w+)cc\b", r"\1 community college", regex=True)
            .str.replace(r"univ\b", "university", regex=True)
            .str.replace(r"^lastate", "la state", regex=True)
            .str.replace(r"^la\b", "louisiana", regex=True)
            .str.replace(r"^lsuhscno", "lsuhsc new orleans", regex=True)
            .str.replace(r"orleanspd$", "orleans pd", regex=True)
            .str.replace(r"new orleans criminal court", "orleans criminal court", regex=False)
            .str.replace(r"orleans da office", "orleans da", regex=False)
            .str.replace(r"st bernard pd", "st bernard so", regex=False)
            .str.replace(r"lsuh?sc university pd no", "lsuhsc new orleans university pd")
            .str.replace(r"fd$", "pd", regex=True)
            .str.replace(r"(tang(\w+)|tano(\w+)) so", "tangipahoa so", regex=True)
            .str.replace(r"^orleans so$", "new-orleans-so", regex=True)
            .str.replace(r"(\w+)pd$", r"\1 pd", regex=True)
            .str.replace(r"(\w+)so", r"\1 so", regex=True)
            .str.replace(r"lapayette", "lafayette", regex=False)
            .str.replace(r"vidaliap", "vidalia pd", regex=False)
            .str.replace(r"greina", "gretna", regex=False)
            .str.replace(r"tberia s[oq]", "iberia so", regex=True)
            .str.replace(r"bossibr ciry pd", "bossier city pd", regex=False)
            .str.replace(r"sofulltime", "so", regex=False)
            .str.replace(r"b baton", "baton", regex=False)
            .str.replace(r"^(take)?\b ?providence pd$", "lake providence pd", regex=True)
            .str.replace(r"orlbans", "orleans", regex=False)
            .str.replace(r"(jebferson|jerferson|je[fbr]fer son)", "jefferson", regex=True)
            .str.replace(r"charity hospital pdno", "charity hospital pd", regex=False)
            .str.replace(r"calcasibu", "calcasieu", regex=False)
            .str.replace(r"scortd", "scott pd", regex=False)
            .str.replace(r"departmentcorrections", "department of corrections", regex=False)
            .str.replace(r"kenner\?", "kenner pd", regex=True)
            .str.replace(r"marshat", "marshal", regex=False)
            .str.replace(r"chathamed", "chatham pd", regex=False)
            .str.replace(r"madi son detentioncenter", "madison detention center", regex=False)
            .str.replace(r"university university pd pdbossier community college", 
                         "bossier community college university pd", regex=False)
            .str.replace(r"0 ?(\w+)?$", "so", regex=True)
            .str.replace(r"\bd$", "pd", regex=True)
            
    )
    return df



def clean_agency_2(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
            .str.replace(r"hamm(.+)", "hammond pd", regex=True)
            .str.replace(r"mande ville", "mandeville", regex=False)
            .str.replace(r"madi sonville", "madisonville", regex=False)
            .str.replace(r"officejuvenile", "office of juvenile", regex=False)
            .str.replace(r"agricultureforestry", "agriculture forestry", regex=False)
            .str.replace(r"^(patrol|vans|lltim)\b(.+)", "", regex=True)
            .str.replace(r"parksbayou", "parks bayou", regex=False)
            .str.replace(r"east bn(.+)", "east baton rouge so", regex=True)
            .str.replace(r"^(generals office|(la|a|no) pd)$", "", regex=True)
            .str.replace(r"^ponc(.+)", "ponchatoula pd")
            .str.replace(r"fol som", "folsom", regex=False)
            .str.replace(r"(\w+) son\b", r"\1son", regex=True)
            .str.replace(r"st erlington", "sterlington", regex=False)
            .str.replace(r"correctionscenter", "corrections center", regex=False)
            .str.replace(r"deptjustice", "department of justice", regex=False)
            .str.replace(r"^city park pd$", "new orleans city park pd", regex=True)
            .str.replace(r"^orleans pd$", "new orleans pd", regex=True)
            .str.replace(r"pd no", "pd", regex=False)
            .str.replace(r"jefferson ist\b", "jefferson first", regex=True)
            .str.replace(r"wcarroll", "west carroll", regex=False)
            .str.replace(r"rapids so", "rapides so", regex=False)
            .str.replace(r"^baton rouge so$", "east baton rouge so", regex=True)
            .str.replace(r"^minden$", "minden pd", regex=True)
            .str.replace(r"^holy university pd cross$", "holy cross university pd", regex=True)
            .str.replace(r"^west baton rouge pd$", "west baton rouge so", regex=True)
            .str.replace(r"(st charles pd|(.+)time|bossier criminal justice institute)", "", regex=True)
            .str.replace(r"st tammany pd", "st tammany so", regex=False)
            .str.replace(r"parksgrand", "parks grand", regex=False)
            .str.replace(r"harri sonburg", "harrisonburg", regex=False)
            .str.replace(r"st helena pd", "st helena so", regex=False)
            .str.replace(r"(.+)262(.+)", "", regex=True)
            .str.replace(r"(.+) \bla pd$", "", regex=True)
            .str.replace(r"(\w+) $", r"\1", regex=True)
            .str.replace(r"\s+", "-", regex=True)       
    )
    return df[~((df.agency.fillna("") ==""))]

def clean_parsed_dates(df):
    df.loc[:, "hire_date"] = df.hire_date.str.replace(r"21\/2001", "2/1/2001", regex=True).str.replace(r"^(\w{1,4})\/(\w{4})$", "", regex=True).str.replace(r"(\w{3})\/(\w{3})", "", regex=True).str.replace(r"(\w+)\/(\w+)\/(\w)(\w{4})", r"\1/\2/\4", regex=True).str.replace(r"3\/17\/01410", "", regex=True)

    df.loc[:, "left_date"] = (df.left_date.str.replace(r"924\/2020", "9/24/2020", regex=True)
    .str.replace(r"(\w)[12](\w{1})\/(\w{4})", r"\1/1\2/\3", regex=True).str.replace(r"(\w{2})(\w{2})\/(\w{4})", r"\1/\2/\3", regex=True)
    .str.replace(r"^(\w)(\w)\/(\w{4})$", r"\1/\2/\3", regex=True).str.replace(r"^(\w{1,4})\/(\w{4})$", "", regex=True)
    .str.replace(r"_\/1\/2019", "", regex=True).str.replace(r"\/72019", "/2019", regex=True).str.replace(r"(\w{3})\/(\w{3})", "", regex=True).str.replace(r"(\w+)\/(\w+)\/(\w)(\w{4})", r"\1/\2/\4", regex=True).str.replace(r"3\/17\/01410", "", regex=True)
    )
    return df 

def clean_employment_status(df):
    df.loc[:, "employment_status"] = (df.employment_status.str.replace(r" (\w+)", r"\1", regex=True).str.replace(r"(\w+)time", r"\1-time", regex=True)
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
        .pipe(clean_agency_1)
        .pipe(clean_agency_2)
        .pipe(clean_parsed_dates)
        .pipe(clean_employment_status)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
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
