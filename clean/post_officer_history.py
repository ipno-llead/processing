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
            .str.replace(r" (p[bop]?)$", " pd", regex=True)
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
            .str.replace(r"^orleans-so$", "new-orleans-so", regex=True)
            .str.replace(r"^lsuhscno", "lsuhsc new orleans", regex=True)
            .str.replace(r"orleanspd$", "orleans pd", regex=True)
            .str.replace(r"new orleans criminal court", "orleans criminal court", regex=False)
            .str.replace(r"orleans da office", "orleans da", regex=False)
            .str.replace(r"st bernard pd", "st bernard so", regex=False)
            .str.replace(r"lsuh?sc university pd no", "lsuhsc new orleans university pd")
            .str.replace(r"fd$", "pd", regex=True)
            .str.replace(r"tangipahioa so", "tangipahoa so")
            .str.replace(r"^orleans so$", "new-orleans-so", regex=True)
            .str.replace(r"\s+", r"-", regex=True)
    )
    return df[~((df.agency.fillna("") ==  ""))]


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
