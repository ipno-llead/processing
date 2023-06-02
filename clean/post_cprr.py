import pandas as pd
import deba
from lib.columns import clean_column_names
from lib.clean import standardize_desc_cols, clean_dates
from lib.uid import gen_uid


def split_name(df):
    col_name = [col for col in df.columns if col.endswith("name")][0]
    names = (
        df[col_name]
        .str.strip()
        .str.lower()
        .str.replace("van tran", "vantran", regex=False)
        .str.replace("de' clouet", "de'clouet", regex=False)
        .str.replace(r"(\w+) \b(\w{2})$", r"\2 \1", regex=True)
        .str.replace(r"\"", "", regex=True)
        .str.extract(r"^(\w+) ?(\w{3,})? ?(jr|sr)? (\w+-?\'?\w+)$")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "middle_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")
    df.loc[:, "last_name"] = names[3]
    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["name", "suffix"])[~(df.last_name.fillna("") == "")]


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.reason.str.replace(r"^(\w+)-(\w+)", r"\1 \2", regex=True)
        .str.replace(r"\/", "|", regex=True)
        .str.replace("-", ": ", regex=False)
        .str.replace(r"^for cause$", "for cause: criminal conviction", regex=True)
        .str.replace(
            r"^(\w+) in: service deficiency$", "in service deficiency", regex=True
        )
    )
    return df.drop(columns="reason")


def assign_action(df):
    df.loc[:, "action"] = "decertified"
    return df


def clean_agency(df):
    df.loc[:, "agency"] = (
        df.agency.str.lower()
        .str.strip()
        .str.replace(r"district attorney", "da", regex=False)
        .str.replace(r"(\w+)\/ ?(\w+)", "", regex=True)
        .str.replace(r"jda$", "da", regex=True)
        .str.replace(r"alexandria marshal", "alexandria city marshal", regex=False)
        .str.replace(r"parish ", "", regex=False)
        .str.replace(r"prob&parole", "probation parole", regex=False)
        .str.replace(
            r"univ\. pd southern\- shreveport",
            "southern shreveport university pd",
            regex=True,
        )
        .str.replace(r"^ebrso$", "east baton rouge so", regex=True)
        .str.replace(r"^e ", "east ", regex=True)
        .str.replace(r"^lsp$", "louisiana state pd", regex=True)
        .str.replace(r"^opelousas marshal$", "opelousas city marshal", regex=True)
        .str.replace(r"^nopd$", "new orleans pd", regex=True)
        .str.replace(r"la ag\'s office", "attorney generals office", regex=True)
        .str.replace(r"^broussard$", "broussard pd", regex=True)
        .str.replace(r"^jp constable$", "jefferson constable", regex=True)
        .str.replace(r"^opelousas  landry$", "", regex=True)
        .str.replace(r"^st bernard$", "st bernard so", regex=True)
        .str.replace(r"^st martin$", "st martin so", regex=True)
        .str.replace(r"^univ. lsu-a$", "", regex=True)
        .str.replace(r"^orleans so$", "new orleans so", regex=True)
        .str.replace(r"^wbrso$", "west baton rouge so", regex=True)
        .str.replace(r"livington\b", "livingston", regex=True)
        .str.replace(r"coushatte", "coushatta", regex=False)
        .str.replace(r"^hammond marshal$", "hammond city marshal", regex=True)
        .str.replace(r"\s+", "-", regex=True)
        .str.replace(r"^cheneyville-$", "cheneyville-pd", regex=True)
        .str.replace(r"p\.d\.$", "pd", regex=True)
        .str.replace(r"-police-department$", "-pd", regex=True)
        .str.replace(r"-sheriff\’s-office$", "-so", regex=True)
        .str.replace(r"^orleans-so$", "new-orleans-so", regex=True)
        .str.replace(r"^camerson-so$", "cameron-so", regex=True)
        .str.replace(r"^e\.-baton", "east-baton", regex=True)
        .str.replace(r"^w-carroll", "west-carroll", regex=True)
        .str.replace(r"^la-state-police", "louisiana-state-pd", regex=True)
        .str.replace(r"-par-", "-", regex=False)
        .str.replace(r"^railroad(.+)", "", regex=True)
        .str.replace(r"^univ.-pd---(\w+)", r"\1-university-pd", regex=True)
        .str.replace(
            r"probation-&-parole---adult", "probation-parole-adult", regex=True
        )
        .str.replace(
            r"^office-of-juvenile-justice-central-office$",
            "office-of-juvenile-justice",
            regex=True,
        )
        .str.replace(
            r"^(la-lottery-corp|louisiana-lottery-corporation-headquarters"
            r"|probation-parole|desheriff’s-officeto-so)$",
            "louisiana-lottery-corp",
            regex=True,
        )
        .str.replace(
            r"^univ.-police-department---sheriff\’s-officeuthe(.+)", "", regex=True
        )
        .str.replace(r"fort-polk-mp", "fort-polk-military-pd", regex=False)
        .str.replace(
            r"alcohol-&-tobacco-control", "alcohol-tobacco-control", regex=False
        )
        .str.replace(r"-jdc", "-judicial-district-court", regex=False)
        .str.replace(r"supreme-court-of-la", "louisiana-supreme-court", regex=False)
        .str.replace(
            r"orleans-constable-1st-city-court", "orleans-constable", regex=False
        )
        .str.replace(
            r"^(6th-district-da-office|6th-district-da-office)$",
            "6th-judicial-district-court",
            regex=True,
        )
    )
    return df[~((df.agency.fillna("") == ""))]


def filter_agencies(df):
    agencies = pd.read_csv(deba.data("clean/agency_reference_list.csv"))
    agencies = agencies.agency_slug.tolist()
    df = df[df.agency.isin(agencies)]
    return df


def assign_date(df):
    df.loc[
        df.reason.fillna("").str.contains("2021"), "decertification_date"
    ] = "12/31/2021"
    df.loc[
        df.reason.fillna("").str.contains("2020"), "decertification_date"
    ] = "12/31/2020"
    df.loc[:, "decertification_date"] = df.decertification_date.fillna("")
    return df[~((df.decertification_date == ""))]


def clean23():
    df = (
        pd.read_csv(
            deba.data("raw/post_council/post_decertifications_4_18_2023.csv"),
            encoding="cp1252",
        )
        .pipe(clean_column_names)
        .rename(columns={"date": "decertification_date"})
        .pipe(clean_agency)
        .pipe(filter_agencies)
        .pipe(assign_date)
        .pipe(clean_allegations)
        .pipe(assign_action)
        .pipe(standardize_desc_cols, ["agency"])
        .pipe(split_name)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "decertification_date"], "allegation_uid")
    )
    return df


def clean20():
    df = (
        pd.read_csv(deba.data("raw/post_council/post_decertifications_2016_2019.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "decertification_date"})
        .pipe(split_name)
        .pipe(clean_allegations)
        .pipe(assign_action)
        .pipe(clean_agency)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "decertification_date"], "allegation_uid")
    )
    return df


def concat_dfs(dfa, dfb):
    df = pd.concat([dfa, dfb], axis=0)
    return df 

if __name__ == "__main__":
    df = clean20()
    df23 = clean23()
    df = concat_dfs(df, df23)
    df.to_csv(deba.data("clean/cprr_post_2016_2019.csv"), index=False)
    df23.to_csv(
        deba.data("clean/cprr_post_decertifications_4_18_2023.csv"), index=False
    )
    df.to_csv(deba.data("clean/cprr_post_decertifications_2016_2023.csv"), index=False)
