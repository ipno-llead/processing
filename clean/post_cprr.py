import pandas as pd
import deba
from lib.columns import clean_column_names
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
    return df.drop(columns=["name", "suffix"])


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.reason.str.replace(r"^(\w+)-(\w+)", r"\1 \2", regex=True)
        .str.replace(r"\/", "|", regex=True)
        .str.replace("-", ": ", regex=False)
    )
    return df.drop(columns="reason")


def assign_action(df):
    df.loc[:, "action"] = "decertified"
    return df


def clean_agency(df):
    df.loc[:, "agency"] = (df.agency
                           .str.lower()
                           .str.strip()
                           .str.replace(r"district attorney", "da", regex=False)
                           .str.replace(r"(\w+)\/ ?(\w+)", "", regex=True)
                           .str.replace(r"jda$", "da", regex=True)
                           .str.replace(r"alexandria marshal", "alexandria city marshal", regex=False)
                           .str.replace(r"parish ", "", regex=False)
                           .str.replace(r"prob&parole", "probation parole", regex=False)
                           .str.replace(r"univ\. pd southern\- shreveport", "southern shreveport university pd", regex=True)
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
                           
    )
    return df[~((df.agency.fillna("") == ""))]


def clean():
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


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_post_2016_2019.csv"), index=False)
