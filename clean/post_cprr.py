import pandas as pd
import deba
from lib.columns import clean_column_names
from lib.clean import standardize_desc_cols, clean_dates, clean_names
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
        .pipe(clean_agency)
        .pipe(clean_allegations)
        .pipe(assign_action)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "decertification_date"], "allegation_uid")
    )
    return df

def split_name_24(df: pd.DataFrame) -> pd.DataFrame:
    def split_parts(full_name):
        if pd.isna(full_name) or str(full_name).strip() == "":
            return pd.Series(["", "", ""])
        parts = full_name.strip().split()
        if len(parts) == 1:
            return pd.Series([parts[0], "", ""])
        elif len(parts) == 2:
            return pd.Series([parts[0], "", parts[1]])
        else:
            return pd.Series([parts[0], " ".join(parts[1:-1]), parts[-1]])
    
    df[['first_name', 'middle_name', 'last_name']] = df['name'].apply(split_parts)
    df = df.drop(columns=['name'])
    return df


def clean_agency_24(df: pd.DataFrame,
                 src_col: str = "agency",
                 out_col: str = "agency_slug",
                 drop_empty: bool = True) -> pd.DataFrame:
    s = (
        df[src_col]
        .astype(str)
        .str.strip()
        .str.lower()

        # unify punctuation / spacing
        .str.replace("\u2019", "'", regex=False)         # curly apostrophe → straight
        .str.replace(r"[()]", "", regex=True)
        .str.replace(r"\s*&\s*", " & ", regex=True)      # keep & before later mapping
        .str.replace(r"\s+", " ", regex=True)            # collapse spaces

        # quick targeted fixes seen in your uniques
        .str.replace(r"\blivington\b", "livingston", regex=True)
        .str.replace(r"\bcamerson\b", "cameron", regex=True)
        .str.replace(r"\bcoushatte\b", "coushatta", regex=True)
        .str.replace(r"\bmarshal's office\b", "marshal", regex=True)

        # standardize agencies before hyphenation
        .str.replace(r"district attorney", "da", regex=False)
        .str.replace(r"\bjda\b$", "da", regex=True)
        .str.replace(r"^e\s+", "east ", regex=True)
        .str.replace(r"^ebrso$", "east baton rouge so", regex=True)
        .str.replace(r"^wbrso$", "west baton rouge so", regex=True)
        .str.replace(r"^lsp$", "louisiana state pd", regex=True)
        .str.replace(r"^nopd$", "new orleans pd", regex=True)
        .str.replace(r"la ag's office", "attorney generals office", regex=False)
        .str.replace(r"^jp constable$", "jefferson constable", regex=True)
        .str.replace(r"\bprob&parole\b", "probation parole", regex=True)
        .str.replace(r"alexandria marshal", "alexandria city marshal", regex=False)
        .str.replace(r"\bparish ", "", regex=True)       # remove literal "parish " prefix
        .str.replace(r"^orleans so$", "new orleans so", regex=True)

        # slash combos like "Cheneyville PD/ Ball PD" → drop the second agency
        .str.replace(r"(\w+)\/ ?(\w+.*)$", r"\1", regex=True)

        # final formatting to slug
        .str.replace(r"\s*&\s*", "-", regex=True)        # "&" to hyphen
        .str.replace(r"\s+", "-", regex=True)            # spaces → hyphen
        .str.replace(r"p\.d\.$", "pd", regex=True)
        .str.replace(r"-police-department$", "-pd", regex=True)
        .str.replace(r"-sheriff[’']?s-office$", "-so", regex=True)

        .str.replace(r"^orleans-constable-1st-city-court$", "orleans-constable", regex=True)
        .str.replace(r"^camerson-so$", "cameron-so", regex=True)
        .str.replace(r"^e\.-baton", "east-baton", regex=True)
        .str.replace(r"^w-carroll", "west-carroll", regex=True)
        .str.replace(r"^la-state-police$", "louisiana-state-pd", regex=True)
        .str.replace(r"-par-", "-", regex=False)
        .str.replace(r"^railroad(.+)$", "", regex=True)
        .str.replace(r"^univ\.-pd---(\w+)$", r"\1-university-pd", regex=True)
        .str.replace(r"probation-&-parole---adult", "probation-parole-adult", regex=True)
        .str.replace(
            r"^office-of-juvenile-justice-central-office$",
            "office-of-juvenile-justice", regex=True
        )
        .str.replace(
            r"^(la-lottery-corp|louisiana-lottery-corporation-headquarters|probation-parole|desheriff’s-officeto-so)$",
            "louisiana-lottery-corp", regex=True
        )
        .str.replace(r"^univ\.-police-department---sheriff[’']s-officeuthe.*$", "", regex=True)
        .str.replace(r"fort-polk-mp$", "fort-polk-military-pd", regex=True)
        .str.replace(r"alcohol-&-tobacco-control", "alcohol-tobacco-control", regex=False)
        .str.replace(r"-jdc$", "-judicial-district-court", regex=True)
        .str.replace(r"supreme-court-of-la$", "louisiana-supreme-court", regex=True)
        .str.replace(r"^(6th-district-da-office)$", "6th-judicial-district-court", regex=True)

        .str.replace(r"^post$", "post", regex=True)
        .str.replace(r"^ebrs-o$", "east-baton-rouge-so", regex=True)  # safety
        .str.replace(r"^ouachita-so$", "ouachita-so", regex=True)
        .str.replace(r"^baton-rouge-airport-p\.?d\.?$", "baton-rouge-airport-pd", regex=True)
    )

    out = df.copy()
    out[out_col] = s

    if drop_empty:
        out = out[~(out[out_col].fillna("") == "")]
    return out


def clean24():
    df24 = (
        pd.read_csv(
            deba.data("raw/post_council/post_decertifications_5_12_2023.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "decertification_date"})
        .pipe(split_name_24)
        .pipe(clean_agency_24, drop_empty=False)
        .drop(columns=["agency"])
        .rename(columns={"agency_slug": "agency"})
        .pipe(filter_agencies)
        .pipe(assign_date)
        .pipe(clean_allegations)
        .pipe(assign_action)
        .pipe(standardize_desc_cols, ["agency"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "decertification_date"], "allegation_uid")
    )
    return df24


def concat_dfs(dfa, dfb):
    df = pd.concat([dfa, dfb])
    return df

def concat_drop_dupes(dfa: pd.DataFrame, dfb: pd.DataFrame, subset=None) -> pd.DataFrame:
    """
    Concatenate two DataFrames and drop duplicates.

    Args:
        dfa (pd.DataFrame): First DataFrame
        dfb (pd.DataFrame): Second DataFrame
        subset (list, optional): Columns to check for duplicates.
            If None, checks all columns.

    Returns:
        pd.DataFrame: Concatenated DataFrame with duplicates removed.
    """
    df_combined = pd.concat([dfa, dfb], ignore_index=True)
    if subset:
        df_combined = df_combined.drop_duplicates(subset=subset)
    else:
        df_combined = df_combined.drop_duplicates()
    return df_combined.reset_index(drop=True)

if __name__ == "__main__":
    df20 = clean20()
    df23 = clean23()
    df24 = clean24()
    df = concat_dfs(df20, df23)
    df2 = concat_drop_dupes(df, df24, subset=["first_name", "last_name", "decertification_date"])
    df20.to_csv(deba.data("clean/cprr_post_2016_2019.csv"), index=False)
    df23.to_csv(
        deba.data("clean/cprr_post_decertifications_4_18_2023.csv"), index=False
    )
    df24.to_csv(
        deba.data("clean/cprr_post_decertifications_5_12_2023.csv"), index=False
    )
    df.to_csv(deba.data("clean/cprr_post_decertifications_2016_2023.csv"), index=False)    
