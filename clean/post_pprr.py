from lib.columns import clean_column_names
import deba
from lib.clean import (
    clean_names,
    clean_dates,
    names_to_title_case,
    standardize_desc_cols,
)
from lib.uid import gen_uid
import pandas as pd


def clean_agency(df):
    df.loc[:, "agency"] = (
        df.agency.str.lower()
        .str.strip()
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\bd +a\b", "da", regex=True)
        .str.replace(r"\s+", "-", regex=True)
        .str.replace(r"\-da\-office", "-da", regex=True)
        .str.replace(r"&", "", regex=True)
        .str.replace(r"\-+", "-", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\'", "", regex=True)
        .str.replace(r"^(\w+)-district-attorney", r"\1-da", regex=True)
        .str.replace(r"connpd", "conn-pd", regex=False)
        .str.replace(r"^ebr", "east-baton-rouge", regex=True)
        .str.replace(r"^e\-", "east-", regex=True)
        .str.replace(
            r"^univ\-pdbaton\-rouge\-cc$", "baton-rouge-cc-univ-pd", regex=True
        )
        .str.replace(r"\’", "", regex=True)
        .str.replace(r"das-office$", "da", regex=True)
        .str.replace(r"^univ\-pd\-(.+)", r"\1-university-pd", regex=True)
        .str.replace(r"\-pari?s?h?\-", "-", regex=True)
        .str.replace(r"^la\-", "louisiana-", regex=True)
        .str.replace(r"\-police$", "-pd", regex=True)
        .str.replace(r"^orleans-so$", "new-orleans-so", regex=True)
        .str.replace(r"^w\-", "west-", regex=True)
        .str.replace(r"\bdept\b", "department", regex=True)
        .str.replace(
            r"^medical-center-of-la-no$",
            "medical-center-of-louisiana-new-orleans-pd",
            regex=True,
        )
        .str.replace(
            r"office-of-youth-dev\,\-dept-of-corrections",
            "office-of-youth-development-department-of-corrections",
            regex=True,
        )
        .str.replace(
            r"^louisiana-house-of-rep-sergeant-at-arms$",
            "louisiana-house-of-representatives-sergeant-at-arms",
            regex=True,
        )
        .str.replace(
            r"^delgado-cc-university-pd$",
            "delgado-community-college-university-pd",
            regex=True,
        )
        .str.replace(
            r"^lafayette-city-park-rec$",
            "lafayette-city-parks-and-recreation",
            regex=True,
        )
        .str.replace(r"^jefferson-1st-court$", "jefferson-first-court", regex=True)
        .str.replace(
            r"^housing-authority-of-no$", "housing-authority-of-new-orleans", regex=True
        )
        .str.replace(r"\buniv\b", "university", regex=True)
        .str.replace(r"\b-cc-\b", "-community-college-", regex=True)
        .str.replace(r"^city-park-pd-no$", "new-orleans-city-park-pd", regex=True)
        .str.replace(r"-jdc-", "-judicial-district-court-", regex=True)
        .str.replace(
            r"^5th-jdc-district-attorneys-office$",
            "5th-judicial-district-court-district-attorneys-office",
            regex=True,
        )
        .str.replace(r"^29th-jdc-da$", "29th-judicial-district-court-da", regex=True)
        .str.replace(r"jdc$", "judicial-district-court", regex=True)
        .str.replace(
            r"jdc-da$", "judicial-district-court-district-attorneys-office", regex=True
        )
        .str.replace(
            r"^se-la-flood-protection-auth-e$",
            "southeast-louisiana-flood-protection-authority",
            regex=True,
        )
        .str.replace(r"^$", "", regex=True)
        .str.replace(r"^$", "", regex=True)
        .str.replace(
            r"^office-of-youth-dev,-department-of-corrections$",
            "office-of-youth-development-department-of-corrections",
            regex=True,
        )
        .str.replace(
            r"^new-orleans-criminal-court$", "orleans-criminal-court", regex=True
        )
        .str.replace(
            r"^lsuhsc-no-university-pd$", "lsuhsc-new-orleans-university-pd", regex=True
        )
        .str.replace(r"^vermilion-o$^", "vermilion-so", regex=True)
        .str.replace(r"broussard-fd$", "broussard-fire-department", regex=True)
        .str.replace(r"^my-shal$", "", regex=True)
        .str.replace(r"^gretnapld$", "gretna-pd", regex=True)
        .str.replace(r"^sthelena-so$", "st-helena-so", regex=True)
        .str.replace(r"^ng-oaparish-so$", "", regex=True)
        .str.replace(
            r"^shreveport-city-marshall$", "shreveport-city-marshal", regex=True
        )
        .str.replace(r"^jefferson$", "jefferson-so", regex=True)
        .str.replace(r"^sulphur-city-marshall$", "sulphur-city-marshal", regex=True)
        .str.replace(r"^natchitoches$", "natchitoches-so", regex=True)
        .str.replace(r"4th-da", "morehouse-da", regex=False)
        .str.replace(r"^juvenile-services-br$", "juvenile-services-bureau", regex=True)
        .str.replace(
            r"^alcoholic-beverage-control-ebr$",
            "east-baton-rouge-office-of-alcohol-beverage-control",
            regex=True,
        )
        .str.replace(r"culture\,-rec-tourism", "culture-recreation-tourism", regex=True)
        .str.replace(r"hammond-marshal", "hammond-city-marshal", regex=True)
        .str.replace(r"^unknown$", "", regex=True)
    )
    return df[~((df.agency.fillna("") == ""))]


def replace_impossible_dates(df):
    df.loc[:, "level_1_cert_date"] = df.level_1_cert_date.str.replace(
        "3201", "2013", regex=False
    )
    return df


def fix_date_format(df):
    df.loc[:, "level_1_cert_date"] = df.level_1_cert_date.astype(str).str.replace(
        r"(\w+)\/(\w+)\/(\w+)", r"\3-\1-\2", regex=True
    )
    df.loc[
        :, "last_pc_12_qualification_date"
    ] = df.last_pc_12_qualification_date.astype(str).str.replace(
        r"(\w+)\/(\w+)\/(\w+)", r"\3-\1-\2", regex=True
    )
    return df


def filter_agencies(df):
    agencies = pd.read_csv(deba.data("raw/agency/agency_reference_list.csv"))
    agencies = agencies.agency_slug.tolist()
    df = df[df.agency.isin(agencies)]
    return df


def remove_test(df):
    df.loc[:, "last_name"] = df.last_name.str.replace(r"test2", "", regex=False)
    df.loc[:, "first_name"] = df.first_name.str.replace(r"officer", "", regex=False)
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean_hire_dates(df):
    df.loc[:, "hire_date"] = df.hire_date.fillna("").str.replace(
        r"(.+)\/1900", "", regex=True
    )
    return df[~((df.hire_date == ""))]


def clean_lvl_1_cert(df):
    df.loc[:, "level_1_cert_date"] = df.level_1_cert_date.str.replace(
        r"(\w{4})-(\w+)-(\w+)", r"\2/\3/\1", regex=True
    )
    return df


def split_hire_dates(df):
    dates = df.hire_date.str.extract(r"(\w+)\/(\w+)\/(\w+)")

    df.loc[:, "hire_month"] = dates[0]
    df.loc[:, "hire_day"] = dates[1]
    df.loc[:, "hire_year"] = dates[2]
    return df


def clean23():
    df = (
        pd.read_csv(
            deba.data("raw/post_council/post_pprr_4_26_2023.csv"), encoding="cp1252"
        )
        .pipe(clean_column_names)
        .rename(
            columns={
                "lastname": "last_name",
                "firstname": "first_name",
                "agency_name": "agency",
            }
        )
        .pipe(clean_agency)
        .pipe(filter_agencies)
        .pipe(replace_impossible_dates)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(remove_test)
        .pipe(clean_hire_dates)
        .pipe(split_hire_dates)
        .pipe(
            gen_uid,
            [
                "agency",
                "last_name",
                "first_name",
                "hire_year",
                "hire_month",
                "hire_day",
            ],
        )
        .drop_duplicates(
            subset=["hire_year", "hire_month", "hire_day", "uid"], keep="first"
        )
        .pipe(standardize_desc_cols, ["agency"])
    )
    return df


def clean20():
    df = pd.read_csv(deba.data("raw/post_council/post_pprr_11-6-2020.csv"))
    df = clean_column_names(df)
    df.columns = [
        "agency",
        "last_name",
        "first_name",
        "employment_status",
        "hire_date",
        "level_1_cert_date",
        "last_pc_12_qualification_date",
    ]
    df.loc[:, "data_production_year"] = "2020"
    df = (
        df.pipe(clean_agency)
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(fix_date_format)
        .pipe(replace_impossible_dates)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            gen_uid,
            [
                "agency",
                "last_name",
                "first_name",
                "hire_year",
                "hire_month",
                "hire_day",
            ],
        )
        .drop_duplicates(
            subset=["hire_year", "hire_month", "hire_day", "uid"], keep="first"
        )
        .pipe(standardize_desc_cols, ["agency"])
    )
    return df[~((df.last_name.fillna("") == ""))]


def concat_dfs(dfa, dfb):
    dfa_uids = [x for x in dfa["uid"]]
    dfb = dfb[~(dfb.uid.isin(dfa_uids))]
    return dfa, dfb


if __name__ == "__main__":
    df20 = clean20()
    df23 = clean23()
    df20, df23 = concat_dfs(df20, df23)
    df20.to_csv(deba.data("clean/pprr_post_2020_11_06.csv"), index=False)
    df23.to_csv(deba.data("clean/pprr_post_4_26_2023.csv"), index=False)