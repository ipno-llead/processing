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
        .str.replace(r"^shreveport-city-marshall$", "shreveport-city-marshal", regex=True)
        .str.replace(r"^jefferson$", "jefferson-so", regex=True)
        .str.replace(r"^sulphur-city-marshall$", "sulphur-city-marshal", regex=True)
        .str.replace(r"^natchitoches$", "natchitoches-so", regex=True)
    )
    return df


def replace_impossible_dates(df):
    df.loc[:, "level_1_cert_date"] = df.level_1_cert_date.str.replace(
        "3201", "2013", regex=False
    )
    return df


def clean():
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
    df = df.dropna(0, "all", subset=["first_name"])
    df = (
        df.pipe(clean_agency)
        .pipe(standardize_desc_cols, ["employment_status"])
        .pipe(clean_dates, ["hire_date"])
        .pipe(replace_impossible_dates)
        .pipe(
            clean_dates,
            ["level_1_cert_date", "last_pc_12_qualification_date"],
            expand=False,
        )
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
    return df


if __name__ == "__main__":
    df = clean()

    df.to_csv(deba.data("clean/pprr_post_2020_11_06.csv"), index=False)
