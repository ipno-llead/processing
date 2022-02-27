from lib.columns import clean_column_names
import deba
from lib.clean import clean_dates, standardize_desc_cols
from lib.columns import set_values
from lib.uid import gen_uid
import pandas as pd
import sys

sys.path.append("../")


def join_citizen_columns(df):
    df.loc[:, "citizen"] = (
        df.subject_ethnicity.fillna("null")
        + " ; "
        + df.subject_gender.fillna("null")
        + " ; "
        + df.subject_hospitalized.fillna("null")
        + " ; "
        + df.subject_injured.fillna("null")
        + " ; "
        + df.subject_influencing_factors.fillna("null")
        + " ; "
        + df.subject_distance_from_officer.fillna("null")
        + " ; "
        + df.subject_age.fillna("null")
        + " ; "
        + df.subject_build.fillna("null")
        + " ; "
        + df.subject_height.fillna("null")
        + " ; "
        + df.subject_arrested.fillna("null")
        + " ; "
        + df.subject_arrest_charges.fillna("null")
    )
    df.loc[:, "citizen"] = (
        df.citizen.str.lower()
        .str.strip()
        .fillna("null")
        .astype(str)
        .str.replace(r">", "", regex=True)
        .str.replace(r"<", "", regex=True)
        .str.replace(r'"', "", regex=True)
        .str.replace(r"'", "", regex=True)
        .str.replace(r"- ?", "", regex=True)
        .str.replace(r"\|  \|", "| null |", regex=True)
        .str.replace(r"\|  \)", "| null)", regex=True)
        .str.replace(
            r"(\w+) (\w+) ?(\w+)? ?(\w+)? ?(\w+)? ?(\w+)?",
            r"\1\2\3\4\5\6",
            regex=True,
        )
        .str.replace(r"(\w+)\|", r"\1 |", regex=True)
        .str.replace(r"(\w+);", r"\1 ;", regex=True)
        .str.replace(
            r"(\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)?",
            r"(\1 | \3 | \5 | \7 | \9 | \11 | \13 | \15 | \17 | \19 | \21) & (\2 | \4 | \6 | \8 | \10 | \12 | \14 | \16 | \18 | \20 | \22)",
            regex=True,
        )
        .str.replace(
            r"(\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)? ?\|? ?(\w+)?",
            r"(\1 | \4 | \7 | \10 | \13 | \16 | \19 | \22 | \25 | \28 | \31) & (\2 | \5 | \8 | \11 | \14 | \17 | \20 | \23 | \26 | \29 | \32) & (\3 | \6 | \9 | \12 | \15 | \18 | \21 | \24 | \27 | \30 | \33)",
            regex=True,
        )
        .str.replace(
            r"(\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)?",
            r"(\1 | \5 | \9 | \13 | \17 | \21 | \25 | \29 | \33 | \37 | \41) & (\2 | \6 | \10 | \14 | \18 | \22 | \26 | \30 | \34 | \38 | \42) & (\3 | \7 | \11 | \15 | \19 | \23 | \27 | \31 | \35 | \39 | \43) & (\4 | \8 | \12 | \16 | \20 | \24 | \28 | \32 | \36 | \40 | \44)",
            regex=True,
        )
        .str.replace(
            r"(\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)?",
            r"(\1 | \6 | \11 | \16 | \21 | \26 | \31 | \36 | \41 | \46 | \51) & (\2 | \7 | \12 | \18 | \23 | \28 | \33 | \38 | \43 | \48 | \53) & (\3 | \8 | \13 | \18 | \23 | \28 | \33 | \38 | \43 | \48 | \53) & (\4 | \9 | \14 | \19 | \24 | \29 | \34 | \39 | \44 | \49 | \54) & (\5 | \10 | \15 | \20 | \25 | \30 | \35 | \40 | \45 | \50 | \55)",
            regex=True,
        )
        .str.replace(
            r"(\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)?",
            r"(\1 | \7 | \13 | \19 | \25 | \31 | \37 | \43 | \49 | \55 | \61) & (\2 | \8 | \14 | \20 | \26 | \32 | \38 | \44 | \50 | \56 | \62) & (\3 | \9 | \15 | \21 | \27 | \33 | \39 | \45 | \51 | \57 | \63) & (\4 | \10 | \16 | \22 | \28 | \34 | \40 | \46 | \52 | \58 | \64) & (\5 | \11 | \17 | \23 | \29 | \35 | \41 | \47 | \53 | \59 | \65) & (\6 | \12 | \18 | \24 | \30 | \36 | \42 | \48 | \54 | \60 | \66)",
            regex=True,
        )
        .str.replace(
            r"(\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)?",
            r"(\1 | \8 | \15 | \22 | \29 | \36 | \43 | \50 | \57 | \64 | \71) & (\2 | \9 | \16 | \23 | \30 | \37 | \44 | \51 | \58 | \65 | \72) & (\3 | \10 | \17 | \24 | \31 | \38 | \45 | \52 | \59 | \66 | \73) & (\4 | \11 | \18 | \25 | \32 | \39 | \46 | \53 | \60 | \67 | \74) & (\5 | \12 | \19 | \26 | \33 | \40 | \47 | \54 | \61 | \68 | \75) & (\6 | \13 | \20 | \27 | \34 | \41 | \48 | \55 | \62 | \69 | \76) & (\7 | \14 | \21 | \28 | \35 | \42 | \49 | \56 | \63 | \70 | \77)",
            regex=True,
        )
        .str.replace(
            r"(\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)?",
            r"(\1 | \9 | \17 | \25 | \33 | \41 | \49 | \57 | \65 | \73 | \81) & (\2 | \10 | \18 | \26 | \34 | \42 | \50 | \58 | \66 | \74 | \82) & (\3 | \11 | \19 | \27 | \35 | \43 | \51 | \59 | \67 | \75 | \83) & (\4 | \12 | \20 | \28 | \36 | \44 | \52 | \60 | \68 | \76 | \84) & (\5 | \13 | \21 | \29 | \37 | \45 | \53 | \61 | \69 | \77 | \85) & (\6 | \14 | \22 | \30 | \38 | \46 | \54 | \62 | \70 | \78 | \86) & (\7 | \15 | \23 | \31 | \39 | \47 | \55 | \63 | \71 | \79 | \87) & (\8 | \16 | \24 | \32 | \40 | \48 | \56 | \64 | \72 | \80 | \88)",
            regex=True,
        )
        .str.replace(
            r"(\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)? ?\|? ?(\w+)?",
            r"(\1 | \10 | \19 | \28 | \37 | \46 | \55 | \64 | \73 | \82 | \91) & (\2 | \11 | \20 | \29 | \38 | \47 | \56 | \65 | \74 | \83 | \92) & (\3 | \12 | \21 | \30 | \39 | \48 | \57 | \66 | \75 | \84 | \93) & (\4 | \13 | \22 | \31 | \40 | \49 | \58 | \67 | \76 | \85 | \94) & (\5 | \14 | \23 | \32 | \41 | \50 | \59 | \68 | \77 | \86 | \95) & (\6 | \15 | \24 | \33 | \42 | \51 | \60 | \69 | \78 | \87 | \96) & (\7 | \16 | \25 | \34 | \43 | \52 | \61 | \70 | \79 | \88 | \97) & (\8 | \17 | \26 | \35 | \44 | \53 | \62 | \71 | \80 | \89 | \98) & (\9 | \18 | \27 | \36 | \45 | \54 | \63 | \72 | \81 | \90 | \99)",
            regex=True,
        )
    )
    return df


def split_rows_with_multiple_citizens(df):
    df = (
        df.drop("citizen", axis=1)
        .join(
            df["citizen"]
            .str.split("&", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("citizen"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df.drop(
        columns=[
            "subject_ethnicity",
            "subject_gender",
            "subject_hospitalized",
            "subject_injured",
            "subject_influencing_factors",
            "subject_distance_from_officer",
            "subject_age",
            "subject_build",
            "subject_height",
            "subject_arrested",
            "subject_arrest_charges",
        ]
    )


def join_officer_columns(df):
    df.loc[:, "officer"] = (
        df.officer_name.fillna("null")
        + " ; "
        + df.officer_race_ethnicity.fillna("null")
        + " ; "
        + df.officer_gender.fillna("null")
        + " ; "
        + df.officer_age.fillna("null")
        + " ; "
        + df.use_of_force_effective.fillna("null")
        + " ; "
        + df.use_of_force_type.fillna("null")
        + " ; "
        + df.use_of_force_level.fillna("null")
        + " ; "
        + df.officer_injured.fillna("null")
    )
    df.loc[:, "officer"] = (
        df.officer.str.lower()
        .str.strip()
        .fillna("null")
        .astype(str)
        .str.replace(r'"', "", regex=True)
        .str.replace(r"'", "", regex=True)
        .str.replace(r"\|  \|", "| null |", regex=True)
        .str.replace(r"\(", "", regex=True)
        .str.replace(r"\)", "", regex=True)
        .str.replace(r"- ?", "", regex=True)
        .str.replace(r"\/", "", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"^ ", "", regex=True)
        .str.replace(
            r"(\w+) ?(\w+)? ?(\w+)? ?(\w+)? ?(\w+)? ?(\w+)?",
            r"\1\2\3\4\5\6",
            regex=True,
        )
        .str.replace(r"(\w+)\|", r"\1 |", regex=True)
        .str.replace(r"(\w+);", r"\1 ;", regex=True)
        .str.replace(
            r"(\w+\,? ?\w+?) \| (\w+\,? ?\w+?) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+) ; (\w+) \| (\w+)",
            r"(\1 | \3 | \5 | \7 | \9 | \11 | \13 | \15) & (\2 | \4 | \6 | \8 | \10 | \12 | \14 | \16)",
            regex=True,
        )
        .str.replace(
            r"(\w+\,? ?\w+?) \| (\w+\,? ?\w+?) \| (\w+\w+?\,? ?\w+?) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+)",
            r"(\1 | \4 | \7 | \10 | \13 | \16 | \19 | \22) & (\2 | \5 | \8 | \11 | \14 | \17 | \20 | \23) & (\3 | \6 | \9 | \12 | \15 | \18 | \21 | \24)",
            regex=True,
        )
        .str.replace(
            r"(\w+\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+)",
            r"(\1 | \5 | \9 | \13 | \17 | \21 | \25 | \29) & (\2 | \6 | \10 | \14 | \18 | \22 | \26 | \30) & (\3 | \7 | \11 | \15 | \19 | \23 | \27 | \31) & (\4 | \8 | \12 | \16 | \20 | \24 | \28 | \32)",
            regex=True,
        )
        .str.replace(
            r"(\w+\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+)",
            r"(\1 | \6 | \11 | \16 | \21 | \26 | \31 | \36) & (\2 | \7 | \12 | \17 | \22 | \27 | \32 | \37) & (\3 | \8 | \13 | \18 | \23 | \28 | \33 | \38) & (\4 | \9 | \14 | \19 | \24 | \29 | \34 | \39) & (\5 | \10 | \15 | \20 | \25 | \30 | \35 | \40)",
            regex=True,
        )
        .str.replace(
            r"(\w+\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+)",
            r"(\1 | \7 | \13 | \19 | \25 | \31 | \37 | \43) & (\2 | \8 | \14 | \20 | \26 | \32 | \38 | \44) & (\3 | \9 | \15 | \21 | \27 | \33 | \39 | \45) & (\4 | \10 | \16 | \22 | \28 | \34 | \40 | \46) & (\5 | \11 | \17 | \23 | \29 | \35 | \41 | \47) & (\6 | \12 | \18 | \24 | \30 | \36 | \42 | \48)",
            regex=True,
        )
        .str.replace(
            r"(\w+\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+)",
            r"(\1 | \8 | \15 | \22 | \29 | \36 | \43 | \50) & (\2 | \9 | \16 | \23 | \30 | \37 | \44 | \51) & (\3 | \10 | \17 | \24 | \31 | \38 | \45 | \52) & (\4 | \11 | \18 | \25 | \32 | \39 | \46 | \53) & (\5 | \12 | \19 | \26 | \33 | \40 | \47 | \54) & (\6 | \13 | \20 | \27 | \34 | \41 | \48 | \55) & (\7 | \14 | \21 | \28 | \35 | \42 | \49 | \56)",
            regex=True,
        )
        .str.replace(
            r"(\w+\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) \| (\w+?\,? ?\w+?) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) ; (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+) \| (\w+)",
            r"(\1 | \9 | \17 | \25 | \33 | \41 | \49 | \57) & (\2 | \10 | \18 | \26 | \34 | \42 | \50 | \58) & (\3 | \11 | \19 | \27 | \35 | \43 | \51 | \59) & (\4 | \12 | \20 | \28 | \36 | \44 | \52 | \60) & (\5 | \13 | \21 | \29 | \37 | \45 | \53 | \61) & (\6 | \14 | \22 | \30 | \38 | \46 | \54 | \62) & (\7 | \15 | \23 | \31 | \39 | \47 | \55 | \63) & (\7 | \16 | \24 | \32 | \40 | \48 | \56 | \64)",
            regex=True,
        )
    )
    return df


def split_rows_with_multiple_officers(df):
    df = (
        df.drop("officer", axis=1)
        .join(
            df["officer"]
            .str.split("&", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("officer"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df.drop(
        columns=[
            "officer_name",
            "officer_race_ethnicity",
            "officer_gender",
            "officer_age",
            "use_of_force_effective",
            "use_of_force_type",
            "use_of_force_level",
            "officer_injured",
        ]
    )


def split_citizen_column(df):
    df.loc[:, "citizen"] = df.citizen.str.replace(r"\(|\)", "", regex=True).str.replace(
        r"^ ", "", regex=True
    )

    data = df.citizen.str.extract(
        r"(\w+?) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+) [;\|] "
        r"(\w+) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+)"
    )

    df.loc[:, "citizen_race"] = (
        data[0]
        .str.replace(r"^raceunknown$", "", regex=True)
        .str.replace(r"^w$", "white", regex=True)
        .str.replace("null", "", regex=False)
    )
    df.loc[:, "citizen_sex"] = (
        data[1]
        .str.replace(r"^f[ae]maa?le$", "female", regex=True)
        .str.replace(r"sexunk", "", regex=False)
        .str.replace("null", "", regex=False)
    )
    df.loc[:, "citizen_hospitalized"] = data[2]
    df.loc[:, "citizen_injured"] = data[3]
    df.loc[:, "citizen_influencing_factors"] = (
        data[4]
        .fillna("")
        .str.replace(
            r"^alchoholandunknowndrugs$", "alchohol and unknown drugs", regex=True
        )
        .str.replace(r"nonedetected", "none detected", regex=True)
        .str.replace(r"mentallyunstable", "mentally unstable", regex=True)
        .str.replace(r"^unknowndrugs$", "unknown drugs", regex=True)
        .str.replace(r"null", "", regex=True)
    )
    df.loc[:, "citizen_distance_from_officer"] = (
        data[5]
        .str.replace(r"(\w+)feetto(\w+)feet", r"\1' feet to \2' feet", regex=True)
        .str.replace(r"null", "", regex=True)
    )
    df.loc[:, "citizen_age"] = data[6].str.replace("null", "", regex=False)
    df.loc[:, "citizen_build"] = data[7].str.replace("null", "", regex=False)
    df.loc[:, "citizen_height"] = (
        data[8]
        .fillna("")
        .str.replace("510to60", "5'10 to 6'0", regex=False)
        .str.replace(r"^(\w{1})(\w{1})to(\w{1})(\w{1})", r"\1'\2 to '\3'\4", regex=True)
        .str.replace("null", "", regex=False)
    )
    df.loc[:, "citizen_arrested"] = data[9].fillna("")
    df.loc[:, "citizen_arrest_charges"] = (
        data[10]
        .str.replace(
            r"^illegalcarryingofaweapon$", "illegal carrying of a weapon", regex=True
        )
        .str.replace(
            r"^possessionofstolenproperty$", "possession of stolen property", regex=True
        )
        .str.replace(r"^publicintoxication$", "public intoxication", regex=True)
        .str.replace(r"(yes|no)", "", regex=True)
        .str.replace("null", "", regex=False)
    )
    return df.drop(columns=["citizen"])


def split_officer_column(df):
    df.loc[:, "officer"] = df.officer.str.replace(r"\(|\)", "", regex=True)

    data = df.officer.str.extract(
        r"(\w+\, ?\w+?) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+) [;\|] (\w+)"
    )

    df.loc[:, "officer_name"] = data[0]
    names = df.officer_name.str.extract(r"(\w+)\, ?(\w+)?")
    df.loc[:, "last_name"] = (
        names[0]
        .str.replace(r"^sanclementehaynes$", "sanclemente-haynes", regex=True)
        .str.replace(r"^hamiltonmeyers$", "hamilton-meyers", regex=True)
        .str.replace(r"^oquendojohnson$", "oquendo-johnson", regex=True)
        .str.replace(r"^lewiswilliams$", "lewis-williams", regex=True)
    )
    df.loc[:, "first_name"] = names[1].str.replace(
        r"seanmichael", "sean-michael", regex=True
    )

    df.loc[:, "race"] = (
        data[1]
        .fillna("")
        .str.replace("notspecified", "", regex=False)
        .str.replace("asianpacificislander", "asian/pacific islander", regex=False)
        .str.replace(r"^notapplicablenonus$", "", regex=True)
        .str.replace(r"^americanindianalaskanative$", "native american", regex=True)
    )
    df.loc[:, "sex"] = data[2]
    df.loc[:, "age"] = data[3]
    df.loc[:, "use_of_force_effective"] = data[4]
    df.loc[:, "use_of_force_type"] = (
        data[5]
        .str.replace("firearmexhibited", "firearm exhibited", regex=False)
        .str.replace(r"^takedownwinjury$", "takedown with injury", regex=True)
        .str.replace(r"^riflepointed$", "rifle pointed", regex=True)
        .str.replace(r"^forcetakedown$", "force takedown", regex=True)
        .str.replace(r"^takedownnoinjury$", "takedown no injury", regex=True)
        .str.replace(r"^cewdeployment$", "cew deployment", regex=True)
        .str.replace(r"^headstrikenowep$", "headstrike no weapon", regex=True)
        .str.replace(r"^handcuffedsubject$", "handcuffed subject", regex=True)
        .str.replace(r"^firearmdischarged$", "firearm discharged", regex=True)
        .str.replace(r"^forceescorttech$", "force escort tech", regex=True)
        .str.replace(r"^forcedefensetech$", "force defense tech", regex=True)
        .str.replace(r"^caninenobite$", "canine no bite", regex=True)
        .str.replace(r"^caninebite$", "canine bite", regex=True)
        .str.replace(r"^vehicleasweapon$", "vehicle as weapon", regex=True)
        .str.replace(r"^cewexhibitedlaser$", "cew exhibited laser", regex=True)
        .str.replace(r"^batonpr24nonstrk$", "baton pr 24 non-strike", regex=True)
        .str.replace(r"^batonpr24miss$", "baton pr 24 miss", regex=True)
        .str.replace(r"^caninecontact$", "canine contact", regex=True)
        .str.replace(r"^shotgunpointed$", "shotgun pointed", regex=True)
        .str.replace(
            r"^nontradimpactweapon$", "non traditional impact weapon", regex=True
        )
        .str.replace(r"^batonpr24strike$", "baton pr 24 strike", regex=True)
        .str.replace(r"^batonnonstrike$", "baton non strike", regex=True)
        .str.replace(r"^firearmetended$", "firearm exhibited", regex=True)
        .str.replace(r"^forceneckholds$", "force neckholds", regex=True)
        .str.replace(r"^handswithinjury$", "hands with injury", regex=True)
        .str.replace(r"^vehpursuitswinjury$", "vehicle pursuit with injury", regex=True)
        .str.replace(r"^rifledischarged$", "rifle discharged", regex=True)
    )

    df.loc[:, "use_of_force_level"] = data[6]
    df.loc[:, "officer_injured"] = data[7].str.replace("l1", "", regex=False)

    df = df[~((df.first_name == "null") & (df.last_name == "null"))]
    return df.drop(columns=["officer", "officer_name"])


def clean_tracking_number(df):
    df.loc[:, "tracking_number"] = (
        df.filenum.str.lower().str.strip().str.replace(r"^20", "ftn20", regex=True)
    )
    return df.drop(columns=["filenum"])


def clean_originating_bureau(df):
    df.loc[:, "originating_bureau"] = (
        df.originating_bureau.str.lower()
        .str.strip()
        .str.replace(r"^(\w+) - ", "", regex=True)
    )
    return df


def clean_division_level(df):
    df.loc[:, "division_level"] = (
        df.division_level.str.lower()
        .str.strip()
        .str.replace("police ", "", regex=False)
        .str.replace(" division", "", regex=False)
        .str.replace(r"\bisb\b", "investigative services", regex=True)
        .str.replace(r" ?staff", "", regex=True)
        .str.replace(" section", "", regex=False)
    )
    return df


def clean_division(df):
    df.loc[:, "division"] = (
        df.division.str.lower()
        .str.strip()
        .str.replace(" section", "", regex=False)
        .str.replace(" team", "", regex=False)
        .str.replace(r"\bisb\b", "investigative services", regex=True)
        .str.replace(r" ?staff", "", regex=True)
        .str.replace(r"^b$", "", regex=True)
        .str.replace("tact", "tactical", regex=False)
        .str.replace(r" \| ", "; ", regex=True)
        .str.replace("diu", "d.i.u", regex=False)
    )
    return df


def clean_unit(df):
    df.loc[:, "unit"] = (
        df.unit.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(" unit", "", regex=False)
        .str.replace("unknown", "", regex=False)
        .str.replace(r"null \| ", "", regex=True)
        .str.replace(r"^patr?$", "patrol", regex=True)
        .str.replace(r" diu$", "", regex=True)
        .str.replace(r"^k$", "k-9", regex=True)
        .str.replace(r" other$", "", regex=True)
        .str.replace("dwi", "d.i.u", regex=False)
        .str.replace(r"^uof$", "use of force", regex=True)
        .str.replace(r"^admin$", "administration", regex=True)
        .str.replace(r"\btact\b", "tactical", regex=True)
        .str.replace("diu", "d.i.u", regex=False)
        .str.replace(" section", "", regex=False)
        .str.replace(" staff", "", regex=False)
    )
    return df


def clean_working_status(df):
    df.loc[:, "working_status"] = (
        df.working_status.str.lower()
        .str.strip()
        .str.replace("unknown working status", "", regex=False)
        .str.replace("rui", "resigned under investigation", regex=False)
        .str.replace("off duty", "off-duty", regex=False)
        .str.replace("workingre", "working", regex=False)
    )
    return df


def clean_shift(df):
    df.loc[:, "shift_time"] = (
        df.shift_time.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("unknown shift hours", "", regex=False)
        .str.replace("8a-4p", "between 8am-4pm", regex=False)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"uof", "use of force", regex=False)
    )
    return df


def clean_service_type(df):
    df.loc[:, "service_type"] = (
        df.service_type.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r" \| ", "; ", regex=True)
        .str.replace("not used", "", regex=False)
    )
    return df


def clean_light_condition(df):
    df.loc[:, "light_condition"] = (
        df.light_condition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"null \| ", "", regex=True)
    )
    return df


def clean_weather_condition(df):
    df.loc[:, "weather_condition"] = (
        df.weather_condition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"null \| ", "", regex=True)
    )
    return df


def clean_use_of_force_reason(df):
    df.loc[:, "use_of_force_reason"] = (
        df.use_of_force_reason.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("refuse", "refused", regex=False)
        .str.replace("resisting", "resisted", regex=False)
        .str.replace(r"^no$", "", regex=True)
        .str.replace(r"w\/", "with ", regex=True)
        .str.replace(" police ", " ", regex=False)
    )
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_uof_2016_2021.csv"))
        .pipe(clean_column_names)
        .drop(columns="officer_years_of_service")
        .rename(columns={"occurred_date": "occur_date", "shift": "shift_time"})
        .pipe(join_citizen_columns)
        .pipe(join_officer_columns)
        .pipe(split_rows_with_multiple_citizens)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_officer_column)
        .pipe(split_citizen_column)
        .pipe(clean_tracking_number)
        .pipe(clean_dates, ["occur_date"])
        .pipe(clean_originating_bureau)
        .pipe(clean_division_level)
        .pipe(clean_division)
        .pipe(clean_unit)
        .pipe(clean_working_status)
        .pipe(clean_shift)
        .pipe(clean_disposition)
        .pipe(clean_service_type)
        .pipe(clean_light_condition)
        .pipe(clean_weather_condition)
        .pipe(clean_use_of_force_reason)
        .pipe(standardize_desc_cols, ["investigation_status", "use_of_force_level"])
        .pipe(set_values, {"agency": "New Orleans PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_number",
                "use_of_force_reason",
                "use_of_force_level",
                "use_of_force_type",
                "use_of_force_effective",
                "disposition",
                "citizen_race",
                "citizen_sex",
                "citizen_age",
            ],
            "uof_uid",
        )
        .drop_duplicates(subset=["uof_uid"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/uof_new_orleans_pd_2016_2021.csv"), index=False)
