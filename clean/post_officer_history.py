import deba
import pandas as pd
from lib.uid import gen_uid
from lib.clean import names_to_title_case, clean_sexes, clean_names


def drop_rows_missing_names(df):
    df.loc[:, "officer_name"] = df.officer_name.fillna("")
    return df[~((df.officer_name == ""))]


def split_names(df):
    names = df.officer_name.str.strip().str.extract(r"(\w+(?:'\w+)?),? (\w+)(?: (\w+))?")

    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    return df.drop(columns=["officer_name"]).pipe(names_to_title_case, ["first_name", "middle_name", "last_name"])


def clean_post_agency_row(df):
    for col in df.columns:
        if col.startswith("agency"):
            df[col] = (
                df[col]
                .str.strip()
                .str.lower()
                .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
                .str.replace(
                    r"^orleans parish coroner\'s office",
                    "orleans coroners office",
                    regex=True,
                )
                .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
                .str.replace(r"1st parish court", "first court", regex=False)
                .str.replace(
                    r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True
                )
                .str.replace(r" Â© ", "", regex=True)
                .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
                .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
                .str.replace(r" â€”= ", "", regex=True)
                .str.replace(r" +", " ", regex=True)
                .str.replace(r" & ", "", regex=True)
                .str.replace(r" - ", "", regex=True)
                .str.replace(r" _?â€\” ", "", regex=True)
                .str.replace(r" _ ", "", regex=True)
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
                .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
                .str.replace("new orleans harbor", "orleans harbor", regex=False)
                .str.replace(
                    "probation & parcole - adult",
                    "adult probation",
                    regex=True,
                )
            )
    return df


def extract_agency(df):
    for col in df.columns:
        if col.startswith("agency"):
            df[col] = (
                df[col]
                .fillna("")
                .str.lower()
                .str.strip()
                .str.replace(r"(\w? ?\w+ ?\w+? ?\w+) (.+)", r"\1", regex=True)
            )
    return df


def clean_post_agency_column(df):
    for col in df.columns:
        if col.startswith("agency"):
            df.loc[:, col] = (
                df[col]
                .str.strip()
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
                .str.replace(
                    r"^Deptpublic Safety$", "Department Of Public Safety", regex=True
                )
                .str.replace(r"^W\b ", "West ", regex=True)
                .str.replace(
                    r"^E jefferson levee pd", "East Jefferson Levee PD", regex=True
                )
                .str.replace(r"^E\b ", "", regex=True)
                .str.replace(r"^Univ PDxavier", "Xavier University PD", regex=True)
                .str.replace(r"La State Police", "Louisiana State PD", regex=True)
                .str.replace(
                    r"^Univ PDlsuhscno$", "LSUHSC - No University PD", regex=True
                )
                .str.replace(
                    r"^Univ PDdelgado Cc$",
                    "Delgado Community College University PD",
                    regex=True,
                )
                .str.replace(r"Univ PDdillard", "Dillard University PD", regex=True)
                .str.replace(r"^Outstate", "Out of State", regex=True)
                .str.replace(
                    r"^Medical Centerlano$", "Medical Center Of La - No", regex=True
                )
                .str.replace(r"^Univ PDloyola$", "Loyola University PD", regex=True)
                .str.replace(r"^Univ PDlsu$", "LSU University PD", regex=True)
                .str.replace(r"^Orleans", "New Orleans", regex=True)
                .str.replace(r"\bLsu\b", "LSU", regex=True)
                .str.replace(
                    r"Univ PDsouthernno",
                    "Southern University PD - New Orleans ",
                    regex=True,
                )
                .str.replace(r" Par ", "", regex=True)
                .str.replace(
                    r"Alcoholtobacco Control", "Alcohol Tobacco Control", regex=True
                )
                .str.replace(
                    r"^Housing Authorityno$",
                    "Housing Authority of New Orleans",
                    regex=True,
                )
                .str.replace(r"^Univ PDtulane$", "Tulane University PD", regex=True)
                .str.replace(r"Univ PDuno", "UNO University PD")
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
            )
    return df


def drop_duplicates(df):
    return df.drop_duplicates(subset=["first_name", "last_name"], keep="last")


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
        ]
    ].stack()

    stacked_agency_df = stacked_agency_sr.reset_index().iloc[:, [0, 2]]
    stacked_agency_df.columns = ["history_id", "agency"]

    names_df = df[["first_name", "last_name", "middle_name"]].reset_index()
    names_df = names_df.rename(columns={"index": "history_id"})

    stacked_agency_df = stacked_agency_df.merge(names_df, on="history_id", how="right")

    return stacked_agency_df


def check_for_duplicate_uids(df):
    uids = df.groupby(["uid"])["history_id"].agg(list).reset_index()

    for row in uids["history_id"]:
        unique = all(element == row[0] for element in row)
        if unique:
            continue
        else:
            raise ValueError("uid found in multiple history ids")

    return df[~((df.agency.fillna("") == ""))]


def switched_job(df):
    df.loc[:, "switched_job"] = df.duplicated(subset=["history_id"], keep=False)
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/post/post_officer_history.csv"))
        .pipe(drop_rows_missing_names)
        .rename(columns={"officer_sex": "sex"})
        .pipe(clean_sexes, ["sex"])
        .pipe(split_names)
        .pipe(clean_post_agency_row)
        .pipe(extract_agency)
        .pipe(
            names_to_title_case,
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
            ],
        )
        .pipe(clean_post_agency_column)
        .pipe(drop_duplicates)
        .pipe(generate_history_id)
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
        .pipe(check_for_duplicate_uids)
        .pipe(switched_job)
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)
