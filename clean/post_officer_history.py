import deba
import pandas as pd
from lib.uid import gen_uid
from lib.clean import names_to_title_case, clean_sexes, clean_dates


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
    return df.pipe(names_to_title_case, ["first_name", "middle_name", "last_name"])[
        ~((df.first_name == "") & (df.last_name == ""))
    ]


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
            "first_name",
            "last_name",
            "middle_name",
        ]
    ].reset_index()
    names_df = names_df.rename(columns={"index": "history_id"})

    stacked_agency_df = stacked_agency_df.merge(names_df, on="history_id", how="right")

    return stacked_agency_df


def clean_agency_pre_split(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
        .str.lower()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office",
            "orleans coroners office",
            regex=True,
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
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
        .str.replace(r"-time(\w+\/\w+\/\w+)", r"-time \1", regex=True)
        .str.replace(r"\'", "", regex=True)
        .str.replace(r"_(\w+)", r"\1", regex=True)
    )
    return df


def split_agency_column(df):
    data = (
        df.agency.fillna("")
        .str.lower()
        .str.strip()
        .str.extract(
            r"(\w+? ?\w+? ? ?\w+? ?\w+? ? ?\w+?) ?(?:(full-time|reserve|retired|part-time|deceased?) )? ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?((.+)?termi?n?a?t?i?o?n?(.+)?|(.+)?resig(nation|ned)(.+)?|(.+)?(retired)(.+)?)?(.+)?$"
        )
    )

    df.loc[:, "agency"] = data[0]
    df.loc[:, "employment_status"] = data[1].str.replace(
        r"^decease$", "deceased", regex=True
    )
    df.loc[:, "hire_date"] = (
        data[2]
        .fillna("")
        .str.replace(r"^d(\w{1})", r"\1", regex=True)
        .str.replace(r"^(0|s)\/(.+)", "", regex=True)
        .str.replace(r"^in/i/i995$", "", regex=True)
        .str.replace(r"(.+)?7209(.+)?", "", regex=True)
        .str.replace(r"^2\/31(.+)", "", regex=True)
        .str.replace(r"^os/a7/2021$", "", regex=True)
    )

    df.loc[:, "left_date"] = (
        data[3]
        .fillna("")
        .str.replace(r"^_4\/g(.+)", "", regex=True)
        .str.replace(r"^(0|a|s|o)\/(.+)", "", regex=True)
        .str.replace(r"^in/i/i995$", "", regex=True)
        .str.replace(r"^7/51/2020", "", regex=True)
        .str.replace(r"^4/g/2012$", "", regex=True)
        .str.replace(r"^os/a7/2021$", "", regex=True)
    )
    df.loc[:, "left_reason"] = data[4].fillna("")

    return df


def clean_agency(df):
    df.loc[:, "agency"] = (
        df.agency.str.strip()
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
        .str.replace(r"^Deptpublic Safety$", "Department Of Public Safety", regex=True)
        .str.replace(r"^W\b ", "West ", regex=True)
        .str.replace(r"^E\b ", "", regex=True)
        .str.replace(r"^Univ PDxavier", "Xavier University PD", regex=True)
        .str.replace(r"La State Police", "Louisiana State PD", regex=True)
        .str.replace(
            r"^Univ PDlsuhscno$", "LSUHSC - New Orleans University PD", regex=True
        )
        .str.replace(
            r"^Univ PDdelgado Cc$",
            "Delgado Community College University PD",
            regex=True,
        )
        .str.replace(r"Univ PDdillard", "Dillard University PD", regex=True)
        .str.replace(r"^Outstate", "Out of State", regex=True)
        .str.replace(r"^Medical Centerlano$", "Medical Center Of La - No", regex=True)
        .str.replace(r"^Univ PDloyola$", "Loyola University PD", regex=True)
        .str.replace(r"^Univ PDlsu$", "LSU University PD", regex=True)
        .str.replace(r"^Orleans", "New Orleans", regex=True)
        .str.replace(r"\bLsu\b", "LSU", regex=True)
        .str.replace(
            r"Univ PDsouthernno",
            "Southern University PD",
            regex=True,
        )
        .str.replace(r" Par ", "", regex=True)
        .str.replace(r"Alcoholtobacco Control", "Alcohol Tobacco Control", regex=True)
        .str.replace(
            r"^Housing Authorityno$",
            "Housing Authority of New Orleans",
            regex=True,
        )
        .str.replace(r"^Univ PDtulane$", "Tulane University PD", regex=True)
        .str.replace(r"Univ PDuno", "UNO University PD")
        .str.replace(r"Univ PDla Tech", "Louisiana Tech University PD")
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
        .str.replace(r"^22Nd", "22nd", regex=True)
        .str.replace(r" Decease$", "", regex=True)
        .str.replace(r" \bParish\b ", " ", regex=True)
        .str.replace(r"^Jefferson Levee PD$", "East Jefferson Levee PD", regex=True)
        .str.replace(r"Time(.+)", "", regex=True)
        .str.replace(r"^St\b ", "St.", regex=True)
        .str.replace(r"(.+)?Range(.+)?", "", regex=True)
        .str.replace(r"(\d+)", "", regex=True)
        .str.replace(r"P_D$", "PD", regex=True)
        .str.replace(r"^Univ PDnicholls", "Nicholls University PD", regex=True)
        .str.replace(r"^Stjames SO$", "St. James SO", regex=True)
        .str.replace(r"^Wildlifefisheries$", "Wildlife & Fisheries", regex=True)
        .str.replace(r"^State Park(.+)", "", regex=True)
        .str.replace(r"^St\.(\w+)", r"St. \1", regex=True)
        .str.replace(r"^Lastate Police$", "Louisiana State PD", regex=True)
        .str.replace(r"_$", "", regex=True)
        .str.replace(r"^Houma ?[Pp]l?d", "Houma PD", regex=True)
        .str.replace(r"(Lapd(.+)|Sits(.+)|Poncr(.+)|Gional(.+))", "", regex=True)
        .str.replace(r"(\w+) So\b", r"\1 SO", regex=True)
        .str.replace(r"(PD|SO)? ?Unknown$", r"\1", regex=True)
        .str.replace(r"^Pearlriver", "Pearl River", regex=True)
        .str.replace(r"^Officeyouth Dev Deptcorrections$", "", regex=True)
        .str.replace(r"(.+)Reserve(.+)", "", regex=True)
        .str.replace(r"(.+)?Academy(.+)?", "", regex=True)
        .str.replace(r"Univ PDull", "", regex=True)
        .str.replace(r"(\w+)pd$", r"\1 PD", regex=True)
        .str.replace(r"^nd District Attorney$", "", regex=True)
        .str.replace(r"Univ PDsoutheastern", "Southeastern University PD", regex=True)
        .str.replace(r"^Univ PDmcneese$", "Mcneese University PD", regex=True)
        .str.replace(r"^Ladeptjustice$", "Louisiana Department Of Justice", regex=True)
        .str.replace(r"(^Tulane$|^Univ PD Tulane$)", "Tulane University PD", regex=True)
        .str.replace(r" \bUniv\b ", " University ", regex=True)
        .str.replace(r"^Charity Hospital Policeno", "Charity Hospital PD", regex=True)
        .str.replace(r"^Univ PDlsuhsc", "LSUHSC University PD", regex=True)
        .str.replace(r"^Univ PDsouthern$", "Southern University PD", regex=True)
        .str.replace(r"PDd$", "PD", regex=True)
        .str.replace(r"Tafourche SO", "Lafourche SO", regex=True)
        .str.replace(r"^Iefferson SO$", "Jefferson SO", regex=True)
        .str.replace(r"^Tangtpahoa SO$", "Tangipahoa SO", regex=True)
        .str.replace(r"^Dillard$", "Dillard University PD", regex=True)
        .str.replace(r"^Bunice PD", "Eunice PD", regex=True)
        .str.replace(r"^New Orleans Da Office$", "New Orleans DA", regex=True)
        .str.replace(r"^Crescent City Conn PD$", "Crescent City ConnPD.", regex=True)
    )
    return df


def clean_left_reason(df):
    l_reasons = df.left_reason.str.replace(
        r"volumtary", "voluntary", regex=False
    ).str.extract(
        r"(termination|involuntary resignation|voluntary resignation|resignation)"
    )

    df.loc[:, "left_reason"] = l_reasons[0]
    return df


def drop_duplicates(df):
    return df.drop_duplicates(subset="uid")


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
    dfa = pd.read_csv(deba.data("ner/advocate_post_officer_history_reports.csv"))
    dfb = pd.read_csv(deba.data("ner/post_officer_history_reports.csv"))
    df = (
        pd.concat([dfa, dfb], axis=0)
        .pipe(drop_rows_missing_names)
        .rename(columns={"officer_sex": "sex"})
        .pipe(clean_sexes, ["sex"])
        .pipe(split_names)
        .pipe(generate_history_id)
        .pipe(clean_agency_pre_split)
        .pipe(split_agency_column)
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
            ],
        )
        .pipe(clean_agency)
        .pipe(clean_left_reason)
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency"])
        .pipe(drop_duplicates)
        .pipe(check_for_duplicate_uids)
        .pipe(switched_job)
    )
    return df.fillna("")


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)
