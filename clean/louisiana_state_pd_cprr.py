import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def drop_rows_missing_dates(df):
    return df[~((df.notification_date.fillna("") == ""))]


def extract_ids_and_subject(df):
    cols = [
        "accused_name_2",
        "tracking_id",
        "tracking_id_1",
        "tracking_id_2",
        "tracking_id_3",
    ]
    df["data"] = df[cols].apply(lambda row: " ".join(row.values.astype(str)), axis=1)

    subjects = df.data.str.lower().str.extract(r"(re.+)")

    df.loc[:, "subject_of_letter"] = (
        subjects[0]
        .str.replace(r"( ?nan ?|re: | ?_ ?)", "", regex=True)
        .str.replace(r"(\w+) \((\w+)\) hour", r"\2-hour", regex=True)
        .str.replace(r"(\w+) hour", r"\1-hour", regex=True)
        .str.replace("eight", "8", regex=False)
        .str.replace(r"twenty- ", "", regex=False)
    )

    ids = (
        df.data.str.lower()
        .str.replace(r" ?nan ?", " ", regex=True)
        .str.replace(r"re.+", " ", regex=True)
        .str.extract(r"(.+)")
    )

    df.loc[:, "tracking_id"] = (
        ids[0]
        .str.replace(r" +$", "", regex=True)
        .str.replace(r" lt. marie", "", regex=True)
        .str.replace(r"014a", "014", regex=False)
        .str.replace(r"(.+) ola", r"\1; ola", regex=True)
        .str.replace(r"^[1t]a", "ia", regex=True)
        .str.replace(r"(\w+) ?# ?(\w+)", r"\1 \2", regex=True)
        .str.replace(r"n19", "19", regex=True)
    )

    return df.drop(
        columns=[
            "accused_name_2",
            "tracking_id_1",
            "tracking_id_2",
            "tracking_id_3",
            "data",
        ]
    )


def split_names(df):
    names = (
        df.accused_name_1.str.lower()
        .str.strip()
        .str.replace(r"st\, clair", "stclair", regex=True)
        .str.replace(r"(“(\w+)”) ", "", regex=True)
        .str.extract(
            r"(master trooper|pilot|sergeant|lt\.|cadet|trooper|lieutenant) (\w+) (\w+)"
        )
    )

    df.loc[:, "rank_desc"] = names[0].str.replace(r"lt\.", "lieutenant", regex=True)
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns=["accused_name_1"])


def sanitize_dates(df):
    df.loc[:, "notification_date"] = (
        df.notification_date.str.lower()
        .str.strip()
        .str.replace(r"(\w+)\, (\w+)", r"\1/\2", regex=True)
        .str.replace(r"october (.+)", r"10/\1", regex=True)
        .str.replace(r"december (.+)", r"12/\1", regex=True)
        .str.replace(r"august (.+)", r"08/\1", regex=True)
        .str.replace(r"june (.+)", r"06/\1", regex=True)
        .str.replace(r"may (.+)", r"05/\1", regex=True)
        .str.replace(r"april (.+)", r"04/\1", regex=True)
        .str.replace(r"march (.+)", r"03/\1", regex=True)
        .str.replace(r"february (.+)", r"02/\1", regex=True)
    )
    return df


def clean_letters_2019():
    df = (
        pd.read_csv(deba.data("ner/letters_louisiana_state_pd_2019.csv"))
        .pipe(clean_column_names)
        .pipe(drop_rows_missing_dates)
        .pipe(extract_ids_and_subject)
        .pipe(split_names)
        .pipe(sanitize_dates)
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["subject_of_letter", "uid", "notification_date"], "allegation_uid")
    )
    return df


def join_allegation_columns(df):
    for index, row in df.iterrows:
        subjects = [row for row in df]
    return subjects

# def join_allegation_columns(df):
#     df = df.fillna("")
#     cols = [
#         "previous_discipline",
#         "previous_discipline_1",
#         "previous_discipline_2",
#         "previous_discipline_3",
#         "previous_discipline_4",
#         "previous_discipline_5",
#         "previous_discipline_6",
#     ]   
#     df["discipline"] = df[cols].apply(lambda row: "".join(row.fillna("").values.astype(str)), axis=1)
#     df.loc[:, "discipline_extracted"] = ""
#     # df.loc[
#     #     (df.md5.fillna("") == "5483e73cc28de9b2b05fecd6ba0c4f24") & (df.report_date.fillna("") == "September 28, 2020"),
#     #     "discipline_extracted",
#     # ] = df.discipline
#     return df



# def generate_history_id(df):
#     stacked_agency_sr = df[
#         [
#             "md5",
#         ]
#     ].stack()

#     stacked_agency_df = stacked_agency_sr.reset_index().iloc[:, [0, 2]]
#     stacked_agency_df.columns = ["history_id", "md5"]

#     names_df = df[
#         [
#             "report_date", "allegation", "previous_disicpline"
#         ]
#     ].reset_index()
#     names_df = names_df.rename(columns={"index": "history_id"})

#     stacked_agency_df = stacked_agency_df.merge(names_df, on="history_id", how="right")

#     return stacked_agency_df


# def search(df):
#     values = "5483e73cc28de9b2b05fecd6ba0c4f24"
#     dates = [x.notna() for x in str(df["report_date"]) if x]
#     return dates

def clean_reports_2020():
    df = pd.read_csv(deba.data("ner/reports_louisiana_state_pd_2020.csv"))\
        .pipe(clean_column_names)\
        .pipe(join_allegation_columns)
    return df

if __name__ == "__main__":
    df1 = clean_letters_2019()
    df2 = clean_reports_2020()
    df1.to_csv(deba.data("clean/cprr_louisiana_state_pd_2019.csv"), index=False)
