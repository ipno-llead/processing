import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def drop_rows_missing_dates(df):
    return df[~((df.notification_date.fillna("") == ""))]


def clean_tracking_id(df):
    subject = "R", "r", "T", "t"
    subjects = []
    for index, row in df.iterrows():
        if str(row).startswith(subject):
            df["subjects"] = row
    return df


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


def clean():
    df = (
        pd.read_csv(deba.data("ner/letters_louisiana_state_pd_2019.csv"))
        .pipe(clean_column_names)
        .drop(columns=["notification_date_1", "notification_date_2"])
        .pipe(drop_rows_missing_dates)
        .pipe(extract_ids_and_subject)
        .pipe(split_names)
        .pipe(sanitize_dates)
        .pipe(set_values, {"agency": "louisiana-state-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/letters_louisiana_state_pd_2019.csv"), index=False)
