import bolo
from lib.columns import clean_column_names
from lib.clean import float_to_int_str
import pandas as pd


def initial_processing():
    df = pd.read_csv(
        bolo.data("raw/ipm/new_orleans_pd_cprr_actions_taken_1931-2020.csv"),
        escapechar="\\",
    )
    df = df.dropna(axis=1, how="all")
    df = clean_column_names(df)
    df = df[
        [
            "allegation_primary_key",
            "action_primary_key",
            "action_taken_date",
            "action_taken_year",
            "action_taken_month",
            "action_taken_completed",
            "action_taken_category",
            "action_taken",
            "action_taken_oipm",
        ]
    ]
    return (
        df[df.allegation_primary_key.notna() & df.action_primary_key.notna()]
        .drop_duplicates()
        .reset_index(drop=True)
    )


def clean_category(df):
    df.loc[:, "action_taken_category"] = df.action_taken_category.str.replace(
        r"Adminstrative", "Administrative"
    )
    return df


def combine_date_columns(df):
    df.loc[:, "action_taken_date"] = df.action_taken_year.str.cat(
        [
            df.action_taken_month.str.zfill(2),
            df.action_taken_date.fillna("")
            .str.replace(r"^\d+\/(\d+)\/\d+$", r"\1")
            .str.zfill(2),
        ],
        sep="-",
    ).str.replace(r"^-.+", "")
    return df.drop(columns=["action_taken_year", "action_taken_month"])


def combine_columns(df):
    def combine(row):
        txts = []
        if pd.notnull(row.action_taken_category):
            txts.append("Category: %s" % row.action_taken_category)
        if pd.notnull(row.action_taken_date):
            txts.append("Date: %s" % row.action_taken_date)
        if pd.notnull(row.action_taken):
            txts.append(row.action_taken)
        if pd.notnull(row.action_taken_oipm):
            txts.append("OIPM: %s" % row.action_taken_oipm)
        if row.action_taken_completed == "Yes":
            txts.append("Completed")
        elif row.action_taken_completed == "No":
            txts.append("Not completed")
        return "; ".join(txts)

    df.loc[:, "action"] = df.apply(combine, axis=1, result_type="reduce")
    df = df.drop(
        columns=[
            "action_taken_date",
            "action_taken_completed",
            "action_taken_category",
            "action_taken",
            "action_taken_oipm",
            "action_primary_key",
        ]
    )
    return df


def combine_rows(df):
    records = []
    for idx, frame in df.groupby("allegation_primary_key"):
        records.append((idx, " | ".join(frame.action.to_list())))
    return pd.DataFrame.from_records(
        records, columns=["allegation_primary_key", "action"]
    )


def clean():
    df = initial_processing()
    return (
        df.pipe(
            float_to_int_str,
            [
                "allegation_primary_key",
                "action_primary_key",
                "action_taken_year",
                "action_taken_month",
            ],
        )
        .pipe(clean_category)
        .pipe(combine_date_columns)
        .pipe(combine_columns)
        .pipe(combine_rows)
    )


if __name__ == "__main__":
    df = clean()

    df.to_csv(bolo.data("clean/cprr_actions_new_orleans_pd_1931_2020.csv"), index=False)
