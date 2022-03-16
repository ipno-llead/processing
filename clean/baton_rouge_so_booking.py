import pandas as pd
import deba
from lib.columns import clean_column_names
from lib.clean import clean_sexes, standardize_desc_cols


def clean_race(df):
    df.loc[:, "citizen_race"] = (
        df.race.str.lower()
        .str.strip()
        .str.replace("other", "", regex=False)
        .str.replace("unknown", "", regex=False)
    )
    return df.drop(columns="race")


def clean_homeless(df):
    df.loc[:, "citizen_homeless"] = (
        df.is_homeless.str.lower().str.strip().str.replace("no", "", regex=False)
    )
    return df.drop(columns="is_homeless")


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/baton_rouge_so/east_baton_rouge_booking_log_2020.csv")
        )
        .pipe(clean_column_names)
        .drop(
            columns=[
                "inmate_first_name",
                "middle_name",
                "last_name",
                "suffix",
                "inmate_zip",
                "inmate_dob",
                "employer",
                "occupation",
                "is_out_of_parish",
                "is_out_of_state",
                "inmate_dl",
            ]
        )
        .rename(
            columns={
                "booking_number": "tracking_id",
                "gender": "citizen_sex",
                "admission_date": "check_in_date",
                "is_doc": "is_department_of_corrections",
            }
        )
        .pipe(clean_race)
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_homeless)
        .pipe(standardize_desc_cols, ["jail_name", "is_weekender", "is_work_release"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/booking_baton_rouge_so_2020.csv"), index=False)
