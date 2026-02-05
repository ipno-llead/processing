import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, clean_names, clean_races, clean_sexes, standardize_desc_cols
from lib.uid import gen_uid


def drop_invalid_dates(df):
    """Remove rows with placeholder dates like 01/01/1900"""
    df = df[df.occurred_date != "01/01/1900"]
    return df


def clean_citizen_arrested(df):
    df.loc[:, "citizen_arrested"] = (
        df.citizen_arrested
        .str.lower()
        .str.strip()
        .str.replace(r"^yes$", "yes", regex=True)
        .str.replace(r"^no$", "no", regex=True)
    )
    return df


def clean_citizen_injured(df):
    df.loc[:, "citizen_injured"] = (
        df.citizen_injured
        .str.lower()
        .str.strip()
        .str.replace(r"^yes$", "yes", regex=True)
        .str.replace(r"^no$", "no", regex=True)
    )
    return df


def clean_citizen_hospitalized(df):
    df.loc[:, "citizen_hospitalized"] = (
        df.citizen_hospitalized
        .str.lower()
        .str.strip()
        .str.replace(r"^yes$", "yes", regex=True)
        .str.replace(r"^no$", "no", regex=True)
    )
    return df


def clean_citizen_resistance(df):
    """Standardize citizen resistance types"""
    df.loc[:, "citizen_resistance"] = (
        df.citizen_resistance
        .str.lower()
        .str.strip()
        .str.replace(r"pushed / shoved", "pushed/shoved", regex=False)
        .str.replace(r"pushing/shove", "pushed/shoved", regex=False)
        .str.replace(r"att\.", "attempted", regex=True)
    )
    return df


def clean_use_of_force_type(df):
    """Clean force type values"""
    df.loc[:, "use_of_force_type"] = (
        df.use_of_force_type
        .str.lower()
        .str.strip()
        .str.replace(r"o\.c\.", "oc spray", regex=True)
        .str.replace(r"k-9 utilized", "k9", regex=False)
    )
    return df


def clean_citizen_condition(df):
    df.loc[:, "citizen_condition"] = (
        df.citizen_condition
        .str.lower()
        .str.strip()
    )
    return df


def create_tracking_id(df):
    """Create tracking_id from IA No, Item No, and occurred_date"""
    df.loc[:, "tracking_id_og"] = (
        df.ia_no.fillna("").astype(str) + "-" +
        df.item_no.fillna("").astype(str) + "-" +
        df.occurred_year.fillna("").astype(str) + "-" +
        df.occurred_month.fillna("").astype(str) + "-" +
        df.occurred_day.fillna("").astype(str)
    )
    return df


def clean25():
    df = (
        pd.read_csv(deba.data("raw/st_tammany_so/stpso_2025_UOF_Report.csv"), low_memory=False)
        .pipe(clean_column_names)
        .rename(columns={
            "inc_ia_no": "ia_no",
            "inc_item_no": "item_no",
            "inc_occurred_date": "occurred_date",
            "uof_citizen_condition_injury": "citizen_condition",
            "uof_citizen_resistance": "citizen_resistance",
            "uof_citizen_was_arrested_y_n": "citizen_arrested",
            "uof_citizen_was_injured_y_n": "citizen_injured",
            "uof_citizen_went_to_hospital_y_n": "citizen_hospitalized",
            "uof_type_of_force_used": "use_of_force_type",
            "cit_gender": "citizen_sex",
            "cit_race": "citizen_race",
            "emp_gender": "officer_sex",
            "emp_race": "officer_race",
            "emp_last_name": "last_name",
            "emp_first_name": "first_name",
        })
        .drop_duplicates()
        .pipe(drop_invalid_dates)
        .pipe(clean_citizen_arrested)
        .pipe(clean_citizen_injured)
        .pipe(clean_citizen_hospitalized)
        .pipe(clean_citizen_resistance)
        .pipe(clean_use_of_force_type)
        .pipe(clean_citizen_condition)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(clean_sexes, ["citizen_sex", "officer_sex"])
        .pipe(clean_races, ["citizen_race", "officer_race"])
        .pipe(standardize_desc_cols, ["ia_no", "item_no"])
        .pipe(clean_dates, ["occurred_date"])
        .pipe(create_tracking_id)
        .pipe(set_values, {"agency": "st-tammany-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["tracking_id_og", "agency"],
            "tracking_id"
        )
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "citizen_resistance",
                "use_of_force_type",
                "citizen_race",
                "citizen_sex",
            ],
            "uof_uid",
        )
    )

    # Create citizen dataframe with citizen-specific columns
    citizen_df = df[
        [
            "uof_uid",
            "citizen_sex",
            "citizen_race",
            "citizen_arrested",
            "citizen_injured",
            "citizen_hospitalized",
            "citizen_condition",
            "agency",
        ]
    ].pipe(
        gen_uid,
        ["uof_uid", "citizen_sex", "citizen_race", "agency"],
        "citizen_uid",
    ).drop_duplicates()

    # Create UOF dataframe with officer/incident columns
    uof_df = df[
        [
            "ia_no",
            "item_no",
            "tracking_id",
            "tracking_id_og",
            "citizen_resistance",
            "use_of_force_type",
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "first_name",
            "last_name",
            "officer_sex",
            "officer_race",
            "agency",
            "uid",
            "uof_uid",
        ]
    ].drop_duplicates()

    return uof_df, citizen_df


if __name__ == "__main__":
    uof, citizen_uof = clean25()
    uof.to_csv(deba.data("clean/uof_st_tammany_so_2025.csv"), index=False)
    citizen_uof.to_csv(deba.data("clean/uof_cit_st_tammany_so_2025.csv"), index=False)
