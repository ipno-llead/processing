from lib.columns import clean_column_names
import deba
from lib.clean import clean_names, standardize_desc_cols
from lib.uid import gen_uid
import pandas as pd


def assign_agency(df, year):
    agency_dict = {"EJLD": "east-jefferson-levee-pd", "OLDP": "orleans-levee-pd"}
    df.loc[:, "agency"] = df.ejld_oldp.str.strip().map(lambda x: agency_dict.get(x, x))
    df.loc[:, "data_production_year"] = year
    return df.drop(columns=["ejld_oldp"])


def split_name_19(df):
    names = (
        df.officer_s_involved.fillna("")
        .str.strip()
        .str.extract(r"^([^ ]+) (\w+) (\w+)$")
    )
    df.loc[:, "rank_desc"] = (
        names.loc[:, 0]
        .str.lower()
        .str.replace(r"^po$", "officer", regex=True)
        .str.replace(r"^lt\.$", "lieutenant", regex=True)
        .str.replace(r"^sgt\.$", "sergeant", regex=True)
    )
    df.loc[:, "first_name"] = names.loc[:, 1]
    df.loc[:, "last_name"] = names.loc[:, 2]
    return df.drop(columns=["officer_s_involved"])


def split_name_20(df):
    names = df.officer_s_involved.fillna("").str.strip().str.extract(r"^(\w+) (\w+)$")
    df.loc[:, "first_name"] = names.loc[:, 0]
    df.loc[:, "last_name"] = names.loc[:, 1]
    return df.drop(columns=["officer_s_involved"])


def assign_uid(df):
    df.loc[df.first_name != "", "uid"] = gen_uid(
        df[df.first_name != ""], ["agency", "first_name", "last_name"]
    )
    return df


def remove_NA_values(df, cols):
    for col in cols:
        df.loc[:, col] = (
            df[col].str.strip().str.replace(r"^(unk|n\/a)$", "", regex=True, case=False)
        )
    return df


def clean_agency_19(df):
    df.loc[:, "agency"] = df.agency.str.lower().str.strip()\
        .str.replace(r"^harahan pd$", "harahan-pd", regex=True)
    return df

def clean19():
    return (
        pd.read_csv(deba.data("raw/levee_pd/levee_pd_cprr_2019.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "case_no": "tracking_id",
                "reserve_full_time": "employment_status",
                "type_of_complaint_or_allegation": "allegation",
                "assigned_investigator": "investigator",
                "name_of_shift_supervisor_if_handeled_by_shift": "shift_supervisor",
                "internal_external": "complainant_type",
                "date_occurred": "occur_date",
                "date_received_by_iad": "receive_date",
                "date_investigation_started": "investigation_start_date",
                "date_investigation_complete": "investigation_complete_date",
                "action_taken": "action",
            }
        )
        .drop(columns=["complainant"])
        .dropna(subset=["ejld_oldp"])
        .reset_index(drop=True)
        .pipe(assign_agency, 2019)
        .pipe(split_name_19)
        .pipe(clean_agency_19)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            standardize_desc_cols,
            ["employment_status", "disposition", "action", "complainant_type"],
        )
        .pipe(assign_uid)
        .pipe(gen_uid, ["agency", "tracking_id"], "allegation_uid")
        .pipe(
            remove_NA_values,
            [
                "occur_date",
                "receive_date",
                "shift_supervisor",
                "employment_status",
                "action",
            ],
        )
    )


def clean20():
    return (
        pd.read_csv(deba.data("raw/levee_pd/levee_pd_cprr_2020.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "case_no": "tracking_id",
                "reserve_full_time": "employment_status",
                "type_of_complaint_or_allegation": "allegation",
                "assigned_investigator": "investigator",
                "name_of_shift_supervisor_if_handeled_by_shift": "shift_supervisor",
                "internal_external": "complainant_type",
                "date_occurred": "occur_date",
                "date_received_by_iad": "receive_date",
                "date_investigation_started": "investigation_start_date",
                "date_investigation_complete": "investigation_complete_date",
                "action_taken": "action",
            }
        )
        .drop(columns=["complainant"])
        .dropna(subset=["ejld_oldp"])
        .reset_index(drop=True)
        .pipe(assign_agency, 2020)
        .pipe(split_name_20)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            standardize_desc_cols,
            ["employment_status", "disposition", "action", "complainant_type"],
        )
        .pipe(assign_uid)
        .pipe(gen_uid, ["agency", "tracking_id"], "allegation_uid")
        .pipe(remove_NA_values, ["shift_supervisor", "action"])
    )


if __name__ == "__main__":
    df20 = clean20()
    df19 = clean19()
    df = pd.concat([df19, df20])
    df.to_csv(deba.data("clean/cprr_levee_pd.csv"), index=False)
