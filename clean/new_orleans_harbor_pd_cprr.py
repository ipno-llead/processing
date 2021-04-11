from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_dates, clean_names, standardize_desc_cols, clean_sexes, clean_races
)
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append("../")


def realign():
    df = pd.read_csv(data_file_path(
        "new_orleans_harbor_pd/new_orleans_harbor_pd_cprr_2014-2020.csv"))
    df = df.set_index("Unnamed: 0").transpose().dropna(
        1, how="all").reset_index(drop=True)
    df = clean_column_names(df)
    return df


def split_name(df):
    names = df.full_name.str.split(" ", expand=True)
    df.loc[:, "first_name"] = names.iloc[:, 0]
    df.loc[:, "last_name"] = names.iloc[:, 1]
    df = df.drop(columns="full_name")
    return df


def strip_badge(df):
    df.loc[:, "badge_no"] = df.badge_no.str.strip(
    ).str.replace(r"\s+\(call sign\)$", "")
    return df


def clean_officer_sex(df):
    df.loc[:, "sex"] = df.sex.str.lower()
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "New Orleans Harbor PD"
    df.loc[:, "data_production_year"] = "2020"
    return df


def clean():
    df = realign()
    df = df[[
        "1_name", "2_badge_number", "3_gender",
        "6_unit_assignment_on_the_date_of_the_complaint_incident", "7_rank_on_the_date_of_the_complaint_incident",
        "8_date_of_appointment", "b_rule_violation", "c_paragraph_violation", "e_the_recommended_discipline",
        "e_the_final_discipline_imposed", "a_the_incident_type", "b_the_complaint_tracking_number",
        "c_the_date_on_which_the_complaint_incident_took_place", "d_the_date_on_which_the_complaint_was_received",
        "e_the_date_on_which_the_complaint_investigation_was_completed", "f_the_classification_of_the_complaint",
        "g_the_status_of_the_investigation", "1_gender", "2_race"]]
    df.columns = [
        "full_name", "badge_no", "sex", "department_desc", "rank_desc", "hire_date", "rule_code", "paragraph_code",
        "recommended_action", "action", "incident_type", "tracking_number", "occur_date", "receive_date",
        "investigation_complete_date", "disposition", "investigation_status", "complainant_sex", "complainant_race"]
    df = df\
        .pipe(split_name)\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(strip_badge)\
        .pipe(clean_officer_sex)\
        .pipe(standardize_desc_cols, [
            "department_desc", "rank_desc", "recommended_action", "action", "incident_type", "disposition",
            "investigation_status"
        ])\
        .pipe(clean_sexes, ["complainant_sex"])\
        .pipe(clean_races, ["complainant_race"])\
        .pipe(clean_dates, ["hire_date", "occur_date", "receive_date", "investigation_complete_date"])\
        .pipe(assign_agency)\
        .pipe(gen_uid, ['agency', 'tracking_number'], 'complaint_uid')
    return df


if __name__ == "__main__":
    df = clean()
    ensure_data_dir("clean")
    df.to_csv(data_file_path("clean/cprr_new_orleans_harbor_pd_2020.csv"))
