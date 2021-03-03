from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.clean import (
    float_to_int_str, clean_sexes, clean_races, standardize_desc_cols, clean_datetimes,
    clean_dates, clean_times
)
import pandas as pd
import sys
sys.path.append("../")


def initial_processing():
    df = pd.read_csv(data_file_path(
        "ipm/new_orleans_pd_cprr_allegations_1931-2020.csv"), escapechar="\\")
    df = df.dropna(axis=1, how="all")
    df = df.drop_duplicates()
    return clean_column_names(df)


def split_table(df):
    officer_cols = [
        'officer_primary_key', 'officer_unknown_id', 'officer_sex', 'officer_race',
    ]
    officers = df[officer_cols].drop_duplicates().dropna(how="all")\
        .dropna(subset=["officer_primary_key"])

    citizen_cols = [
        'citizen_primary_key', 'citizen_sex', 'citizen_race'
    ]
    citizens = df[citizen_cols].drop_duplicates().dropna(how="all")\
        .dropna(subset=["citizen_primary_key"])

    complaint_cols = [
        'pib_control_number', 'incident_type', 'occurred_date', 'year_occurred', 'month_occurred',
        'status', 'disposition_nopd', 'day_of_week', 'hour_of_day', 'ocurred_time', 'received_date',
        'open_date', 'due_date', 'assigned_date', 'completed_date', 'created_date', 'assigned_unit',
        'assigned_department', 'assigned_division', 'assigned_sub_division_a', 'assigned_sub_division_b',
        'working_status', 'shift_details', 'priority', 'source', 'service_type', 'rule_violation',
        'paragraph_violation', 'cit_complaint', 'unidentified_officer', 'why_forwarded', 'county',
        'traffic_stop', 'field_unit_level', 'length_of_job', 'sustained', 'body_worn_camera_available',
        'app_used', 'citizen_arrested', 'citizen_involvement'
    ]
    complaints = df[complaint_cols].drop_duplicates().dropna(how="all")\
        .dropna(subset=["pib_control_number"])

    allegation_cols = ['pib_control_number', 'officer_primary_key']+[
        col for col in df.columns
        if col not in officer_cols + citizen_cols + complaint_cols
    ]
    allegations = df[allegation_cols].drop_duplicates().dropna(how="all")\
        .dropna(subset=["allegation_primary_key"])

    allegation_citizens = df[
        ["allegation_primary_key", "citizen_primary_key"]
    ].drop_duplicates().dropna(how="all")

    return officers, citizens, complaints, allegations, allegation_citizens


def clean_officers(officers):
    officers.columns = [
        'officer_primary_key', 'officer_unknown_id', 'sex', 'race']
    return officers\
        .pipe(float_to_int_str, ["officer_primary_key", "officer_unknown_id"])\
        .pipe(clean_sexes, ["sex"])\
        .pipe(clean_races, ["race"])


def clean_citizens(citizens):
    return citizens\
        .pipe(float_to_int_str, ["citizen_primary_key"])\
        .pipe(clean_sexes, ["citizen_sex"])\
        .pipe(clean_races, ["citizen_race"])


def clean_complaints(complaints):
    return complaints\
        .pipe(float_to_int_str, [
            "pib_control_number", 'year_occurred', 'month_occurred', 'hour_of_day'
        ])\
        .pipe(standardize_desc_cols, ['incident_type', 'status', 'disposition_nopd', 'day_of_week'])


def clean_allegations(allegations):
    return allegations\
        .pipe(float_to_int_str, [
            'pib_control_number', 'officer_primary_key', 'allegation_primary_key'
        ])\
        .pipe(standardize_desc_cols, [
            'allegation', 'allegation_finding', 'allegation_final_disposition', 'all_findings',
            'disposition_oipm_by_officer', 'allegation_finding_oipm', 'allegation_1', 'allegation_class',
            'allegation_class_1', 'allegation_directive', 'allegation_alert_processed'
        ])
    # .pipe(clean_dates, ["occur_date", 'allegation_final_disposition_date', 'allegation_alert_processed_date'])\
    # .pipe(clean_datetimes, ['allegation_finding_datetime', 'allegation_created_on_datetime'])\
    # .pipe(clean_times, ["occur_time"])


def clean():
    df = initial_processing()
    officers, citizens, complaints, allegations, allegation_citizens = split_table(
        df
    )
    officers = clean_officers(officers)
    citizens = clean_citizens(citizens)
    complaints = clean_complaints(complaints)
    allegations = clean_allegations(allegations)
    return officers, citizens, complaints, allegations, allegation_citizens
