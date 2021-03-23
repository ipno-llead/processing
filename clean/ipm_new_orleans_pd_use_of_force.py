from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    float_to_int_str, standardize_desc_cols, clean_sexes, clean_races
)
import pandas as pd
import sys
sys.path.append('../')


def clean_trailing_empty_time(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r' 0:00$', '')
    return df


def clean():
    df = pd.read_csv(data_file_path(
        'ipm/new_orleans_pd_use_of_force_2012-2019.csv'))
    df = df.dropna(axis=1, how='all')
    df = clean_column_names(df)
    df = df.rename(columns={
        'occurred_date': 'occur_date',
        'month_occurred': 'occur_month',
        'year_occurred': 'occur_year',
        'year_reported': 'report_year',
        'officer_age_at_time_of_uof': 'officer_age',
        'officer_years_exp_at_time_of_uof': 'officer_years_exp',
        'received_date': 'receive_date',
    })
    df = df.drop(columns=[
        'citizen_primary_key', 'officer_sex', 'officer_race', 'incident_type'
    ])
    return df\
        .pipe(float_to_int_str, [
            'officer_primary_key', 'ds_num', 'air_cart_num', 'cycles_num', 'num_darts_hit',
            'num_darts_used', 'hour_of_day', 'citizen_age', 'citizen_num_shots', 'citizen_age_1',
            'officer_db_primary_key', 'officer_current_supervisor', 'officer_age', 'officer_years_exp',
            'officer_years_with_unit'
        ])\
        .pipe(standardize_desc_cols, [
            'uof_effective', 'accidental_discharge', 'deployed_only', 'arc_display_only',
            'ds_contact', 'ds_injury', 'proj_contact', 'proj_injury', 'skin_penetration',
            'less_than_lethal', 'citizen_painted', 'cartridge_attached', 'follow_up_discharge',
            'additional_cartridge', 'day_of_week', 'cit_complaint', 'unidentified_officer',
            'traffic_stop', 'field_unit_level', 'sustained', 'reason_for_force', 'other_incidents',
            'light_condition', 'weather_condition', 'body_worn_camera_available', 'app_used',
            'citizen_arrested', 'citizen_hospitalized', 'citizen_injured', 'citizen_build',
            'citizen_involvement', 'citizen_inmate', 'officer_title', 'officer_injured', 'officer_hospital',
            'officer_type', 'officer_employment_status'
        ])\
        .pipe(clean_trailing_empty_time, [
            'receive_date', 'occur_date', 'open_date', 'due_date', 'assigned_date', 'completed_date', 'created_date',
        ])\
        .pipe(clean_sexes, ['citizen_sex'])\
        .pipe(clean_races, ['citizen_race'])
