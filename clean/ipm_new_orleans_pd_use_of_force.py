from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    float_to_int_str, standardize_desc_cols, clean_sexes, clean_races
)
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append('../')


def clean_trailing_empty_time(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r' \d:\d\d$', '', regex=True)
    return df


def add_occur_day(df):
    df.loc[:, 'occur_day'] = df.occur_date.str.replace(
        r'^\d+\/(\d+)\/\d+.+', r'\1', regex=True)
    df = df.drop(columns=['occur_date', 'day_of_week'])
    return df


def add_occur_time(df):
    df.loc[:, 'occur_time'] = df.occur_hour.str.zfill(2).str.cat(
        df.occur_minute.str.replace(r'\:.+', '', regex=True), sep=':')
    return df.drop(columns=['occur_hour', 'occur_minute'])


def assign_agency(df):
    df.loc[:, 'data_production_year'] = 2019
    df.loc[:, 'agency'] = 'New Orleans PD'
    return df


def clean():
    df = pd.read_csv(data_file_path(
        'raw/ipm/new_orleans_pd_use_of_force_2012-2019.csv'))
    df = df.dropna(axis=1, how='all')
    df = clean_column_names(df)
    df = df.drop(columns=[
        'deployed_only', 'arc_display_only', 'ds_contact', 'ds_injury', 'proj_contact',
        'proj_injury', 'skin_penetration', 'ds_num', 'air_cart_num', 'cycles_num', 'num_darts_hit',
        'num_darts_used', 'citizen_painted', 'cartridge_attached', 'follow_up_discharge',
        'additional_cartridge', 'open_date', 'bureau', 'assigned_unit', 'division_assignment',
        'unit_assignment', 'working_status', 'shift_details', 'cit_complaint', 'unidentified_officer',
        'field_unit_level', 'length_of_job', 'other_incidents', 'light_condition', 'citizen_num_shots',
        'citizen_inmate', 'citizen_role', 'district_or_division', 'officer_hospital', 'officer_db_primary_key'
    ])
    df = df.rename(columns={
        'fit_number': 'uof_tracking_number',
        'occurred_date': 'occur_date',
        'month_occurred': 'occur_month',
        'year_occurred': 'occur_year',
        'year_reported': 'report_year',
        'description_of_force': 'force_description',
        'uof_effective': 'effective_uof',
        'hour_of_day': 'occur_hour',
        'ocurred_time': 'occur_minute',
        'off_duty': 'officer_off_duty',
        'reason_for_force': 'force_reason',
        'weather_condition': 'weather_description',
        'citizen_build': 'citizen_body_type',
        'officer_age_at_time_of_uof': 'officer_age',
        'officer_years_exp_at_time_of_uof': 'officer_years_exp',
        'received_date': 'receive_date',
        'due_date': 'due_datetime'
    })
    df = df.drop(columns=[
        'officer_sex', 'officer_race', 'incident_type'
    ])
    return df\
        .pipe(float_to_int_str, [
            'officer_primary_key', 'occur_hour', 'citizen_primary_key', 'citizen_age', 'citizen_age_1',
            'officer_current_supervisor', 'officer_age', 'officer_years_exp', 'officer_years_with_unit'
        ])\
        .pipe(standardize_desc_cols, [
            'effective_uof', 'accidental_discharge', 'less_than_lethal', 'day_of_week',
            'traffic_stop', 'sustained', 'force_reason', 'weather_description',
            'body_worn_camera_available', 'app_used', 'citizen_arrested', 'citizen_hospitalized',
            'citizen_injured', 'citizen_body_type', 'citizen_involvement', 'officer_title',
            'officer_injured', 'officer_type', 'officer_employment_status'
        ])\
        .pipe(clean_trailing_empty_time, [
            'receive_date', 'assigned_date', 'completed_date', 'created_date',
        ])\
        .pipe(add_occur_day)\
        .pipe(add_occur_time)\
        .pipe(clean_sexes, ['citizen_sex'])\
        .pipe(clean_races, ['citizen_race'])\
        .drop_duplicates()\
        .pipe(assign_agency)\
        .pipe(gen_uid, [
            'agency', 'uof_tracking_number', 'officer_primary_key', 'citizen_primary_key',
            'force_description', 'force_type', 'force_level', 'effective_uof',
            'accidental_discharge', 'less_than_lethal'
        ], 'uof_uid')\
        .pipe(gen_uid, ['agency', 'citizen_primary_key'], 'citizen_uid')


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/uof_new_orleans_pd_2012_2019.csv'), index=False)
