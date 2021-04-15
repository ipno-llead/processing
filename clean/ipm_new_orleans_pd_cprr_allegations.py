from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.clean import (
    float_to_int_str, clean_sexes, clean_races, standardize_desc_cols, clean_dates
)
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append("../")


def initial_processing():
    df = pd.read_csv(data_file_path(
        "ipm/new_orleans_pd_cprr_allegations_1931-2020.csv"), escapechar="\\")
    df = df.dropna(axis=1, how="all")
    df = df.drop_duplicates()
    return clean_column_names(df)


def combine_citizen_columns(df):
    citizen_cols = [
        'allegation_primary_key', 'citizen_primary_key', 'citizen_sex', 'citizen_race'
    ]
    citizens = df[citizen_cols].dropna(how="all").drop_duplicates(subset=[
        'allegation_primary_key', 'citizen_primary_key'
    ]).set_index(['allegation_primary_key', 'citizen_primary_key'])
    citizen_dict = dict()
    for idx, frame in citizens.groupby(level=0):
        citizen_list = []
        for _, row in frame.iterrows():
            citizen = [
                x for x in [row.citizen_race, row.citizen_sex]
                if x != '' and not x.startswith('unknown')
            ]
            if len(citizen) > 0:
                citizen_list.append(' '.join(citizen))
        sexrace = ', '.join(citizen_list)
        citizen_dict[idx] = sexrace
    df.loc[:, 'citizen'] = df.allegation_primary_key\
        .map(lambda x: citizen_dict.get(x, ''))
    df = df.drop(columns=['citizen_primary_key', 'citizen_sex', 'citizen_race'])\
        .drop_duplicates().reset_index(drop=True)
    return df


def rename_columns(df):
    cols = [
        'pib_control_number', 'incident_type', 'occurred_date',
        'disposition_nopd', 'ocurred_time', 'received_date', 'assigned_unit',
        'assigned_department', 'assigned_division', 'assigned_sub_division_a', 'assigned_sub_division_b',
        'source', 'rule_violation', 'paragraph_violation', 'traffic_stop', 'body_worn_camera_available',
        'app_used', 'citizen_arrested', 'officer_primary_key', 'allegation_primary_key',
        'allegation_finding_oipm', 'allegation', 'allegation_class', 'allegation_created_on',
        'citizen_primary_key', 'citizen_sex', 'citizen_race'
    ]
    df = df[cols].drop_duplicates().dropna(how="all").rename(columns={
        'pib_control_number': 'tracking_number',
        'occurred_date': 'occur_date',
        'disposition_nopd': 'disposition',
        'ocurred_time': 'occur_time',
        'received_date': 'receive_date',
        'source': 'complainant_type',
        'allegation_finding_oipm': 'allegation_finding',
        'allegation_created_on': 'allegation_create_date'
    })

    return df


def drop_rows_without_tracking_number(df):
    return df.dropna(subset=['tracking_number'])


def clean_occur_time(df):
    df.loc[:, 'occur_time'] = df.occur_time.str.replace(r'\.0$', '')
    return df


def clean_trailing_empty_time(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r' 0:00$', '')
    return df


def clean_complainant_type(df):
    df.loc[:, 'complainant_type'] = df.complainant_type.str.lower().str.strip()\
        .str.replace(r' \# \d+$', '').str.replace(r' complain(t|ant)$', '')\
        .str.replace(r'civilian', 'citizen').str.replace(r' offi$', ' office')
    return df


def combine_rule_and_paragraph(df):
    df.loc[:, "charges"] = df.rule_violation.str.cat(df.paragraph_violation, sep='; ')\
        .str.replace(r'^; ', '').str.replace(r'; $', '')
    df = df.drop(columns=['rule_violation', 'paragraph_violation'])
    return df


def assign_agency(df):
    df.loc[:, 'data_production_year'] = '2020'
    df.loc[:, 'agency'] = 'New Orleans PD'
    return df


def clean():
    df = initial_processing()
    return df\
        .pipe(rename_columns)\
        .pipe(drop_rows_without_tracking_number)\
        .pipe(clean_sexes, ['citizen_sex'])\
        .pipe(clean_races, ['citizen_race'])\
        .pipe(combine_citizen_columns)\
        .pipe(standardize_desc_cols, [
            'incident_type', 'disposition', 'rule_violation', 'paragraph_violation',
            'traffic_stop', 'body_worn_camera_available', 'citizen_arrested', 'allegation_finding',
            'allegation', 'allegation_class'
        ])\
        .pipe(float_to_int_str, [
            'officer_primary_key', 'allegation_primary_key'
        ])\
        .pipe(clean_occur_time)\
        .pipe(clean_trailing_empty_time, ['receive_date', 'allegation_create_date'])\
        .pipe(clean_dates, ['receive_date', 'allegation_create_date', 'occur_date'])\
        .pipe(clean_complainant_type)\
        .pipe(combine_rule_and_paragraph)\
        .pipe(assign_agency)\
        .pipe(gen_uid, ['agency', 'tracking_number'], 'complaint_uid')\
        .pipe(gen_uid, ['agency', 'tracking_number', 'officer_primary_key', 'allegation_primary_key'], 'allegation_uid')


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/cprr_new_orleans_pd_1931_2020.csv'), index=False)
