import pandas as pd
from pandas.io.parsers import read_csv
from lib.clean import clean_datetimes, float_to_int_str, standardize_desc_cols, clean_races
from lib.path import data_file_path
from lib.columns import clean_column_names
from lib.path import data_file_path
from lib.rows import duplicate_row
import re


def clean_citizen_gender(df):
    df.loc[:, 'citizen_gender'] = df.subjectgender.fillna('').str.lower().str.strip()\
        .str.replace('black', '', regex=False)
    return df.drop(columns='subjectgender')


def clean_citizen_eye_color(df):
    df.loc[:, 'citizen_eye_color'] = df.subjecteyecolor.fillna('').str.lower().str.strip()\
        .str.replace('150', '', regex=False)
    return df.drop(columns='subjecteyecolor')


def clean_citizen_driver_license_state(df):
    df.loc[:, 'citizen_driver_license_state'] = df.subjectdriverlicstate.fillna('').str.lower().str.strip()\
        .str.replace('black', '', regex=False)
    return df.drop(columns='subjectdriverlicstate')


def extract_assigned_district(df):
    districts = df.officerassignment.str.lower().str.strip()\
        .str.extract(r'((.+) district)')

    df.loc[:, ['assigned_district']] = districts[0]\
        .str.replace(r'  +', ' ', regex=True)
    return df


def extract_stop_results_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(stop results\: (\w+) ?(\w+)?))')

    df.loc[:, 'stop_results'] = col[0]\
        .str.replace(r'(stop results\: )', '', regex=True)\
        .str.replace(r'^l$', '', regex=True)
    return df


def extract_subject_type_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(subject type\: (\w+)))')

    df.loc[:, 'subject_type'] = col[0]\
        .str.replace(r'subject type\: ', '', regex=True)
    return df


def extract_search_occurred_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(search occurred\: (\w+)))')

    df.loc[:, 'search_occurred'] = col[0]\
        .str.replace(r'search occurred\: ', '', regex=True)
    return df


def extract_search_types_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(search types\: (\w+)?\-? ?(\w+)?\(?(\w{1})?\)?))')

    df.loc[:, 'search_types'] = col[0]\
        .str.replace(r'search types\: ', '', regex=True)
    return df


def extract_evidence_siezed_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(evidence seized\: (\w+)))')

    df.loc[:, 'evidence_seized'] = col[0]\
        .str.replace(r'evidence seized\: ', '', regex=True)
    return df


def extract_evidence_types_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(evidence types\: (\w+) ?(\w+)?\(?(\w{1})?\)?))')

    df.loc[:, 'evidence_types'] = col[0]\
        .str.replace(r'evidence types\: ', '', regex=True)
    return df


def extract_legal_basis_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(legal basises\: (\w+) ?(\w+)? ?(\w+)?))')

    df.loc[:, 'legal_basis'] = col[0]\
        .str.replace(r'legal basises\: ', '', regex=True)
    return df


def extract_consent_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(consent to search\: (\w+)))')

    df.loc[:, 'consent_to_search'] = col[0]\
        .str.replace(r'consent to search\: ', '', regex=True)
    return df


def extract_consent_form_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(consent form completed\: (\w+)))')

    df.loc[:, 'consent_form_completed'] = col[0]\
        .str.replace(r'consent form completed\: ', '', regex=True)
    return df


def extract_body_search_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(stripbody cavity search\: (\w+)))')

    df.loc[:, 'strip_body_cavity_search'] = col[0]\
        .str.replace(r'stripbody cavity search\: ', '', regex=True)
    return df


def extract_exit_vehicle_column(df):
    col = df.actionstaken.str.lower().str.strip()\
        .str.extract(r'(?:(exit vehicle\: (\w+)))')

    df.loc[:, 'exit_vehicle'] = col[0]\
        .str.replace(r'exit vehicle\: ', '', regex=True)
    return df.drop(columns='actionstaken')


def clean_assigned_department(df):
    df.loc[:, 'assigned_department'] = df.officerassignment.str.lower().str.strip()\
        .str.replace('pib', 'public integrity bureau', regex=False)\
        .str.replace('fob', 'field operations bureau', regex=False)\
        .str.replace('msb', 'management services bureau', regex=False)\
        .str.replace('superintendent', 'office of the superintendent', regex=False)\
        .str.replace('isb', 'investigations and support bureau', regex=False)\
        .str.replace(r'((.+) district)', '', regex=True)
    return df.drop(columns='officerassignment')


def consolidate_name_and_badge_columns(df):
    df.loc[:, 'officer1badgenumber'] = df.officer1badgenumber.fillna('').astype(str)\
        .str.replace(r'^(\w+)$', r'(\1)', regex=True)
    df.loc[:, 'officer2badgenumber'] = df.officer2badgenumber.fillna('').astype(str)\
        .str.replace(r'^(\w+)\.(\w+)$', r'(\1)', regex=True)

    df.loc[:, 'officer1name'] = df.officer1name.str.cat(df.officer1badgenumber, sep=' ')
    df.loc[:, 'officer2name'] = df.officer2name.fillna('').str.cat(df.officer2badgenumber, sep=' ')

    df.loc[:, 'officer_names_and_badges'] = df.officer1name + '/' + df.officer2name
    df.loc[:, 'officer_names_and_badges'] = df.officer_names_and_badges.str.lower().str.strip()\
        .str.replace(r'\/$', '', regex=True)\
        .str.replace(r'(\w+)\/(\w+) ', r'\1.\2', regex=True)
    return df.drop(columns=['officer1badgenumber', 'officer2badgenumber', 'officer1name', 'officer2name'])


def split_rows_with_multiple_officers(df):
    df = df.drop('officer_names_and_badges', axis=1).join(df['officer_names_and_badges']\
        .str.split('/', expand=True)\
        .stack()\
        .reset_index(level=1, drop=True).rename('officer_names_and_badges'))\
        .reset_index(drop=True)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/ipm/new_orleans_pd_stop_and_search_2007_2021.csv'))\
        .pipe(clean_column_names)\
        .drop(columns=['subjectage'])\
        .rename(columns={
            'subjectheight': 'citizen_height',
            'subjectweight': 'citizen_weight',
            'subjecthaircolor': 'citizen_hair_color',
            'subjectid': 'citizen_id',
            'subjectrace': 'citizen_race',
            'carnumber': 'vehicle_number',
            'vehicleyear': 'vehicle_year',
            'vehiclemodel': 'vehicle_model',
            'vehiclestyle': 'vehicle_style',
            'vehiclecolor': 'vehicle_color',
            'vehiclemake': 'vehicle_make',
            'eventdate': 'stop_and_search_datetime',
            'zip': 'zip_code',
            'blockaddress': 'stop_and_search_location',
            'stop_and_search__field_interviews__fieldinterviewid': 'stop_and_search_interview_id',
            'fic_officersnames_10_21_2021_fieldinterviewid': 'stop_and_search_interview_id_2',
            'nopd_item': 'item_number',
            'stopdescription': 'stop_reason'
        })\
        .pipe(clean_races, [
            'citizen_race'
        ])\
        .pipe(clean_citizen_gender)\
        .pipe(clean_citizen_eye_color)\
        .pipe(clean_citizen_driver_license_state)\
        .pipe(extract_assigned_district)\
        .pipe(clean_assigned_department)\
        .pipe(extract_stop_results_column)\
        .pipe(extract_subject_type_column)\
        .pipe(extract_search_occurred_column)\
        .pipe(extract_evidence_siezed_column)\
        .pipe(extract_evidence_types_column)\
        .pipe(extract_body_search_column)\
        .pipe(extract_legal_basis_column)\
        .pipe(extract_consent_column)\
        .pipe(extract_consent_form_column)\
        .pipe(extract_search_types_column)\
        .pipe(extract_exit_vehicle_column)\
        .pipe(consolidate_name_and_badge_columns)\
        .pipe(split_rows_with_multiple_officers)\
        .pipe(clean_datetimes, [
            'stop_and_search_datetime'
        ])\
        .pipe(standardize_desc_cols, [
            'vehicle_number', 'vehicle_model', 'vehicle_make', 'zone',
            'stop_and_search_location', 'stop_reason',
            'vehicle_style', 'vehicle_color', 'citizen_hair_color'
        ])\
        .pipe(float_to_int_str, [
            'citizen_height', 'citizen_weight', 'citizen_hair_color',
            'vehicle_year', 'zip_code'
        ])
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/stop_and_search_new_orleans_pd_2007_2021.csv'), index=False)
