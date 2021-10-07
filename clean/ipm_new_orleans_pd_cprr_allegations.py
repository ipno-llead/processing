from pandas.io.parsers import read_csv
from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.clean import (
    float_to_int_str, clean_sexes, clean_races, remove_future_dates, standardize_desc_cols, clean_dates
)
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append("../")


def initial_processing():
    df = pd.read_csv(data_file_path(
        "raw/ipm/new_orleans_pd_cprr_allegations_1931-2020.csv"), escapechar="\\")
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


def drop_rows_without_tracking_number(df):
    df = df[df.tracking_number != 'test']
    return df.dropna(subset=['tracking_number']).reset_index(drop=True)


def clean_trailing_empty_time(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r' 0:00$', '', regex=True)
    return df


def clean_complainant_type(df):
    df.loc[:, 'complainant_type'] = df.source.str.lower().str.strip()\
        .str.replace(r' \# \d+$', '', regex=True).str.replace(r' complain(t|ant)$', '', regex=True)\
        .str.replace(r'civilian', 'citizen', regex=True).str.replace(r' offi$', ' office', regex=True)\
        .str.replace('rank', 'nopd employee', regex=False).str.replace('known', '', regex=False)
    return df.drop(columns='source')


def combine_rule_and_paragraph(df):
    df.loc[:, "allegations"] = df.rule_violation.str.cat(df.paragraph_violation, sep='; ')\
        .str.replace(r'^; ', '', regex=True).str.replace(r'; $', '', regex=True)\
        .str.replace(r'\bperf\b', 'performance', regex=True).str.replace('prof', 'professional', regex=False)\
        .str.replace('dept', 'department', regex=False).str.replace(r'-(\w+)', r'- \1', regex=True)\
        .str.replace(r'no violation observed;? '
                     r'?(no violation was observed to have been committed)?|'
                     r'no alligations assigned at this time', '', regex=True)\
        .str.replace(r'\binfo\b', 'information', regex=True).str.replace('mobil', 'mobile', regex=False)\
        .str.replace('drugs off- duty', 'drugs while off duty', regex=False)\
        .str.replace(', giving', ' or giving', regex=False)\
        .str.replace('professionalessionalism', 'professionalism', regex=False)\
        .str.replace(r'^policy$', '', regex=True).str.replace('perform before', 'perform duty before', regex=False)\
        .str.replace('use of alcohol / on- duty', 'use of alcohol while on duty', regex=False)\
        .str.replace(r'(\w+) / (\w+)', r'\1/\2', regex=True)\
        .str.replace('department property', 'department equipment', regex=False)\
        .str.replace('civil service rules; rule v, section 9 - to wit paragraph 1 and paragraph 18', '', regex=False)\
        .str.replace('paragraph 05 - ceasing to perform duty before end of shift',
                     'paragraph 03 - devoting entire time to duty', regex=False)\
        .str.replace('rest activities', 'restricted activities', regex=False)\
        .str.replace('from authoritative', 'from an authoritative', regex=False)\
        .str.replace('paprgraph', 'paragraph', regex=False)\
        .str.replace('digital mobile video audio recording', 'digital, mobile, video, or audio recording', regex=False)\
        .str.replace(r'\bparagraph 1\b', 'paragraph 01', regex=True)\
        .str.replace('policy;', 'policy:', regex=False)
    df = df.drop(columns=['rule_violation', 'paragraph_violation'])
    return df


def clean_charges(df):
    df.loc[:, 'charges'] = df.allegation.str.lower().str.strip()\
        .str.replace('no violation was observed to have been committed by officer/employee',
                     '', regex=False)\
        .str.replace(r'social networking(.+)', 'social networking', regex=True)\
        .str.replace(r'(\d+)-(\w+)', r'\1 - \2', regex=True)\
        .str.replace(r'(\w+) / (\w+)', r'\1/\2', regex=True)\
        .str.replace('paragraph 09 - use of alcohol/on-duty', 
                     'paragraph 08 - use of alcohol/drugs while on duty', regex=False)\
        .str.replace(r'(paragraph 10 - )? ?use of alcohol/(drugs)? ?off-duty',
                     'paragraph 09 - use of alcohol/drugs while off duty', regex=True)\
        .str.replace(r'n\.o\.', 'new orleans', regex=True)\
        .str.replace(r'para\.', 'paragraph', regex=True)\
        .str.replace('equipment', 'property', regex=False)\
        .str.replace('paragraph 03 - cleanliness of department property',
                     'paragraph 03 - cleanliness of department vehicles', regex=False)\
        .str.replace('paragraph 02 - authorized operator of department property',
                     'paragraph 02 - authorized operator of department vehicle', regex=False)\
        .str.replace('paragraph 14 - social networking',
                     'paragraph 13 - social networking', regex=False)\
        .str.replace('paragraph 04,05 - accepting, giving anything of value',
                     'paragraph 04 - accepting, giving anything of value', regex=False)\
        .str.replace('paragraph 11 - interfering with investigations',
                     'paragraph 13 - interfering with investigations', regex=False)\
        .str.replace('no violation was observed to have been committed by officer/employee', 
                     '', regex=False)\
        .str.replace(r'paragraph 12,?1?3? - use of t[oa]bacco', 'paragraph 11 - use of tobacco', regex=True)\
        .str.replace(r'(^paragraph 02 - abuse of position$|^paragraph 01 - professionalism$|'
                     r'paragraph 09 - use of alcohol/drugs while off duty| paragraph 01 - professionalism|'
                     r'paragraph 03 - neatness and attire|paragraph 04 - accepting, giving anything of value|'
                     r'paragraph 05 - referrals|paragraph 06 - commercial endorsement|'
                     r'paragraph 07 - use of drugs/substance abuse testing|'
                     r'paragraph 08 - use of alcohol/drugs while on duty|paragraph 10 - alcohol/drugs influence test|'
                     r'paragraph 11 - use of t[ao]bacco|paragraph 12 - retaliation|paragraph 13 - social networking)', 
                     r'rule 03: moral conduct; \1', regex=True)\
        .str.replace(r'(paragraph 01 - ahherance to law|paragraph 02 - courtesty|'
                     r'paragraph 03 - honesty and truthfulness|paragraph 04 - discrimination|'
                     r'paragraph 05 - verbal intimidation|paragraph 06 - u?n?authorized force|'
                     r'paragraph 07 - courage|paragraph 08 - failure to report misconduct|'
                     r'paragraph 09 - failure to cooperate/withholding information)',
                     r'rule 02: moral conduct; \1', regex=True)\
        .str.replace(r'(paragraph 01 - reporting for duty|paragraph 02 - instructions from an authoritative source|'
                     r'paragraph 03 - devoting entire time to duty|paragraph 04 - neglect of duty|'
                     r'paragraph 05 - ceasing to perform before end of period of duty|'
                     r'paragraph 06 - leaving assigned area|paragraph 07 - leaving city on duty|'
                     r'paragraph 08 - hours of duty|paragraph 09 - safekeeping of valuables by police department|'
                     r'paragraph 10 - escort for valuables or money)',
                     r'rule 04: performance of duty; \1', regex=True)\
        .str.replace(r'(paragraph 01 - ficticious illness or injury|paragraph 02 - associations|'
                     r'paragraph 03 - visiting prohibited establishments|paragraph 04 - subversive activities|'
                     r'paragraph 05 - labor activity|paragraph 06 - acting in civil matters|'
                     r'paragraph 07 - acting impartially|paragraph 08 - civil actions involving members|'
                     r'paragraph 09 - criminal proceeding against member|'
                     r'paragraph 10 - testifying on behalf of defendent(s)|'
                     r'paragraph 11 - tracking of actions by pib|paragraph 12 - disposition documentation|'
                     r'paragraph 13 - interfering with investigations|paragraph 14 - undercover investigations|'
                     r'paragraph 15 - rewards)', r'rule 05: restricted activities; \1', regex=True)\
        .str.replace(r'(paragraph 01 - security of records|paragraph 01 - false or inaccurate reports|'
                     r'paragraph 03 - statements and appearances|paragraph 04 - citizens report complaint|'
                     r'paragraph 05 - informants|paragraph 06 - confidentiality of internal investigations)', 
                     r'rule 06: offical information; \1', regex=True)\
        .str.replace(r'(paragraph 01 - use of department property|'
                     r'paragraph 02 - authorized operator of department vehicle|'
                     r'paragraph 03 - cleanliness of department vehicles|paragraph 04 - use of emergency equipment|'
                     r'paragraph 05 - statement of responsibility|paragraph 06 - operation manuals|'
                     r'paragrpah 07 - surrending department property)', r'rule 07: department property; \1', regex=True)\
        .str.replace(r'(paragraph 0?1 - rules of procedures|paragraph 0?2 - effective|'
                     r'paragraph 03 - violations)', r'rule 01: operation manuals; \1', regex=True)
    return df


def assign_agency(df):
    df.loc[:, 'data_production_year'] = '2020'
    df.loc[:, 'agency'] = 'New Orleans PD'
    return df


def discard_allegations_with_same_description(df):
    finding_cat = pd.CategoricalDtype(categories=[
        'sustained',
        'exonerated',
        'illegitimate outcome',
        'counseling',
        'mediation',
        'not sustained',
        'unfounded',
        'no investigation merited',
        'pending'
    ], ordered=True)
    df.loc[:, 'allegation_finding'] = df.allegation_finding.replace({
        'di-2': 'counseling',
        'nfim': 'no investigation merited'
    }).astype(finding_cat)
    return df.sort_values(['tracking_number', 'complaint_uid', 'allegation_finding'])\
        .drop_duplicates(subset=['complaint_uid'], keep='first')\
        .reset_index(drop=True)


def remove_rows_with_conflicting_disposition(df):
    df.loc[df.allegation_finding == 'sustained', 'disposition'] = 'sustained'
    return df


def clean_tracking_number(df):
    df.loc[:, 'tracking_number'] = df.tracking_number\
        .str.replace(r'^Rule9-', '', regex=True)
    for idx, row in df.loc[df.tracking_number.str.match(r'^\d{3}-')].iterrows():
        df.loc[idx, 'tracking_number'] = row.allegation_create_year + row.tracking_number[3:]
    return df


def replace_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.replace({
        'di-2': 'counseling',
        'nfim': 'no investigation merited'
    })
    return df


def clean_investigating_unit(df):
    df.loc[:, 'investigating_unit'] = df.assigned_unit.str.lower().str.strip()\
        .str.replace('un-assigned', '', regex=False).str.replace('administrative', 'administration', regex=False)\
        .str.replace(r'serv\.', 'service', regex=True).str.replace(r'fob ?-? ?|igo ?-? ?|', '', regex=True)\
        .str.replace(r'isb ?-? ?|msb ?-? ?', '', regex=True).str.replace(r'invest\.', 'investigative', regex=True)\
        .str.replace(r'indep\.', 'independent', regex=True).str.replace('section', '', regex=False)
    return df.drop(columns='assigned_unit')


def clean():
    df = initial_processing()
    return df\
        .drop(columns=[
            'all_findings', 'allegation_1', 'allegation_alert_processed', 'allegation_alert_processed_date',
            'allegation_class_1', 'allegation_directive', 'allegation_final_disposition',
            'allegation_final_disposition_date', 'allegation_finding', 'allegation_finding_date', 'assigned_date',
            'citizen_age', 'citizen_num_shots', 'completed_date', 'county',
            'created_date', 'day_of_week', 'disposition_nopd', 'due_date', 'field_unit_level',
            'hour_of_day', 'is_anonymous', 'length_of_job', 'month_occurred', 'officer_age_at_time_of_uof',
            'officer_badge_number', 'officer_current_supervisor', 'officer_department', 'officer_division',
            'officer_employment_status', 'officer_race', 'officer_sex', 'officer_sub_division_a',
            'officer_sub_division_b', 'officer_title', 'officer_type', 'officer_unknown_id',
            'officer_years_exp_at_time_of_uof', 'officer_years_with_unit', 'open_date', 'priority', 'service_type',
            'shift_details', 'status', 'sustained', 'unidentified_officer', 'why_forwarded', 'working_status',
            'year_occurred', 'citizen_involvement', 'allegation_class', 'cit_complaint', 'incident_type',
            'ocurred_time'
        ])\
        .drop_duplicates()\
        .dropna(how="all")\
        .rename(columns={
            'pib_control_number': 'tracking_number',
            'occurred_date': 'occur_date',
            'disposition_oipm_by_officer': 'disposition',
            'received_date': 'receive_date',
            'allegation_finding_oipm': 'allegation_finding',
            'allegation_created_on': 'allegation_create_date',
        })\
        .pipe(clean_charges)\
        .pipe(drop_rows_without_tracking_number)\
        .pipe(clean_sexes, ['citizen_sex'])\
        .pipe(clean_races, ['citizen_race'])\
        .pipe(combine_citizen_columns)\
        .pipe(standardize_desc_cols, [
            'disposition', 'rule_violation', 'paragraph_violation',
            'traffic_stop', 'body_worn_camera_available', 'citizen_arrested', 'allegation_finding'
        ])\
        .pipe(float_to_int_str, [
            'officer_primary_key', 'allegation_primary_key'
        ])\
        .pipe(clean_trailing_empty_time, ['receive_date', 'allegation_create_date'])\
        .pipe(clean_dates, ['receive_date', 'allegation_create_date', 'occur_date'])\
        .pipe(clean_tracking_number)\
        .pipe(clean_complainant_type)\
        .pipe(combine_rule_and_paragraph)\
        .pipe(clean_investigating_unit)\
        .pipe(assign_agency)\
        .pipe(gen_uid, [
            'agency', 'tracking_number', 'officer_primary_key', 'allegation'
        ], 'complaint_uid')\
        .pipe(discard_allegations_with_same_description)\
        .pipe(replace_disposition)\
        .pipe(remove_future_dates, '2020-12-31', ['receive', 'allegation_create', 'occur'])


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path(
        'clean/cprr_new_orleans_pd_1931_2020.csv'), index=False)
