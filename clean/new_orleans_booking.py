import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names
from lib.clean import clean_sexes, float_to_int_str


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip().fillna('')\
        .str.replace('susp sent', 'suspended sentence', regex=False)\
        .str.replace('disposition', '', regex=False)\
        .str.replace(r'rel$', 'release', regex=True)\
        .str.replace('ror', 'released on own recognizance', regex=False)\
        .str.replace(r'rev$', '', regex=True)
    return df


def clean_court_type(df):
    df.loc[:, 'court_type'] = df.court_type.str.lower().str.strip().fillna('')\
        .str.replace('cdc', 'civil district court', regex=False)\
        .str.replace('mun', 'municipal', regex=False)\
        .str.replace('paroled', '', regex=False)\
        .str.replace('0', '', regex=False)\
        .str.replace('court rel', '', regex=False)
    return df


def clean_court_date(df):
    df.loc[:, 'court_date'] = df.court_date.str.lower()\
        .str.replace(r'(\d+)/(\d+)/(\d+)', r'\2/\3/\1', regex=True)\
        .str.replace('1/14/2', '', regex=False)\
        .str.replace(r'^(\w+)$', '', regex=True)\
        .str.replace(r'^(\d+)$', '', regex=True)
    return df


def clean_sentence_date(df):
    df.loc[:, 'sentence_date'] = df.sentence_date.str.lower()\
        .str.replace(r'(\d+)/(\d+)/(\d+)', r'\2/\3/\1')\
        .str.replace(r'^(\d+)$', '', regex=True)\
        .str.replace(r'(\d+)/(\d{4})/(\d+)', r'\3/\1/\2', regex=True)
    return df


def clean_check_in_date(df):
    df.loc[:, 'check_in_date'] = df.check_in_date\
        .str.replace(r'(\d+)/(\d+)/(\d+)', r'\2/\3/\1', regex=True)
    return df


def clean_court_time(df):
    df.loc[:, 'am_pm'] = df.court_time_flag_am_pm.str.lower().fillna('')\
        .str.replace(r'8?0?0', '', regex=True)\
        .str.replace('seventh district', '', regex=False)

    df.loc[:, 'court_time'] = df.court_time.str.lower().fillna('')\
        .str.replace('third district', '', regex=False)\
        .str.replace('police attorney', '', regex=False)\
        .str.replace("criminal sheriff's office", '', regex=False)\
        .str.replace('17/08/03', '', regex=False)\
        .str.replace('first district', '', regex=False)\
        .str.replace(r'^0$', '', regex=True)\
        .str.replace(r'^(\d{1})$', r'\1:00', regex=True)\
        .str.replace(r'^(\d{1})(\d{1})(\d{1})$', r'\1:\2\3', regex=True)\
        .str.replace(r'^(\d{1})(\d{1})(\d{1})(\d{1})$', r'\1\2:\3\4', regex=True)

    df.loc[:, 'court_time'] = df.court_time.str.cat(df.am_pm)\
        .str.replace(r'^(\w+)$', '', regex=True)
    return df.drop(columns=['court_time_flag_am_pm', 'am_pm'])


def clean_release_reason(df):
    df.loc[:, 'release_reason'] = df.release_reason.str.lower().str.strip()\
        .str.replace(r'  +', ' ', regex=True)\
        .str.replace(r'\bror\b', 'released on ones own recognizance', regex=True)\
        .str.replace('inputted twice', 'duplicate', regex=False)\
        .str.replace('rel ', 'released ', regex=False)\
        .str.replace(' juv ct', ' juvenile court', regex=False)\
        .str.replace(r'^ret ', 'returned ', regex=False)\
        .str.replace(r' (\w{4}) depu?t?y?', r'\1 to deputy', regex=True)\
        .str.replace('jpso', "jefferson parish sheriff's office", regex=False)\
        .str.replace('sbso', "st. bernard sheriff's office", regex=False)\
        .str.replace('scso', "st. charles sheriff's office", regex=False)
    return df


def convert_bond_amount_to_dollars(df):
    df.loc[:, 'bond_amount'] = df.bond_amount.astype(int).map('{:,d}'.format)
    return df


def clean_charge_description(df):
    df.loc[:, 'charges'] = df.charge_description.str.lower().str.strip()\
        .str.replace('officr', 'officer', regex=False)\
        .str.replace('jpso', "jefferson parish sheriff's office", regex=False)
    return df.drop(columns='charge_description')


def clean_and_join_sentence_types(df):
    df.loc[:, 'sentence_other'] = df.sentence_other.str.lower().str.strip().fillna('')\
        .str.replace(r'rel$', 'release', regex=True)\
        .str.replace(r' ?0.?0?', '', regex=True)

    df.loc[:, 'sentence_type'] = df.sentence_other + '' + df.sentence_type.fillna('').astype(str)\
        .str.replace(r'\.', '', regex=True)\
        .str.replace(r'(\d+)', '', regex=True)
    return df.drop(columns='sentence_other')


def clean_arresting_department(df):
    df.loc[:, 'department_desc'] = df.arrest_credit_txt.str.lower().str.strip().fillna('')\
        .str.replace(r'\.', '', regex=True)\
        .str.replace(' section', '', regex=False)\
        .str.replace('sod', 'special operations division', regex=False)\
        .str.replace(r' police$', '', regex=True)\
        .str.replace(r' divi?s?i?i??o?n?', '', regex=True)\
        .str.replace(r'other ?(non)? ?nopd ?(units)?', '', regex=True)\
        .str.replace(r' \(juvdiv\)', '', regex=True)\
        .str.replace(' squad', '', regex=False)\
        .str.replace(r'^la\b', 'louisiana', regex=True)\
        .str.replace(' unit', '', regex=False)\
        .str.replace(r'(\d+)', '', regex=True)\
        .str.replace('bureau', '', regex=False)\
        .str.replace("district attorney's office", 'district attorney', regex=False)\
        .str.replace('grant: ', '', regex=False)\
        .str.replace(r'^atf ', 'alcohol, tobacco, firearms and explosives ', regex=True)\
        .str.replace(r'^environmental rangers = dept of sanitatn$', 'sanitation', regex=True)\
        .str.replace('narc rock-a-buy', 'narcotics', regex=False)\
        .str.replace(r' tact?k?', ' task force', regex=False)\
        .str.replace('defelopment', 'development', regex=False)\
        .str.replace('scid  crime lab', 'scientific criminal investigations', regex=False)\
        .str.replace(' personsion', ' persons', regex=False)\
        .str.replace('department of ', '', regex=False)
    return df.drop(columns='arrest_credit_txt')


def clean_race(df):
    df.loc[:, 'citizen_race'] = df.race.str.lower().str.strip()\
        .str.replace(r'^a$', 'asian', regex=True).str.replace(r'^b$', 'black', regex=True)\
        .str.replace(r'^i$', 'asian / pacific islander', regex=True)\
        .str.replace(r'^h$', 'hispanic', regex=True).str.replace(r'^w$', 'white', regex=True)\
        .str.replace('u', '', regex=False)
    return df.drop(columns='race')


def clean():
    dfa = pd.read_csv(data_file_path('raw/ipm/new_orleans_so_charges_2015_2021.csv'))\
        .pipe(clean_column_names)
    dfb = pd.read_csv(data_file_path('raw/ipm/new_orleans_so_bookings_2015_2021.csv'))\
        .pipe(clean_column_names)

    df = pd.merge(dfa, dfb, on='folder_no', how='outer')\
        .drop(columns=[
            'bond_amt', 'subp', 'fel_mis_city_traf', 'category',
            'counts', 'ucr_code', 'arrest_credit', 'check_in_time',
            'agency', 'court_type', 'court_section', 'arrest_district',
            'ccn', 'statute', 'violation', 'reason_at_booking'])\
        .rename(columns={
            'sentence_yrs': 'sentence_years',
            'sentence_mos': 'sentence_months',
            'sentence_dys': 'sentence_days',
            'sentence_oth': 'sentence_other',
            'sex': 'citizen_sex',
            'folder_no': 'tracking_number'
        })\
        .pipe(clean_disposition)\
        .pipe(clean_court_time)\
        .pipe(float_to_int_str, ['arrest_credit'])\
        .pipe(clean_court_date)\
        .pipe(clean_sentence_date)\
        .pipe(clean_check_in_date)\
        .pipe(clean_sexes, ['citizen_sex'])\
        .pipe(convert_bond_amount_to_dollars)\
        .pipe(clean_charge_description)\
        .pipe(clean_and_join_sentence_types)\
        .pipe(clean_release_reason)\
        .pipe(clean_arresting_department)\
        .pipe(clean_race)
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path(
        'clean/booking_new_orleans_so_2015_2021.csv'), index=False)
