import sys
sys.path.append('../')
from lib.path import data_file_path
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import clean_dates
from lib.rows import duplicate_row
import re
from lib.uid import gen_uid


def extract_receive_date(df):
    dates = df.atic_n_date_iad_numbes\
        .str.extract(r'(\d+\/\d+\/\d+)')
    df.loc[:, 'receive_date'] = dates[0]
    return df


def clean_tracking_number(df):
    df.loc[:, 'tracking_number'] = df.atic_n_date_iad_numbes\
        .str.replace(r'(.+) (20-.+) ', r'\2', regex=True)
    return df.drop(columns='atic_n_date_iad_numbes')


def clean_complaint_type(df):
    df.loc[:, 'complaint_type'] = df.chassification.str.lower().str.strip()\
        .str.replace(r'dep[ea]rtmenta?[lds]\!?', 'departmental', regex=True)\
        .str.replace(r'informa[tdl]c?', 'informal', regex=True)
    return df.drop(columns='chassification')


def clean_charges(df):
    df.loc[:, 'charges'] = df.type_complaint.str.lower().str.strip()\
        .str.replace(r'^coda', 'code', regex=True)\
        .str.replace(r'^derefiction', 'dereliction', regex=True)\
        .str.replace('pursuit policy violation', 'vehicle pursuit', regex=False)\
        .str.replace(r'^st[ea]ndard[as] of c[ao]nduct$', 'standards of conduct', regex=True)\
        .str.replace(r'hand[ae]uft?ing', 'handcuffing', regex=True)\
        .str.replace(r'\, ', '/', regex=True)\
        .str.replace('standards of conduct dereliction', 'standards of conduct/dereliction of duty', regex=False)\
        .str.replace('discrimination/haras ment', 'discrimination and harassment')
    return df.drop(columns='type_complaint')


def split_rows_with_multiple_charges(df):
    i = 0
    for idx in df[df.charges.str.contains(r'/')].index:
        s = df.loc[idx + i, 'charges']
        parts = re.split(r'\s*(?:\/)\s*', s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, 'charges'] = name
        i += len(parts) - 1
    return df


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip()\
        .str.replace(r'sus[ti]m?i?a?l?ned', 'sustained', regex=True)\
        .str.replace(r'(\w+)- ?sustained', 'not sustained', regex=True)\
        .str.replace(r'(none|n\/a)', '', regex=True)\
        .str.replace(r'unf ?oun[dt][ea]d', 'unfounded', regex=True)\
        .str.replace(r'syst ?em fai?tur[ea]', 'system failure', regex=True)\
        .str.replace('misconduel', 'misconduct', regex=False)\
        .str.replace('exortersted', 'exonerated', regex=False)\
        .str.replace(r'(\, |\/)', '|', regex=True)
    return df

# dispositions seem to be contradictory. do I drop the least final? probably. like ipm_nopd


def clean_actions(df):
    df.loc[:, 'action'] = df.action_taken.str.lower().str.strip()\
        .str.replace(r'(unfounded|not sustained|[hn]one|exonderated)', '', regex=True)\
        .str.replace(r'declined .+', '', regex=True)\
        .str.replace(r'decin?[nm]ed', 'declined', regex=True)\
        .str.replace('termination', 'terminated', regex=False)\
        .str.replace('worlsy', 'worley', regex=False)\
        .str.replace('los', 'loss of seniority', regex=False)\
        .str.replace('lop', 'loss of pay', regex=False)\
        .str.replace('day suspension loss of senioritys of senority and pay',
                     '5 day suspension|loss of senority|loss of pay')\
        .str.replace('5 day suspension loss of senioritys x2 of seniority',
                     '5 day suspension|loss of seniority', regex=False)\
        .str.replace(r'\, ', '|', regex=True)
    return df.drop(columns='action_taken')


def clean_investigating_supervisor(df):
    df.loc[:, 'investigating_supervisor'] = df.investigatodate.str.lower().str.strip().fillna('')\
        .str.replace('n/a', '', regex=False)\
        .str.replace('meck', 'mack', regex=False)
    return df.drop(columns='investigatodate')


def split_officer_name(df):
    df.loc[:, 'officer'] = df.officer.str.lower().str.strip()\
        .str.replace(r'delectives/officer \$', '', regex=True)\
        .str.replace(r'( \$|0|unknawn)', '', regex=True)\
        .str.replace(r'\.', ',', regex=True)
    names = df.officer.str.extract(r'(\w+),? ?(\w+)?')
    df.loc[:, 'last_name'] = names[0]
    df.loc[:, 'first_name'] = names[1]
    return df


def split_rows_with_multiple_other_officers(df):
    df.loc[:, 'other_officers'] = df.other_officers.str.lower().str.strip().fillna('')\
        .str.replace('; ', '/', regex=False)\
        .str.replace(r'\.', ',', regex=True)\
        .str.replace('jeter, fanning, achanfer, gallon, estess, willams',
                     'jeter, fanning/achanfer, gallon/estess, willams', regex=False)
    
    i = 0
    for idx in df[df.other_officers.str.contains(r'/')].index:
        s = df.loc[idx + i, 'other_officers']
        parts = re.split(r'\s*(?:\/)\s*', s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, 'other_officers'] = name
        i += len(parts) - 1
    return df


def assign_agency(df):
    df.loc[:, 'agency'] = 'Bossier City PD'
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/bossier_city_pd/bossier_city_pd_cprr_2020.csv'))\
        .pipe(clean_column_names)\
        .rename(columns={
            'assigned': 'investigation_start_date',
            'date_returned': 'investigation_complete_date'
        })\
        .pipe(extract_receive_date)\
        .pipe(clean_tracking_number)\
        .pipe(clean_complaint_type)\
        .pipe(clean_charges)\
        .pipe(split_rows_with_multiple_charges)\
        .pipe(clean_disposition)\
        .pipe(clean_actions)\
        .pipe(clean_investigating_supervisor)\
        .pipe(split_officer_name)\
        .pipe(assign_agency)\
        .pipe(clean_dates, ['receive_date', 'investigation_start_date', 'investigation_complete_date'])\
        .pipe(gen_uid, ['first_name', 'last_name', 'agency'])\
        .pipe(gen_uid, ['uid', 'charges', 'disposition', 'action'], 'complaint_uid')
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path(
        'clean/cprr_bossier_city_pd_2020.csv'), index=False)
