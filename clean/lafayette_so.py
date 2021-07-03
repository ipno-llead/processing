import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.clean import clean_dates, standardize_desc_cols

def clean_names(df):
    df.name = df.name.str.lower().str.strip().str.replace(',', '').fillna('')\
        .str.replace('auzennejames', 'auzenne james', regex=False)\
        .str.replace('st.cyrneil', 'st.cyr neil', regex=False)\
        .str.replace('anceletjordan', 'ancelet jordan', regex=False)\
        .str.replace('suarezrichard', 'suarez richard')\
        .str.replace('file for records', '', regex=False)
    parts = df.name.str.extract(r'(?:(\w+|\w+\.\w+) )?(?:([^ ]+) )?(.*)')
    df.loc[:, 'last_name'] = parts[0]
    df.loc[:, 'middle_name'] = parts[1]
    df.loc[:, 'first_name'] = parts[2]
    return df.drop(columns='name')

def clean_complaint(df):
    df.loc[:, 'charges'] = df.complaint.str.lower().str.strip()\
        .str.replace('with drew', 'withdrew', regex=False)
    return df


def clean_disposition(df):
    df.disposition = df.disposition.str.lower().str.strip()\
        .str.replace('non-sustained', 'unsustained', regex=False)\
        .str.replace(r'^policy fail$', 'policy failure', regex=True)
    return df

def clean_action(df):
    df.loc[:, 'action'] = df.leave.str.lower().str.strip()\
        .str.replace(r'l?e?t\.', '', regex=True)\
        .str.replace('n/a', '', regex=False)\
        .str.replace(r' ?\.? ?rep', 'reprimand', regex=True)\
        .str.replace('verbalreprimand', 'verbal reprimand', regex=False)\
        .str.replace(r'^rem\.?(edial)? ?(train)?', 'remedial training', regex=True)\
        .str.replace('suspension', 'suspended', regex=False)\
        .str.replace('termination', 'terminated', regex=False)\
        .str.replace(r'^ ?counsel', 'counsel', regex=True)
    return df.drop(columns='leave')


def clean():
    df = pd.read_csv(data_file_path('lafayette_so/lafayette_so_cprr_2015_2020.csv'))\
        .pipe(clean_column_names)
    df = df\
        .rename(columns={
            'case': 'tracking_number',
            'date': 'receive_date',
        })\
        .pipe(clean_names)\
        .pipe(clean_dates, ['receive_date'])\
        .pipe(clean_complaint)\
        .pipe(standardize_desc_cols, ['complete', 'emp_assign'])\
        .pipe(clean_disposition)\
        .pipe(clean_action)
    return df 


if __name__ == "__main__":
    df = clean()
    ensure_data_dir("clean")