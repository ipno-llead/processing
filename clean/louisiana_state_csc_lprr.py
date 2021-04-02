from lib.clean import clean_dates, standardize_desc_cols, clean_names
from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
import pandas as pd
import re
import sys
sys.path.append("../")


def standardize_appealed(df):
    df.loc[:, "appealed"] = df.appealed.str.strip()
    return df


def split_appellant(df):
    indices_to_remove = list()
    for idx, row in df.loc[
            df.appellant.str.contains('/') |
            df.appellant.str.contains('-')].iterrows():
        if '/' in row.appellant:
            names = row.appellant.strip().split('/')
        elif '-' in row.appellant:
            names = row.appellant.strip().split('-')
        indices_to_remove.append(idx)
        for name in names:
            row.appellant = name
            df = df.append(row, ignore_index=True)
    df = df.drop(index=indices_to_remove)
    df.loc[:, 'appellant'] = df.appellant.str.replace(r'^Matte$', r'John Matte')\
        .str.replace(r'^Perry$', r'Jesse Perry')\
        .str.replace(r'^Cook$', r'Louis Cook')\
        .str.replace(r'^William$', r'Kenneth Williams')
    return df.reset_index(drop=True)


def clean_appellant(df):
    df.loc[:, 'appellant'] = df.appellant.str.replace(r', ', ' ')\
        .str.strip().str.replace(r"D'Berry Jr\. Reuben", 'Berry Jr. Reuben O.')

    def split_name(val):
        m = re.match(r'^(.+ Jr\.) (.+)$', val)
        if m is not None:
            return m.group(2), m.group(1)
        m = re.match(r'^(.+ [A-Z]\.) (.+)$', val)
        if m is not None:
            return m.group(1), m.group(2)
        m = re.match(r'^([\w\.]+) (\w+)', val)
        if m is not None:
            return m.group(2), m.group(1)
        m = re.match(r'^\w+$', val)
        if m is not None:
            return "", val
        raise ValueError('unhandled format %s' % val)

    names = pd.DataFrame.from_records(
        df.appellant.map(split_name)
    )
    df.loc[:, 'last_name'] = names.iloc[:, 1]

    names = names.iloc[:, 0].str.split(' ', expand=True)
    df.loc[:, 'first_name'] = names.iloc[:, 0]
    df.loc[:, 'middle_initial'] = names.iloc[:, 1]
    return df


def clean_docket_no(df):
    df.loc[:, 'docket_no'] = df.docket_no.str.strip()\
        .str.replace(r'- ', '-')

    # split rows with 2 docket numbers
    indices_to_remove = []
    for idx, row in df.loc[df.docket_no.str.contains(r' ')].iterrows():
        docket_nos = row.docket_no.split(' ')
        filed_dates = row.filed_date.split(' ')
        for i, docket_no in enumerate(docket_nos):
            row.docket_no = docket_no
            row.filed_date = filed_dates[i]
            df = df.append(row, ignore_index=True)
        indices_to_remove.append(idx)
    df = df.drop(index=indices_to_remove)

    return df.reset_index(drop=True)


def clean_decision(df):
    df.loc[:, 'decision'] = df.decision.str.strip().str.lower()\
        .str.replace(r'dnied', 'denied')\
        .str.replace(r'denited', 'denied')\
        .str.replace(r'ganted', 'granted')\
        .str.replace(r'settlemenet', 'settlement')\
        .str.replace(r'(\w)- ', r'\1 - ')
    return df


def assign_agency(df):
    df.loc[:, 'agency'] = 'Louisiana State Police'
    df.loc[:, 'data_production_year'] = 2020
    return df


def clean():
    df = pd.read_csv(data_file_path(
        "louisiana_state_csc/louisianastate_csc_lprr_1991-2020.csv"))
    df = clean_column_names(df)
    df = df.rename(columns={
        'docket': 'docket_no',
        'apellant': 'appellant',
        'colonel': 'charging_supervisor',
        'filed': 'filed_date',
        'rendered': 'rendered_date'
    })
    df = df.drop(columns=['delay'])
    return df\
        .pipe(standardize_appealed)\
        .pipe(split_appellant)\
        .pipe(clean_appellant)\
        .pipe(clean_names, ['first_name', 'middle_initial', 'last_name'])\
        .pipe(clean_docket_no)\
        .pipe(clean_dates, ['filed_date', 'rendered_date'])\
        .pipe(clean_decision)\
        .pipe(assign_agency)\
        .pipe(gen_uid, ['agency', 'first_name', 'middle_initial', 'last_name'])


if __name__ == "__main__":
    df = clean()
    ensure_data_dir("clean")
    df.to_csv(data_file_path(
        "clean/lprr_louisiana_state_csc_1991_2020.csv"), index=False)
