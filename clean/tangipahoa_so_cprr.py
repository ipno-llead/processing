import sys
sys.path.append('../')
from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import (
    clean_names, standardize_desc_cols, clean_dates
)
import pandas as pd


def split_rows_with_name(df):
    for idx, row in df[df.full_name.fillna('').str.contains(' & ')].iterrows():
        names = row.full_name.split(' & ')
        df.loc[idx, 'full_name'] = names[0]
        row = row.copy()
        row.loc['full_name'] = names[1]
        df = df.append(row)
    return df


def split_full_name(df):
    df.loc[:, 'full_name'] = df.full_name.str.lower().str.strip()\
        .str.replace(r'^(unknown|unk|tpso|tp715|facebook comments)$', '', regex=True)\
        .str.replace('.', '', regex=False)\
        .str.replace(r'(\w+), (\w+)', r'\2 \1', regex=True)
    parts = df.full_name.str.extract(r'(?:(dy|sgt|lt|capt) )?([^ ]+) (.+)')
    df.loc[:, 'rank_desc'] = parts[0].replace({
        'dy': 'deputy',
        'sgt': 'sargeant',
        'lt': 'lieutenant',
        'capt': 'captain'
    })
    df.loc[:, 'first_name'] = parts[1].str.strip().str.replace('jessical', 'jessica', regex=False)
    df.loc[:, 'last_name'] = parts[2]
    df.loc[df.full_name == 'deputy', 'rank_desc'] = 'deputy'
    return df.drop(columns='full_name')



def clean_dept_desc(df):
    df.dept_desc = df.dept_desc.str.lower().str.strip()\
        .str.replace('sro', 'school resource department', regex=False)\
        .str.replace(r'^res$', 'reserve', regex=True)\
        .str.replace('cid', 'criminal investigations division', regex=False)\
        .str.replace(r'pat(?:roll?)?', r'patrol', regex=True)\
        .str.replace('comm', 'communications', regex=False)\
        .str.replace('admin', 'administration', regex=False)
    return df.fillna('')

def clean_complaint_type(df):
    df.complaint_type = df.complaint_type.str.lower().str.strip().fillna('')\
        .str.replace('citizen complaint', 'citizen', regex=False)\
        .str.replace('admin inv', 'administrative', regex=False)\
        .str.replace(r'int(?:\.?)?', r'internal', regex=True)\
        .str.replace(r'ex[rt](?:\.?)?', r'external', regex=True)
    return df

def clean_rule_violation(df):
    df.rule_violation = df.rule_violation.str.lower().str.strip()\
        .str.replace(r' ?/ ?', '/', regex=True)\
        .str.replace('w/', 'with ', regex=False)\
        .str.replace('.', '', regex=False)\
        .str.replace('$', '', regex=False)\
        .str.replace('-', '', regex=False)\
        .str.replace(r'cour?ter?se?y', 'courtesy', regex=True)\
        .str.replace('authoruty', 'authority', regex=False)\
        .str.replace(r'unsar?tasfactory', 'unsatisfactory', regex=True)\
        .str.replace(r'pefr?ormance', 'performance', regex=True)\
        .str.replace('accidnet', 'accident', regex=False)\
        .str.replace('rudness', 'rudeness', regex=False)\
        .str.replace('policy violation', '', regex=False)\
        .str.replace('mishandeling', 'mishandling', regex=False)\
        .str.replace('handeling', 'handling', regex=False)\
        .str.replace('uof', 'use of force', regex=False)\
        .str.replace('trafic', 'traffic', regex=False)\
        .str.replace('mistratment', 'mistreatment', regex=False)\
        .str.replace('misued', 'misuse', regex=False)\
        .str.replace(' pursuit', 'pursuit', regex=False)\
        .str.replace('delayed responsetime', 'delayed response time', regex=False)\
        .str.replace('social mediathreat', 'social media threat', regex=False)\
        .str.replace('behaivor', 'behavior', regex=False)
    return df



def clean_investigating_supervisor(df):
    df.loc[:, 'investigating_supervisor'] = df.investigating_supervisor.str.lower().str.strip().fillna('')\
        .str.replace('.', '', regex=False)\
        .str.replace(r'\bca?pt\b', 'captain', regex=True)\
        .str.replace('det', 'detective', regex=False)\
        .str.replace('sgt', 'sargeant', regex=False)\
        .str.replace('km', 'kim', regex=False)\
        .str.replace('mke', 'mike', regex=False)\
        .str.replace(r'\b(lt|ltj|it)\b', 'lieutenant', regex=True)
    parts = df.investigating_supervisor.str.extract(r'(?:(lieutenant|captain|detective|chief|major|sargeant))?( [^ ]*) (.+)')
    df.loc[:, 'supervisors_rank_desc'] = parts[0]
    df.loc[:, 'supervisors_first_name'] = parts[1]
    df.loc[:, 'supervisors_last_name'] = parts[2]
    return df


def clean_disposition(df):
    df.disposition = df.disposition.str.lower().str.strip()\
        .str.replace('not sustained', 'unsustained', regex=False)\
        .str.replace('exonerted', 'exonerated', regex=False)\
        .str.replace(r'(?:admin[\.]?[closed]?)', 'administrative', regex=True)
    return df


def clean_action(df):
    df.action = df.action.str.lower().str.strip()\
        .str.replace('')


def clean():
    df = pd.read_csv(data_file_path(
        'tangipahoa_so/tangipahoa_so_cprr_2015_2021.csv')
        )\
        .pipe(split_rows_with_name)\
        .pipe(split_full_name)\
        .pipe(clean_dept_desc)\
        .pipe(clean_complaint_type)\
        .pipe(clean_rule_violation)\
        .pipe(clean_investigating_supervisor)\
        .pipe(clean_disposition)
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(
        data_file_path('clean/cprr_tangipahoa_so_2015_2021.csv'),
        index=False)