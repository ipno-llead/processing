import sys
sys.path.append('../')
from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import (
    clean_names, standardize_desc_cols, clean_dates
)
import pandas as pd



#  for col, pat in [('rank_desc', r'^((\w{2}\.|\w{2}\s)?)'), ('first_name', r"^([\w'-]+)(.*)$")]:
#         names = series[series.str.match(pat)].str.extract(pat)
#         df.loc[series.str.match(pat), col] = names[0]
#         series = series.str.replace(pat, r'\2').str.strip()
#     df.loc[:, 'last_name'] = series
#     return df.drop(columns=['full_name'])



def split_name(df):
    df.full_name = df.full_name.str.lower().str.strip()\
        .str.replace('.', '', regex=False)\
        .str.replace('jessical', 'jessica', regex=False)\
        .str.replace('dy', 'deputy', regex=False)\
        .str.replace('capt', 'captain', regex=False)\
        .str.replace('sgt', 'sargeant', regex=False)\
        .str.replace('lt', 'lieutenant', regex=False)\
        .str.replace(r'unk|unknown', '', regex=True)
    series = df.full_name.fillna('').str.strip()
    for col, pat in [('rank_desc', r'^((lieutenant|deputy|captain|sargeant)?)'), ('first_name', r'((\s?[A-Za-z]+))')]:
        names = series[series.str.match(pat)].str.extract(pat)
        df.loc[series.str.match(pat), col] = names[0]
        series = series.str.replace(pat, r'\2').str.strip()
    df.loc[:, 'last_name'] = series
    df.first_name = df.first_name.str.replace(r'deputy|lieutenant|sargeant|captain', '', regex=True)
    return df.drop(columns=['full_name'])


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
        .str.replace('coutersy', 'courtesty', regex=False)\
        .str.replace('authoruty', 'authority', regex=False)\
        .str.replace(r'unsa[r]?tasfactory', r'unsatisfactory', regex=True)\
        .str.replace('peformance', 'performance', regex=False)\
        .str.replace('policy violation', '', regex=False)\
        .str.replace('uof', 'use of force', regex=False)\
        .str.replace('trafic', 'traffic', regex=False)\
        .str.replace('mistratment', 'mistreatment', regex=False)\
        .str.replace('misued', 'misuse', regex=False)\
        .str.replace(' pursuit', 'pursuit', regex=False)
    return df
     

def clean():
    df = pd.read_csv(data_file_path(
        'tangipahoa_so/tangipahoa_so_cprr_2015-2021.csv')
        )\
        .pipe(split_name)\
        .pipe(clean_dept_desc)\
        .pipe(clean_complaint_type)\
        .pipe(clean_rule_violation)
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(
        data_file_path('clean/cprr_tangipahoa_so_2015_2021.csv'),
        index=False)

print(df['rule_violation'].unique())
print(df.columns.to_list())
