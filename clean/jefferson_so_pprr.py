import sys
sys.path.append('../')
from lib.path import data_file_path
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid


def extract_department_desc(df):
    departments = df.rank_desc\
        .str.replace(r'\.', '', regex=True)\
        .str.extract(r'(it|accounting|payroll|purchasing|property|narcotics|baliff|intake|jail|communications|'
                     r'propert|cib|juvenile|internal affairs|burglary|central records|'
                     r'crime lab|ems|homicide|robbery|court|'
                     r'human resources|accounting|academy|mail|database|network)')

    df.loc[:, 'department_desc'] = departments[0].fillna('')\
        .str.replace(r'\bpropert\b', 'property', regex=True)\
        .str.replace(r'(database|network)', 'it', regex=True)\
        .str.replace('ems', 'emergency medial services', regex=False)

    return df


def clean_district_desc(df):
    df.loc[:, 'district'] = df.dept_desc.fillna('')\
        .str.replace(r' ?sergeant ?', '', regex=True)\
        .str.replace('lieutenant', '', regex=False)
    return df.drop(columns='dept_desc')


def split_names(df):
    names = df.name\
        .str.replace(r'\.\,', ',', regex=True)\
        .str.replace('sue ellen', 'sue-ellen', regex=False)\
        .str.replace("jon' janice", "jon'janice", regex=False)\
        .str.replace('photo lab day', 'photolabday', regex=False)\
        .str.replace(' employees', '', regex=False)\
        .str.extract(r'^(\w+\-?\.?\'? ?\w+?\'?) ?(?:(\w+) )?\, (?:(\w+\-?\'?\w+?\'?)) ?(\w+)?\.?$')

    df.loc[:, 'last_name'] = names[0]
    df.loc[:, 'suffix'] = names[1].fillna('')
    df.loc[:, 'first_name'] = names[2]
    df.loc[:, 'middle_name'] = names[3].fillna('')\
        .str.replace(r'\.', '', regex=True)
    df.loc[:, 'last_name'] = df.last_name.str.cat(df.suffix, sep=' ')
    return df.drop(columns=['suffix', 'name'])


def clean_rank_desc(df): 
    df.loc[:, 'rank_desc'] = df.rank_desc\
        .str.extract(r'(it|accounting|payroll|purchasing|property|narcotics|baliff|intake|jail|communications|'
                     r'propert|cib|juvenile|internal affairs|burglary|central records|'
                     r'crime lab|ems|homicide|robbery|court|'
                     r'human resources|accounting|academy|mail|database|network)')
        .str.replace(r'\bcommaander\b', 'commander', regex=True)\
        .str.replace(r'\bpropert\b', 'property', regex=True)\
        .str.replace(r'\bcustodia\b', 'custodian', regex=True)\
        .str.replace('&', 'and', regex=True)\
        .str.replace(r'\bcomm\b', 'communications', regex=True)\
        .str.replace(r'\bdetecti\b', 'detective', regex=True)\
        .str.replace('booking of', 'booking officer', regex=False)
    return df
        

def clean():
    df = pd.read_csv(data_file_path('raw/jefferson_so/jefferson_parish_so_pprr_2020.csv'))\
        .pipe(clean_column_names)\
        .pipe(extract_department_desc)\
        .pipe(clean_district_desc)\
        .pipe(split_names)\
        .pipe(clean_rank_desc)\
        .pipe(set_values, {
            'agency': 'Jefferson SO'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'middle_name', 'last_name'])
    return df
