import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names
from lib.uid import gen_uid


def split_name(df):
    names = df.name.str.lower().str.strip()\
        .str.extract(r'(\w+) (\w+)')
    df.loc[:, 'first_name'] = names[0]
    df.loc[:, 'last_name'] = names[1]
    return df

def assign_agency(df):
    df.loc[:, 'agency'] = ''
    return df

    
def clean():
    df = pd.read_csv(data_file_path('raw/tangipahoa_da/tangipahoa_da_brady_2021.csv'))\
        .pipe(clean_column_names)\
        .pipe(split_name)\
        .pipe(assign_agency)\
        .pipe(gen_uid, ['first_name', 'last_name', 'agency'])
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path(
        'clean/cprr_tangipahoa_da_2021.csv'), index=False)
