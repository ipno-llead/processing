import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.clean import float_to_int_str


def filter_out_non_duplicates(df):
    #  creates a df in which a given tracking_number must appear more than once
    df[['uid', 'tracking_number']].groupby("tracking_number").filter(lambda x: len(x) > 1)

    #  if a given uid is associated with a given tracking number more than once, drop it
    df['uids'] = list(zip(df.uid, df.tracking_number))
    df = df[['uids']]
    df = df.drop_duplicates(subset=['uids'])

    #  creates a df where a given tracking_number is associated with atleast two persons
    df['uid'], df['tracking_number'] = df.uids.str

    df.loc[:, 'tracking_number'] = df.tracking_number.fillna('')
    return df[~((df.tracking_number == ''))].drop(columns='uids')


def groupby_tracking_number_and_uid(df):
    df = df.groupby('tracking_number')['uid']
    df = pd.DataFrame(df).astype(str)
    df.columns = ['tracking_number', 'uid']
    df = df.replace(r'\n', ', ', regex=True)

    df.loc[:, 'uid'] = df.uid.str.lower().str.strip()\
        .str.replace(r'\, (\w+) ', '', regex=True)\
        .str.replace(r'^(\w+)  +', '', regex=True)\
        .str.replace(r'\,? name(.+)', '', regex=True)\
        .str.replace(r'^(\w+)$', '', regex=True)\
        .str.replace(r'  +', ', ', regex=True)
    return df[~((df.uid == ''))]


def generate_duplicate_df(df):
    df = df[df.duplicated(subset=['uid'], keep=False)]
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/ipm/allegations_nopd_merged.csv'))\
        .pipe(float_to_int_str, ['tracking_number', 'uid', 'uids'])\
        .pipe(filter_out_non_duplicates)\
        .pipe(groupby_tracking_number_and_uid)\
        .pipe(generate_duplicate_df)
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('match/clusters_allegations_by_tracking_number_nopd_1931-2020.csv'), index=False)
