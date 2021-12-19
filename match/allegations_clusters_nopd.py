
import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names
from pandas_dedupe import dedupe_dataframe


def filter_out_non_duplicates(df):
    #  if a given uid is associated with a given tracking number more than once, drop it
    df = df.drop_duplicates(subset=['uid', 'tracking_number'])

    #  creates a df in which a given tracking_number must appear more than once
    df[['uid', 'tracking_number']].groupby("tracking_number").filter(lambda x: len(x) > 1)

    #  remove rows with missing tracking_number
    df.loc[:, 'tracking_number'] = df.tracking_number.fillna('')
    return df[~((df.tracking_number == ''))]


def groupby_tracking_number_and_uid(df):
    #  creates a df with two columns: tracking_number (tn), uid
    #  each row contains a unique tn
    #  the corresponding uid column contains a uid, separated by commas, for each officer associated with a given tn
    df = pd.DataFrame(df.groupby('tracking_number')['uid']).astype(str)
    df.columns = ['tracking_number', 'uid']

    df.loc[:, 'uid'] = df.uid.str.lower().str.strip().fillna('')\
        .str.replace(r'\n', ', ', regex=True)\
        .str.replace(r'\, (\w+) ', '', regex=True)\
        .str.replace(r'^(\w+)  +', '', regex=True)\
        .str.replace(r'\,? name(.+)', '', regex=True)\
        .str.replace(r'^(\w+)$', '', regex=True)\
        .str.replace(r'  +', ', ', regex=True)
    return df[~((df.uid == ''))]


def create_clusters(df):
    df = dedupe_dataframe(df, ['uid'])
    return df


def filter_clusters(df):
    df = df[df.duplicated(subset=['cluster_id'], keep=False)].sort_values(by=['cluster_id'])
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/ipm/allegations_nopd_merged.csv'))\
        .pipe(filter_out_non_duplicates)\
        .pipe(groupby_tracking_number_and_uid)\
        .pipe(create_clusters)\
        .pipe(clean_column_names)\
        .pipe(filter_clusters)\
        .rename(columns={
            'confidence': 'confidence_score',
            'uid': 'uids'})
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('match/allegation_clusters_nopd_by_tracking_number.csv'), index=False)
