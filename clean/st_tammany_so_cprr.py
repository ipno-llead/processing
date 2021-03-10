from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import split_names
import pandas as pd
import sys
sys.path.append('../')


def remove_newlines(df):
    for col in ['full_name', 'incident_date', 'infraction']:
        df.loc[:, col] = df[col].str.replace(r'(\d)\r\n(\d)', r'\1\2')\
            .str.replace(r'\r\n', ' ')
    return df


def clean():
    df = pd.read_csv(
        data_file_path(
            'st_tammany_so/st.tammany_so_cprr_lists_2015-2019_tabula.csv')
    )
    df = clean_column_names(df)
    return df\
        .pipe(remove_newlines)\
        .pipe(split_names, "full_name")
