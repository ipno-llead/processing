from lib.columns import clean_column_names
from lib.path import data_file_path
from lib.clean import clean_names
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append("../")


def clean():
    df = pd.read_csv(data_file_path(
        "raw/baton_rouge_da/baton_rouge_da_cprr_2021.csv"))
    df = clean_column_names(df)
    return df\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_name'])\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name', 'middle_name'])


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/cprr_baton_rouge_da_2021.csv'), index=False)
