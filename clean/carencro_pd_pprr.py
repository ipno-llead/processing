import sys

import pandas as pd

from lib.columns import clean_column_names, set_values
from lib.path import data_file_path
from lib.standardize import standardize_from_lookup_table
from lib.uid import gen_uid
from lib.clean import clean_sexes, clean_races

sys.path.append('../')


def clean():
    return pd.read_csv(data_file_path(
        'raw/carencro_pd/carencro_pd_pprr_2021.csv'
    )).pipe(clean_column_names)\
        .pipe(standardize_from_lookup_table, 'rank_desc', [
            ['chief'],
            ['assistant chief'],
            ['officer', 'p.o'],
            ['captain'],
            ['detective'],
            ['sergeant', 'sargeant'],
            ['military/activated']
        ]).pipe(set_values, {
            'agency': 'Carencro PD'
        }).pipe(gen_uid, ['agency', 'first_name', 'last_name'])\
        .pipe(clean_sexes, ['sex'])\
        .pipe(clean_races, ['race'])


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path(
        'clean/pprr_carencro_pd_2021.csv'
    ), index=False)
