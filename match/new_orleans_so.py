import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex, Swap

from lib.path import data_file_path, ensure_data_dir

sys.path.append('../')


def deduplicate_cprr_personnel(cprr):
    df = cprr.loc[:, ['employee_id', 'first_name', 'last_name', 'uid']]\
        .drop_duplicates().set_index('uid', drop=True)

    matcher = ThresholdMatcher(ColumnsIndex('employee_id'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, df, variator=Swap('first_name', 'last_name'))
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/new_orleans_so_cprr_dedup.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    # canonicalize name and uid
    for uid, al_uid in matches:
        for col in ['uid', 'first_name', 'last_name', 'middle_initial']:
            v = cprr.loc[cprr.uid == uid, col]
            if hasattr(v, 'shape'):
                v = v.iloc[0]
            cprr.loc[cprr.uid == al_uid, col] = v
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_new_orleans_so_2019.csv'))
    ensure_data_dir('match')
    cprr = deduplicate_cprr_personnel(cprr)
    cprr.to_csv(data_file_path(
        'match/cprr_new_orleans_so_2019.csv'), index=False)
