from lib.path import data_file_path, ensure_data_dir
from lib.match import (
    ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
)
import pandas as pd
import sys
sys.path.append("../")

def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    return post[post.agency == 'scott pd']


def match_cprr_against_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
    })
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        "match/scott_pd_cprr_2020_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_scott_pd_2020.csv'))
    post = prepare_post_data()
    cprr = match_cprr_against_post(cprr, post)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_scott_pd_2020.csv'), index=False)
