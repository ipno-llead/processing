from lib.match import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append('../')


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency == 'plaquemines par so']


def match_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name']].drop_duplicates(subset=['uid'])\
        .set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']].drop_duplicates(subset=['uid'])\
        .set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity()
    })
    decision = 0.8
    matcher.save_pairs_to_excel(data_file_path(
        "match/plaquemines_so_cprr_2019_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict[x])
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_plaquemines_so_2019.csv'))
    post = prepare_post_data()
    cprr = match_post(cprr, post)
    ensure_data_dir('match')
    cprr.to_csv(data_file_path(
        'match/cprr_plaquemines_so_2019.csv'
    ), index=False)
