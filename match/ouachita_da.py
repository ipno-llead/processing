from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path
import pandas as pd
import sys
sys.path.append('../')


def prepare_lsp_and_opso_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    return post[post.agency.isin(['la state police', 'ouachita parish so'])]


def match_cprr_with_lsp_and_opso_post(cprr, post):
    dfa = cprr[['uid', 'first_name', 'last_name', 'agency']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name', 'agency']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(ColumnsIndex(['fc']), {
        'last_name': JaroWinklerSimilarity(),
        'first_name': JaroWinklerSimilarity(),
        'agency': JaroWinklerSimilarity(),
    }, dfa, dfb)
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        'match/ouachita_da_cprr_2021_v_post_lsp_and_opso_2020_11_06.xlsx'), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, 'uid'] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_ouachita_da_2021.csv'))
    post_lsp_and_opso = prepare_lsp_and_opso_post_data()
    cprr = match_cprr_with_lsp_and_opso_post(cprr, post_lsp_and_opso)
    cprr.to_csv(data_file_path(
        'match/cprr_ouachita_da_2021.csv'), index=False)
