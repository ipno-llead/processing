from lib.match import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append('../')


def prepare_post_data():
    post = pd.read_csv(data_file_path('clean/pprr_post_2020_11_06.csv'))
    post = post[post.agency == 'la state police']
    duplicated_uids = set(post.loc[post.uid.duplicated(), 'uid'].to_list())
    post = post.set_index('uid', drop=False)
    level_1_cert_dates = post.loc[
        post.uid.isin(duplicated_uids) & (post.level_1_cert_date.notna()),
        'level_1_cert_date']
    for idx, value in level_1_cert_dates.iteritems():
        post.loc[idx, 'level_1_cert_date'] = value
    post = post.sort_values('last_pc_12_qualification_date', ascending=False)
    return post[~post.index.duplicated(keep='first')]


def match_post(lprr, post):
    dfa = lprr[['uid', 'first_name', 'last_name']].drop_duplicates()
    dfa_rev = dfa.rename(columns={
        'first_name': 'last_name',
        'last_name': 'first_name'
    })
    dfa_rev.loc[:, 'uid'] = 'rev-'+dfa.uid
    dfa = pd.concat([dfa, dfa_rev]).set_index('uid', drop=True)
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[['uid', 'first_name', 'last_name']].drop_duplicates()\
        .set_index('uid', drop=True)
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(['fc']), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    })
    decision = 0.925
    matcher.save_pairs_to_excel(data_file_path(
        "match/louisiana_state_csc_1991_2020_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    for lprr_uid, post_uid in matches:
        if lprr_uid.startswith('rev-'):
            lprr_uid = lprr_uid[4:]
        lprr.loc[lprr.uid == lprr_uid, 'uid'] = post_uid
    return lprr


if __name__ == '__main__':
    lprr = pd.read_csv(data_file_path(
        'clean/lprr_louisiana_state_csc_1991_2020.csv'))
    post = prepare_post_data()
    lprr = match_post(lprr, post)
    ensure_data_dir('match')
    lprr.to_csv(data_file_path(
        'match/lprr_louisiana_state_csc_1991_2020.csv'
    ), index=False)
