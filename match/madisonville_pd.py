from lib.match import (
    ThresholdMatcher, NoopIndex, JaroWinklerSimilarity, ColumnsIndex
)
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append("../")


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post[post.agency == 'madisonville pd']
    duplicated_uids = set(post.loc[post.uid.duplicated(), 'uid'].to_list())
    post = post.set_index('uid', drop=False)
    level_1_cert_dates = post.loc[
        post.uid.isin(duplicated_uids) & (post.level_1_cert_date.notna()),
        'level_1_cert_date']
    for idx, value in level_1_cert_dates.iteritems():
        post.loc[idx, 'level_1_cert_date'] = value
    post = post.sort_values('last_pc_12_qualification_date', ascending=False)
    return post[~post.index.duplicated(keep='first')]


def match_cprr_and_pprr(cprr, pprr):
    dfa = cprr[["last_name", "first_name"]]
    dfb = pprr[["last_name", "first_name", "uid"]].drop_duplicates()
    dfb = dfb.set_index("uid", drop=True)
    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity()
    })
    decision = 1
    matcher.save_pairs_to_excel(data_file_path(
        "match/madisonville_pd_cprr_2010_2020_v_csd_pprr_2019.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    for idx, uid in matches:
        cprr.loc[idx, "uid"] = uid
    cprr = cprr.drop(columns=["first_name", "last_name"])
    return cprr


def match_pprr_and_post(pprr, post):
    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
    })
    decision = 0.95
    matcher.save_pairs_to_excel(data_file_path(
        "match/madisonville_csd_pprr_2019_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr.loc[:, 'level_1_cert_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'level_1_cert_date'] if x in match_dict else '')
    pprr.loc[:, 'last_pc_12_qualification_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'last_pc_12_qualification_date'] if x in match_dict else '')
    return pprr


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path(
        "clean/cprr_madisonville_pd_2010_2020.csv"))
    pprr = pd.read_csv(data_file_path(
        "clean/pprr_madisonville_csd_2019.csv"))
    post = prepare_post_data()
    cprr = match_cprr_and_pprr(cprr, pprr)
    pprr = match_pprr_and_post(pprr, post)
    ensure_data_dir("match")
    cprr.to_csv(data_file_path(
        "match/cprr_madisonville_pd_2010_2020.csv"), index=False)
    pprr.to_csv(data_file_path(
        "match/pprr_madisonville_csd_2019.csv"), index=False)
