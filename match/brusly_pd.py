from lib.match import (
    ThresholdMatcher, NoopIndex, JaroWinklerSimilarity
)
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys
sys.path.append("../")


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post[post.agency == 'brusly pd']
    duplicated_uids = set(post.loc[post.uid.duplicated(), 'uid'].to_list())
    post = post.set_index('uid', drop=False)
    level_1_cert_dates = post.loc[
        post.uid.isin(duplicated_uids) & (post.level_1_cert_date.notna()),
        'level_1_cert_date']
    for idx, value in level_1_cert_dates.iteritems():
        post.loc[idx, 'level_1_cert_date'] = value
    post = post.sort_values('last_pc_12_qualification_date', ascending=False)
    return post[~post.index.duplicated(keep='first')]


def add_uid_to_complaint(cprr, pprr):
    dfa = cprr[["first_name", "last_name"]]
    dfb = pprr.set_index("uid", drop=True)[["first_name", "last_name"]]

    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity()
    })
    decision = 0.7
    matcher.save_pairs_to_excel(data_file_path(
        "match/brusly_pd_cprr_officer_v_pprr.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(decision)

    matches = dict(matches)
    cprr.loc[:, "uid"] = cprr.index.map(lambda i: matches[i])
    cprr = cprr.drop(columns=["first_name", "last_name"])

    return cprr


def add_supervisor_uid_to_complaint(cprr, pprr):
    dfa = cprr[["supervisor_first_name", "supervisor_last_name"]]
    dfa.columns = ["first_name", "last_name"]
    dfb = pprr.set_index("uid", drop=True)[["first_name", "last_name"]]

    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity()
    })
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/brusly_pd_cprr_supervisor_v_pprr.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    matches = dict(matches)
    cprr.loc[:, "supervisor_uid"] = cprr.index.map(lambda i: matches[i])
    cprr = cprr.drop(columns=["supervisor_first_name", "supervisor_last_name"])

    return cprr


def add_post_columns_to_pprr(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]].set_index("uid", drop=True)
    dfb = post[["uid", "first_name", "last_name"]].set_index("uid", drop=True)

    matcher = ThresholdMatcher(dfa, dfb, NoopIndex(), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity()
    })
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/brusly_pd_pprr_v_post.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    matches = dict(matches)
    pprr.loc[:, 'level_1_cert_date'] = pprr.uid.map(
        lambda x: post.loc[matches[x], 'level_1_cert_date'] if x in matches else '')
    pprr.loc[:, 'last_pc_12_qualification_date'] = pprr.uid.map(
        lambda x: post.loc[matches[x], 'last_pc_12_qualification_date'] if x in matches else '')
    return pprr


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path("clean/cprr_brusly_pd_2020.csv"))
    pprr = pd.read_csv(data_file_path("clean/pprr_brusly_pd_2020.csv"))
    post = prepare_post_data()
    cprr = add_uid_to_complaint(cprr, pprr)
    cprr = add_supervisor_uid_to_complaint(cprr, pprr)
    pprr = add_post_columns_to_pprr(pprr, post)
    ensure_data_dir("match")
    cprr.to_csv(data_file_path("match/cprr_brusly_pd_2020.csv"), index=False)
    pprr.to_csv(data_file_path("match/pprr_brusly_pd_2020.csv"), index=False)
