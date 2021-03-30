from lib.path import data_file_path, ensure_data_dir
from lib.match import (
    ColumnsIndex, JaroWinklerSimilarity, StringSimilarity, DateSimilarity, ThresholdMatcher
)
from lib.uid import gen_uid
from lib.date import combine_date_columns
import pandas as pd
import sys
sys.path.append("../")


def match_csd_pprr_2017_v_2019(df17, df19):
    dfa = df17[["last_name", "first_name",
                "middle_initial", "rank_code", "employee_id"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        df17, "hire_year", "hire_month", "hire_day")
    dfa.loc[:, "rank_code"] = dfa.rank_code.astype("str")
    dfa.loc[:, "lnsf"] = dfa.last_name.map(lambda x: "" if x == "" else x[:2])
    dfa = dfa.drop_duplicates("employee_id").set_index(
        "employee_id", drop=True)

    df19 = gen_uid(df19, ["agency", "data_production_year", "employee_id"])
    dfb = df19[[
        "last_name", "first_name", "middle_initial", "rank_code", "uid"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        df19, "hire_year", "hire_month", "hire_day")
    dfb.loc[:, "rank_code"] = dfb.rank_code.astype("str")
    dfb.loc[:, "lnsf"] = dfb.last_name.map(lambda x: "" if x == "" else x[:2])
    dfb = dfb.drop_duplicates("uid").set_index(
        "uid", drop=True)

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["first_name", "lnsf"]), {
        "last_name": JaroWinklerSimilarity(0.25),
        "middle_initial": StringSimilarity(),
        "rank_code": StringSimilarity(),
        "hire_date": DateSimilarity()
    })
    decision = 0.87
    matcher.save_pairs_to_excel(data_file_path(
        "match/baton_rouge_csd_pprr_2017_v_pprr_2019.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    emp_id_17_to_uid_dict = dict()
    for emp_id_17, emp_id_19 in matches:
        row_17 = dfa.loc[emp_id_17]
        row_19 = dfb.loc[emp_id_19]

        # correct with longest last_name
        last_name = row_19.last_name
        if len(row_17.last_name) > len(last_name):
            last_name = row_17.last_name
        df17.loc[df17.employee_id == emp_id_17, "last_name"] = last_name
        df19.loc[df19.employee_id == emp_id_19, "last_name"] = last_name

        uid = row_19.name
        emp_id_17_to_uid_dict[emp_id_17] = uid

    df17 = gen_uid(df17, ["agency", "data_production_year", "employee_id"])
    uid_17 = df17.employee_id.map(lambda x: emp_id_17_to_uid_dict.get(x, ""))
    df17.loc[:, "uid"] = uid_17.where(uid_17 != "", df17.uid)

    return df17, df19


def match_pd_cprr_2018_v_csd_pprr_2019(cprr, pprr):
    dfa = cprr[["first_name", "last_name", "middle_initial", "uid"]
               ].drop_duplicates("uid").set_index("uid", drop=True)
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["first_name", "last_name", "middle_initial", "uid"]
               ].drop_duplicates("uid").set_index("uid", drop=True)
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
        "middle_initial": JaroWinklerSimilarity(),
    })
    decision = 0.96
    matcher.save_pairs_to_excel(data_file_path(
        "match/baton_rouge_pd_cprr_2018_v_csd_pprr_2019.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    # cprr takes on uid from pprr whenever there is a match
    matches = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: matches.get(x, x))

    return cprr


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post[post.agency == 'baton rouge pd']
    duplicated_uids = set(post.loc[post.uid.duplicated(), 'uid'].to_list())
    post = post.set_index('uid', drop=False)
    level_1_cert_dates = post.loc[
        post.uid.isin(duplicated_uids) & (post.level_1_cert_date.notna()),
        'level_1_cert_date']
    for idx, value in level_1_cert_dates.iteritems():
        post.loc[idx, 'level_1_cert_date'] = value
    post = post.sort_values('last_pc_12_qualification_date', ascending=False)
    return post[~post.index.duplicated(keep='first')]


def match_csd_pprr_against_post(pprr, post, year, decision):
    dfa = pprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day")
    dfa.loc[:, 'fc'] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day")
    dfb.loc[:, 'fc'] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "last_name": JaroWinklerSimilarity(),
        "first_name": JaroWinklerSimilarity(),
        "hire_date": DateSimilarity()
    })
    matcher.save_pairs_to_excel(data_file_path(
        "match/baton_rouge_csd_pprr_%d_v_post_pprr_2020_11_06.xlsx" % year), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr.loc[:, 'level_1_cert_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'level_1_cert_date'] if x in match_dict else '')
    pprr.loc[:, 'last_pc_12_qualification_date'] = pprr.uid.map(
        lambda x: post.loc[match_dict[x], 'last_pc_12_qualification_date'] if x in match_dict else '')
    return pprr


def match_lprr_against_pprr(lprr, pprr, year, decision):
    dfa = lprr[['uid', 'first_name', 'last_name', 'middle_initial']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = pprr[['uid', 'first_name', 'last_name', 'middle_initial']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity(),
        "middle_initial": StringSimilarity(),
    })
    matcher.save_pairs_to_excel(data_file_path(
        "match/baton_rouge_fpcsb_lprr_1992_2012_v_pprr_%d.xlsx" % year), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, 'uid'] = lprr.uid.map(lambda x: match_dict.get(x, x))
    return lprr


def match_lprr_against_post(lprr, post):
    dfa = lprr[['uid', 'first_name', 'last_name']]
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')

    dfb = post[['uid', 'first_name', 'last_name']]
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')

    matcher = ThresholdMatcher(dfa, dfb, ColumnsIndex(["fc"]), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity(),
    })
    decision = 0.93
    matcher.save_pairs_to_excel(data_file_path(
        "match/baton_rouge_fpcsb_lprr_1992_2012_v_post_pprr_2020_11_06.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, 'level_1_cert_date'] = lprr.uid.map(
        lambda x: post.loc[match_dict[x], 'level_1_cert_date'] if x in match_dict else '')
    lprr.loc[:, 'last_pc_12_qualification_date'] = lprr.uid.map(
        lambda x: post.loc[match_dict[x], 'last_pc_12_qualification_date'] if x in match_dict else '')
    return lprr


if __name__ == "__main__":
    df17 = pd.read_csv(data_file_path(
        "clean/pprr_baton_rouge_csd_2017.csv"))
    df19 = pd.read_csv(data_file_path(
        "clean/pprr_baton_rouge_csd_2019.csv"))
    lprr = pd.read_csv(data_file_path(
        "clean/lprr_baton_rouge_fpcsb_1992_2012.csv"))
    cprr = pd.read_csv(data_file_path("clean/cprr_baton_rouge_pd_2018.csv"))
    df17, df19 = match_csd_pprr_2017_v_2019(df17, df19)
    lprr = match_lprr_against_pprr(lprr, df17, 2017, 0.98)
    lprr = match_lprr_against_pprr(lprr, df19, 2019, 0.97)
    post = prepare_post_data()
    lprr = match_lprr_against_post(lprr, post)
    df17 = match_csd_pprr_against_post(df17, post, 2017, 0.798)
    df19 = match_csd_pprr_against_post(df19, post, 2019, 0.894)
    cprr = match_pd_cprr_2018_v_csd_pprr_2019(cprr, df19)
    ensure_data_dir("match")
    lprr.to_csv(
        data_file_path("match/lprr_baton_rouge_fpcsb_1992_2012.csv"),
        index=False)
    df17.to_csv(
        data_file_path("match/pprr_baton_rouge_csd_2017.csv"),
        index=False)
    df19.to_csv(
        data_file_path("match/pprr_baton_rouge_csd_2019.csv"),
        index=False)
    cprr.to_csv(
        data_file_path("match/cprr_baton_rouge_pd_2018.csv"),
        index=False)
