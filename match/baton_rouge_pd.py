from datamatch import (
    ColumnsIndex,
    JaroWinklerSimilarity,
    StringSimilarity,
    ThresholdMatcher,
)
import pandas as pd

import deba
from lib.post import extract_events_from_post, load_for_agency


def match_csd_and_pd_pprr(csd, pprr, year, decision):
    dfa = (
        csd[["last_name", "first_name", "middle_initial", "uid"]]
        .drop_duplicates("uid")
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])

    dfb = (
        pprr[["last_name", "first_name", "middle_initial", "uid"]]
        .drop_duplicates("uid")
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_csd_pprr_%d_v_pd_pprr_2021.xlsx" % year),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    csd.loc[:, "uid"] = csd.uid.map(lambda x: match_dict.get(x, x))
    return csd


def match_pd_cprr_2018_v_pprr(cprr, pprr):
    dfa = (
        cprr[["first_name", "last_name", "middle_initial", "uid"]]
        .drop_duplicates("uid")
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr[["first_name", "last_name", "middle_initial", "uid"]]
        .drop_duplicates("uid")
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
            "middle_initial": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_pd_cprr_2018_v_pd_pprr_2021.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    # cprr takes on uid from pprr whenever there is a match
    matches = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: matches.get(x, x))

    return cprr


def match_pd_cprr_2021_v_pprr(cprr, pprr):
    dfa = (
        cprr[["first_name", "last_name", "middle_initial", "uid"]]
        .drop_duplicates("uid")
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr[["first_name", "last_name", "middle_initial", "uid"]]
        .drop_duplicates("uid")
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
            "middle_initial": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_pd_cprr_2021_v_pd_pprr_2021.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    # cprr takes on uid from pprr whenever there is a match
    matches = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: matches.get(x, x))

    return cprr


def match_pprr_against_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.93
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_pd_pprr_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "Baton Rouge PD")


def match_lprr_against_pprr(lprr, pprr):
    dfa = lprr[["uid", "first_name", "last_name", "middle_initial"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name", "middle_initial"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "middle_initial": StringSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_fpcsb_lprr_1992_2012_v_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, "uid"] = lprr.uid.map(lambda x: match_dict.get(x, x))
    return lprr


if __name__ == "__main__":
    csd17 = pd.read_csv(
        deba.data(
            "clean/pprr_baton_rouge_csd_2017.csv",
        )
    )
    csd19 = pd.read_csv(
        deba.data(
            "clean/pprr_baton_rouge_csd_2019.csv",
        )
    )
    lprr = pd.read_csv(
        deba.data(
            "clean/lprr_baton_rouge_fpcsb_1992_2012.csv",
        )
    )
    cprr18 = pd.read_csv(
        deba.data(
            "clean/cprr_baton_rouge_pd_2018.csv",
        )
    )
    cprr21 = pd.read_csv(
        deba.data(
            "clean/cprr_baton_rouge_pd_2021.csv",
        )
    )
    pprr = pd.read_csv(deba.data("clean/pprr_baton_rouge_pd_2021.csv"))
    csd17 = match_csd_and_pd_pprr(csd17, pprr, 2017, 0.88)
    csd19 = match_csd_and_pd_pprr(csd19, pprr, 2019, 0.88)
    lprr = match_lprr_against_pprr(lprr, pprr)
    cprr18 = match_pd_cprr_2018_v_pprr(cprr18, pprr)
    cprr21 = match_pd_cprr_2021_v_pprr(cprr21, pprr)
    agency = cprr21.agency[0]
    post = load_for_agency(agency)
    post_event = match_pprr_against_post(pprr, post)
    assert post_event[post_event.duplicated(subset=["event_uid"])].shape[0] == 0

    lprr.to_csv(deba.data("match/lprr_baton_rouge_fpcsb_1992_2012.csv"), index=False)
    csd17.to_csv(deba.data("match/pprr_baton_rouge_csd_2017.csv"), index=False)
    csd19.to_csv(deba.data("match/pprr_baton_rouge_csd_2019.csv"), index=False)
    cprr18.to_csv(deba.data("match/cprr_baton_rouge_pd_2018.csv"), index=False)
    cprr21.to_csv(deba.data("match/cprr_baton_rouge_pd_2021.csv"), index=False)
    post_event.to_csv(deba.data("match/event_post_baton_rouge_pd.csv"), index=False)
