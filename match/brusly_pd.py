from datamatch import ThresholdMatcher, NoopIndex, JaroWinklerSimilarity
import pandas as pd

import bolo
from lib.post import extract_events_from_post, load_for_agency


def add_uid_to_complaint(cprr, pprr):
    dfa = cprr[["first_name", "last_name"]]
    dfb = pprr.set_index("uid", drop=True)[["first_name", "last_name"]]

    matcher = ThresholdMatcher(
        NoopIndex(),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.7
    matcher.save_pairs_to_excel(
        bolo.data("match/brusly_pd_cprr_officer_v_pprr.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)

    matches = dict(matches)
    cprr.loc[:, "uid"] = cprr.index.map(lambda i: matches[i])
    cprr = cprr.drop(columns=["first_name", "last_name"])

    return cprr


def add_supervisor_uid_to_complaint(cprr, pprr):
    dfa = cprr[["supervisor_first_name", "supervisor_last_name"]]
    dfa.columns = ["first_name", "last_name"]
    dfb = pprr.set_index("uid", drop=True)[["first_name", "last_name"]]

    matcher = ThresholdMatcher(
        NoopIndex(),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.9
    matcher.save_pairs_to_excel(
        bolo.data("match/brusly_pd_cprr_supervisor_v_pprr.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    matches = dict(matches)
    cprr.loc[:, "supervisor_uid"] = cprr.index.map(lambda i: matches[i])
    cprr = cprr.drop(columns=["supervisor_first_name", "supervisor_last_name"])

    return cprr


def add_uid_to_award(award, pprr):
    dfa = (
        award[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )

    matcher = ThresholdMatcher(
        NoopIndex(),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.9
    matcher.save_pairs_to_excel(
        bolo.data("match/brusly_pd_award_v_pprr.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    matches = dict(matches)
    award.loc[:, "uid"] = award.uid.map(lambda i: matches[i])
    return award


def extract_post_events(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]].set_index("uid", drop=True)
    dfb = post[["uid", "first_name", "last_name"]].set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        NoopIndex(),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.9
    matcher.save_pairs_to_excel(bolo.data("match/brusly_pd_pprr_v_post.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Brusly PD")


if __name__ == "__main__":
    cprr = pd.read_csv(bolo.data("clean/cprr_brusly_pd_2020.csv"))
    pprr = pd.read_csv(bolo.data("clean/pprr_brusly_pd_2020.csv"))
    award = pd.read_csv(bolo.data("clean/award_brusly_pd_2021.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = add_uid_to_complaint(cprr, pprr)
    cprr = add_supervisor_uid_to_complaint(cprr, pprr)
    award = add_uid_to_award(award, pprr)
    post_events = extract_post_events(pprr, post)

    award.to_csv(bolo.data("match/award_brusly_pd_2021.csv"), index=False)
    cprr.to_csv(bolo.data("match/cprr_brusly_pd_2020.csv"), index=False)
    post_events.to_csv(bolo.data("match/post_event_brusly_pd_2020.csv"), index=False)
