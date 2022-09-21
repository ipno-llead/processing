from datetime import datetime

from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    ColumnsIndex,
    Swap,
    NonOverlappingFilter,
)
import pandas as pd

import deba
from lib.post import extract_events_from_post, load_for_agency
from lib.date import combine_date_columns


def match_lprr_and_pprr(lprr, pprr):
    dfa = (
        lprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfa = dfa.fillna(value={"first_name": "", "last_name": ""})
    dfa.loc[:, "fc"] = dfa.apply(
        lambda row: "".join(sorted([row.first_name[:1], row.last_name[:1]])),
        axis=1,
        result_type="reduce",
    )

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb = dfb.fillna(value={"first_name": "", "last_name": ""})
    dfb.loc[:, "fc"] = dfb.apply(
        lambda row: "".join(sorted([row.first_name[:1], row.last_name[:1]])),
        axis=1,
        result_type="reduce",
    )

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        variator=Swap("first_name", "last_name"),
    )
    decision = 0.969
    matcher.save_pairs_to_excel(
        deba.data("match/louisiana_state_csc_lprr_1991_2020_v_csd_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, "uid"] = lprr.uid.map(lambda x: match_dict.get(x, x))
    return lprr


def extract_post_events(pprr, post):
    dfa = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        post[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.924
    matcher.save_pairs_to_excel(
        deba.data("match/louisiana_state_csd_pprr_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "louisiana-state-pd")


def match_pprr_demo_and_term(demo, term):
    dfa = demo[["uid", "first_name", "last_name"]]
    dfa.loc[:, "start_date"] = datetime(1900, 1, 1)
    dfa.loc[:, "end_date"] = combine_date_columns(
        demo, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = (
        dfa.sort_values(["uid", "end_date"], na_position="first")
        .drop_duplicates(subset=["uid"], keep="last")
        .set_index("uid", drop=True)
    )

    dfb = term[["uid", "first_name", "last_name"]]
    dfb.loc[:, "start_date"] = combine_date_columns(
        term, "left_year", "left_month", "left_day"
    )
    dfb.loc[:, "end_date"] = datetime(2100, 1, 1)
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = (
        dfb.sort_values(["uid", "start_date"], na_position="last")
        .drop_duplicates(subset=["uid"], keep="first")
        .set_index("uid", drop=True)
    )

    # left date and hire date of both datasets are fit into start_date - end_date range
    # such that latest hire date of an individual must be less than earliest left date
    # for a pair to be valid
    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        filters=[NonOverlappingFilter("start_date", "end_date")],
    )
    decision = 0.98
    matcher.save_pairs_to_excel(
        deba.data(
            "match/louisiana_state_csd_pprr_demographic_2021_v_terminations_2021.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    term.loc[:, "uid"] = term.uid.map(lambda x: match_dict.get(x, x))
    return term


def match_letters_to_pprr(letters, pprr):
    dfa = letters[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 1

    matcher.save_pairs_to_excel(
        deba.data("match/letters_lsp_2019_v_pprr_lsp.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    letters.loc[:, "uid"] = letters.uid.map(lambda x: match_dict.get(x, x))
    return letters


if __name__ == "__main__":
    lprr = pd.read_csv(deba.data("clean/lprr_louisiana_state_csc_1991_2020.csv"))
    pprr_demo = pd.read_csv(deba.data("clean/pprr_demo_louisiana_csd_2021.csv"))
    pprr_term = pd.read_csv(deba.data("clean/pprr_term_louisiana_csd_2021.csv"))
    letters = pd.read_csv(deba.data("clean/letters_louisiana_state_pd_2019.csv"))
    agency = pprr_term.agency[0]
    post = load_for_agency(agency)
    pprr_term = match_pprr_demo_and_term(pprr_demo, pprr_term)
    lprr = match_lprr_and_pprr(lprr, pprr_demo)
    post_events = extract_post_events(pprr_demo, post)
    letters = match_letters_to_pprr(letters, pprr_demo)
    lprr.to_csv(deba.data("match/lprr_louisiana_state_csc_1991_2020.csv"), index=False)
    post_events.to_csv(
        deba.data("match/post_event_louisiana_state_police_2020.csv"), index=False
    )
    pprr_term.to_csv(deba.data("match/pprr_term_louisiana_csd_2021.csv"), index=False)
    letters.to_csv(deba.data("match/letters_louisiana_state_pd_2019.csv"), index=False)
