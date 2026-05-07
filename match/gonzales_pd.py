import pandas as pd
from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    ColumnsIndex,
    NoopIndex,
    DateSimilarity,
)
import deba
from lib.date import combine_date_columns
from lib.post import extract_events_from_post, load_for_agency


def match_pprr_2026_with_pprr(pprr_26, pprr):
    dfa = (
        pprr_26[["first_name", "last_name", "uid"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr[["first_name", "last_name", "uid"]]
        .drop_duplicates(subset=["uid"])
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
    decision = 0.96
    matcher.save_pairs_to_excel(
        deba.data("match/gonzales_pd_pprr_2026_v_pprr_2010_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr_26.loc[:, "uid"] = pprr_26.uid.map(lambda x: match_dict.get(x, x))
    return pprr_26


def extract_post_events_2026(pprr_26, post):
    dfa = pprr_26[["first_name", "last_name", "uid"]].copy()
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid", drop=True)

    dfb = post[["last_name", "first_name", "uid"]].copy()
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9
    matcher.save_pairs_to_excel(
        deba.data("match/gonzales_pd_pprr_2026_v_post_pprr_2025_08_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "gonzales-pd")


def extract_post_events(pprr, post):
    dfa = pprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index("uid", drop=True)

    dfb = post[["last_name", "first_name", "uid"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day"
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "hire_date": DateSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.816
    matcher.save_pairs_to_excel(
        deba.data("match/gonzales_pd_pprr_2010_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "gonzales-pd")


def match_uof_with_pprr_26(uof, pprr_26):
    dfa = (
        uof[["last_name", "uid"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )

    dfb = (
        pprr_26[["last_name", "uid"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )

    matcher = ThresholdMatcher(
        NoopIndex(),
        {
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9
    matcher.save_pairs_to_excel(
        deba.data("match/gonzales_pd_uof_2023_2026_v_pprr_2026.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    for uid_a, uid_b in matches:
        row = pprr_26.loc[pprr_26.uid == uid_b].iloc[0]
        uof.loc[uof.uid == uid_a, "first_name"] = row.first_name
        uof.loc[uof.uid == uid_a, "last_name"] = row.last_name
        uof.loc[uof.uid == uid_a, "uid"] = uid_b
    return uof


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_gonzales_pd_2010_2021.csv"))
    pprr_26 = pd.read_csv(deba.data("clean/pprr_gonzales_pd_2026.csv"))
    uof = pd.read_csv(deba.data("clean/uof_gonzales_pd_2023_2026.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    pprr_26 = match_pprr_2026_with_pprr(pprr_26, pprr)
    uof = match_uof_with_pprr_26(uof, pprr_26)
    post_events = extract_post_events(pprr, post)
    post_events_26 = extract_post_events_2026(pprr_26, post)
    pprr_26.to_csv(deba.data("match/pprr_gonzales_pd_2026.csv"), index=False)
    uof.to_csv(deba.data("match/uof_gonzales_pd_2023_2026.csv"), index=False)
    post_events.to_csv(
        deba.data("match/post_event_gonzales_pd_2010_2021.csv"), index=False
    )
    post_events_26.to_csv(
        deba.data("match/post_event_gonzales_pd_2026.csv"), index=False
    )
