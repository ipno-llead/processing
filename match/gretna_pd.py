import pandas as pd
from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    ColumnsIndex,
    DateSimilarity,
)

import deba
from lib.date import combine_date_columns
from lib.post import extract_events_from_post, load_for_agency


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
    decision = 0.817
    matcher.save_pairs_to_excel(
        deba.data("match/gretna_pd_pprr_2018_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "gretna-pd")


def match_pprr_and_post(pprr25, post):
    dfa = (
        pprr25.loc[pprr25.uid.notna(), ["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
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
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = .94
    matcher.save_pairs_to_excel(
        deba.data("match/pprr_gretna_pd_2021_2025_v_post.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    pprr25.loc[:, "uid"] = pprr25.uid.map(lambda x: match_dict.get(x, x))
    return pprr25


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_gretna_pd_2018.csv"))
    pprr25 = pd.read_csv(deba.data("clean/pprr_gretna_pd_2021_2025.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_events = extract_post_events(pprr, post)
    pprr25 = match_pprr_and_post(pprr25, post)
    post_events.to_csv(deba.data("match/post_event_gretna_pd_2020.csv"), index=False)
    pprr25.to_csv(deba.data("match/pprr_gretna_pd_2021_2025.csv"), index=False)
