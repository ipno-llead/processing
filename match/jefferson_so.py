from datamatch.indices import ColumnsIndex

from datamatch import ThresholdMatcher, JaroWinklerSimilarity
import pandas as pd
import deba
from lib.post import extract_events_from_post, load_for_agency
from lib.clean import canonicalize_officers


def deduplicate_pprr(pprr):
    df = pprr[["uid", "first_name", "last_name", "middle_name"]]
    df = df.drop_duplicates(subset=["uid"]).set_index("uid")
    df.loc[:, "fc"] = df.first_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.978
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_pprr_jefferson_so.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(pprr, clusters)


def extract_post_events(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.912
    matcher.save_pairs_to_excel(
        deba.data("match/pprr_jefferson_so_2020_pprr_v_post_11_06_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Jefferson SO")


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_jefferson_so_2020.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_events = extract_post_events(pprr, post)
    pprr = deduplicate_pprr(pprr)
    post_events.to_csv(deba.data("match/post_event_jefferson_so_2020.csv"), index=False)
    pprr.to_csv(deba.data("match/pprr_jefferson_so_2020.csv"), index=False)
