import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
from lib.post import extract_events_from_post, load_for_agency
from lib.clean import canonicalize_officers


def deduplicate_cprr(cprr):
    df = cprr[["uid", "first_name", "last_name"]]
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
    decision = 0.950
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_cprr_rayne_2021.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def assign_uid_from_post(cprr, post):
    dfa = (
        cprr.loc[cprr.uid.notna(), ["uid", "first_name", "last_name"]]
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
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_rayne_pd_2019_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def extract_post_events(pprr, post):
    dfa = pprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index("uid", drop=True)

    dfb = post[["last_name", "first_name", "uid"]]
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
    decision = 0.981
    matcher.save_pairs_to_excel(
        deba.data("match/pprr_rayne_pd_2010_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "rayne-pd")



def assign_uid14_from_post(cprr, post):
    dfa = (
        cprr.loc[cprr.uid.notna(), ["uid", "first_name", "last_name"]]
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
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_rayne_pd_2019_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr



if __name__ == "__main__":
    cprr20 = pd.read_csv(deba.data("clean/cprr_rayne_pd_2019_2020.csv"))
    cprr14 = pd.read_csv(deba.data("clean/cprr_rayne_pd_2014_2018.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_rayne_pd_2010_2020.csv"))
    agency = cprr20.agency[0]
    post = load_for_agency(agency)
    cprr20 = deduplicate_cprr(cprr20)
    post_event = extract_post_events(pprr, post)
    cprr20 = assign_uid_from_post(cprr20, post)
    cprr14 = assign_uid14_from_post(cprr14, post)
    cprr20.to_csv(deba.data("match/cprr_rayne_pd_2019_2020.csv"), index=False)
    post_event.to_csv(
        deba.data("match/post_event_rayne_pd_2020_11_06.csv"), index=False
    )
    cprr14.to_csv(deba.data("match/cprr_rayne_pd_2014_2018.csv"), index=False)
