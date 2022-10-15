import pandas as pd
import deba
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.post import load_for_agency, extract_events_from_post


def match_cprr20_and_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_baker_pd_2018_2020_post_2020_11_06.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr17_and_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_baker_pd_2014_2017_post_2020_11_06.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_pprr_and_post(pprr, post):
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
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/pprr_baker_2010_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches,"baker-pd")


if __name__ == "__main__":
    cprr20 = pd.read_csv(deba.data("clean/cprr_baker_pd_2018_2020.csv"))
    cprr17 = pd.read_csv(deba.data("clean/cprr_baker_pd_2014_2017.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_baker_pd_2010_2020.csv"))
    agency = cprr20.agency[0]
    post = load_for_agency(agency)
    cprr20 = match_cprr20_and_post(cprr20, post)
    cprr17 = match_cprr17_and_post(cprr17, post)
    post_event = match_pprr_and_post(pprr, post)
    cprr20.to_csv(deba.data("match/cprr_baker_pd_2018_2020.csv"), index=False)
    post_event.to_csv(
        deba.data("match/post_event_baker_pd_2020_11_06.csv"), index=False
    )
    cprr17.to_csv(deba.data("match/cprr_baker_pd_2014_2017.csv"), index=False)
