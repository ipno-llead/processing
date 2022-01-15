import sys

sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.post import load_for_agency, extract_events_from_post


def match_cprr_and_post(cprr, post):
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
        data_file_path("match/cprr_baker_pd_2018_2020_post_2020_11_06.xlsx"), decision
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
        data_file_path("match/pprr_baker_2010_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "Baker PD")


if __name__ == "__main__":
    cprr = pd.read_csv(data_file_path("clean/cprr_baker_pd_2018_2020.csv"))
    pprr = pd.read_csv(data_file_path("clean/pprr_baker_pd_2010_2020.csv"))
    agency = cprr.agency[0]
    post = load_for_agency("clean/pprr_post_2020_11_06.csv", agency)
    cprr = match_cprr_and_post(cprr, post)
    post_event = match_pprr_and_post(pprr, post)
    cprr.to_csv(data_file_path("match/cprr_baker_pd_2018_2020.csv"), index=False)
    post_event.to_csv(data_file_path("match/post_event_baker_pd_2020_11_06.csv"), index=False)
