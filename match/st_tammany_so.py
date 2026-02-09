from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
from lib.post import extract_events_from_post, load_for_agency
import pandas as pd

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
        deba.data("match/deduplicate_cprr_st_tammany_2011_2021.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def match_cprr(cprr, pprr):
    dfa = (
        cprr[["first_name", "last_name", "uid"]]
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
    decision = 0.94
    matcher.save_pairs_to_excel(
        deba.data("match/st_tammany_so_cprr_2011_2021_v_pprr_2020.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)

    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr["uid"].map(lambda v: match_dict.get(v, v))
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
    decision = 0.955
    matcher.save_pairs_to_excel(
        deba.data("match/st_tammany_so_pprr_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "st-tammany-so")


def match_cprr_and_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
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
    decision = 0.929
    matcher.save_pairs_to_excel(
        deba.data("match/st_tammany_so_cprr_2011_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "st-tammany-so")

def match_pprr26_pprr20(pprr26, pprr):
    dfa = (
        pprr26[["first_name", "last_name", "uid"]]
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
    decision = .99
    matcher.save_pairs_to_excel(
        deba.data("match/st_tammany_so_pprr_2026_v_pprr_2020.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)

    match_dict = dict(matches)
    pprr26.loc[:, "uid"] = pprr26["uid"].map(lambda v: match_dict.get(v, v))
    return pprr26

def match_pprr26_and_post(pprr26, post):
    dfa = pprr26[["uid", "first_name", "last_name"]]
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
    decision = 0.89
    matcher.save_pairs_to_excel(
        deba.data("match/st_tammany_so_pprr_2026_v_post_pprr_2025_08_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "st-tammany-so")

def match_uof_and_post(uof25, post):
    dfa = uof25[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
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
    decision = 0.89
    matcher.save_pairs_to_excel(
        deba.data("match/st_tammany_so_uof_2025_v_post_pprr_2025_08_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    uof25.loc[:, "uid"] = uof25.uid.map(lambda x: match_dict.get(x, x))
    post_events = extract_events_from_post(post, matches, "st-tammany-so")
    return uof25, post_events

def match_uof24_and_post(uof24, post):
    dfa = uof24[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
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
    decision = 0.96
    matcher.save_pairs_to_excel(
        deba.data("match/st_tammany_so_uof_2022_2024_v_post_pprr_2025_08_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    uof24.loc[:, "uid"] = uof24.uid.map(lambda x: match_dict.get(x, x))
    post_events = extract_events_from_post(post, matches, "st-tammany-so")
    return uof24, post_events

if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_st_tammany_so_2011_2021.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_st_tammany_so_2020.csv"))
    pprr26 = pd.read_csv(deba.data("clean/pprr_st_tammany_so_2026.csv"))
    uof24 = pd.read_csv(deba.data("clean/uof_st_tammany_so_2022_2024.csv"))
    uof25 = pd.read_csv(deba.data("clean/uof_st_tammany_so_2025.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    cprr = deduplicate_cprr(cprr)
    cprr = match_cprr(cprr, pprr)
    pprr26 = match_pprr26_pprr20(pprr26, pprr)
    uof24, uof24_post_events = match_uof24_and_post(uof24, post)
    uof25, uof_post_events = match_uof_and_post(uof25, post)
    post_event = pd.concat(
        [match_pprr_and_post(pprr, post), match_cprr_and_post(cprr, post), uof24_post_events, uof_post_events]
    ).drop_duplicates(ignore_index=True)
    cprr.to_csv(deba.data("match/cprr_st_tammany_so_2011_2021.csv"), index=False)
    post_event.to_csv(deba.data("match/post_event_st_tammany_so_2020.csv"), index=False)
    pprr.to_csv(deba.data("match/pprr_st_tammany_so_2020.csv"), index=False)
    pprr26.to_csv(deba.data("match/pprr_st_tammany_so_2026.csv"), index=False)
    uof25.to_csv(deba.data("match/uof_st_tammany_so_2025.csv"), index=False)
    uof24.to_csv(deba.data("match/uof_st_tammany_so_2022_2024.csv"), index=False)
