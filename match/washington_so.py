import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
import deba
from lib.post import load_for_agency


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
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.86
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_washington_so_2015_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

def assign_uid_from_post_24(cprr24, post):
    dfa = (
        cprr24.loc[cprr24.uid.notna(), ["uid", "first_name", "last_name"]]
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
    decision = 0.98
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_washington_so_2023_2025_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr24.loc[:, "uid"] = cprr24.uid.map(lambda x: match_dict.get(x, x))
    return cprr24


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_washington_so_2010_2022.csv"))
    cprr24 = pd.read_csv(deba.data("clean/cprr_washington_so_2023_2025.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = assign_uid_from_post(cprr, post)
    cprr24 = assign_uid_from_post_24(cprr24, post)
    cprr.to_csv(deba.data("match/cprr_washington_so_2010_2022.csv"), index=False)
    cprr24.to_csv(deba.data("match/cprr_washington_so_2023_2025.csv"), index=False)
