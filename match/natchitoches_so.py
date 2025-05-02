import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex, MaxScorer, AlterScorer, SimSumScorer
import deba
from lib.post import load_for_agency


def match_cprr_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
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
    decision = 0.95
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_natchitoches_so_2018_2021_v_pprr_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

def match_cprr_25_post(cprr25, post):

    common_last_names = (
        post["last_name"]
        .value_counts()
        .pipe(lambda x: x[x > 1])
        .index.tolist()
    )

    post_uncommon = post[~post["last_name"].isin(common_last_names)].copy()
    cprr_uncommon = cprr25[~cprr25["last_name"].isin(common_last_names)].copy()

    dfa = cprr_uncommon[["uid", "agency", "last_name"]].drop_duplicates(subset=["uid"]).set_index("uid")
    dfb = post_uncommon[["uid", "agency", "last_name"]].drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex("agency", "last_name"),
        {
            "agency": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )

    decision = 0.92
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_natchitoches_so_2022_2025_v_pprr_post_2020_11_06.xlsx"),
        decision,
    )

    matches = matcher.get_index_pairs_within_thresholds(decision)

    match_dict = dict(matches)
    cprr25.loc[:, "uid"] = cprr25["uid"].map(lambda x: match_dict.get(x, x))

    return cprr25



if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_natchitoches_so_2018_21.csv"))
    cprr25 = pd.read_csv(deba.data("clean/cprr_natchitoches_so_2022_25.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = match_cprr_post(cprr, post)
    cprr25 = match_cprr_25_post(cprr25, post)
    cprr.to_csv(deba.data("match/cprr_natchitoches_so_2018_21.csv"), index=False)
    cprr = cprr.fillna("")
    cprr25.to_csv(deba.data("match/cprr_natchitoches_so_2022_25.csv"), index=False)
    cprr25 = cprr25.fillna("")
