from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
import pandas as pd

from lib.post import load_for_agency


def match_cprr_to_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name", "agency"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name", "agency"]]
    dfb.loc[:, "first_name"] = dfb.first_name.str.lower().str.strip()
    dfb.loc[:, "last_name"] = dfb.last_name.str.lower().str.strip()
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
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_iberia_so_2019_2021_v_post.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    post = load_for_agency("iberia-so")
    cprr = pd.read_csv(deba.data("clean/iberia_so_cprr_2019_2021.csv"))
    cprr = match_cprr_to_post(cprr, post)
    cprr.to_csv(deba.data("match/iberia_so_cprr_2019_2021.csv"), index=False)
