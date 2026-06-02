import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
import deba
from lib.post import load_for_agency


def match_uof_and_post(uof, post):
    dfa = (
        uof.loc[uof.uid.notna(), ["uid", "first_name", "last_name"]]
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
    decision = 0.94
    matcher.save_pairs_to_excel(
        deba.data("match/vermilion_so_uof_2023_2025_v_post_pprr_2025_08_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    uof.loc[:, "uid"] = uof.uid.map(lambda x: match_dict.get(x, x))
    return uof


if __name__ == "__main__":
    uof = pd.read_csv(deba.data("clean/uof_vermilion_so_2023_2025.csv"))
    agency = uof.agency[0]
    post = load_for_agency(agency)
    uof = match_uof_and_post(uof, post)
    uof.to_csv(deba.data("match/uof_vermilion_so_2023_2025.csv"), index=False)
