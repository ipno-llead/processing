import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
import deba
from lib.post import load_for_agency


def assign_uid_from_post_25(uof, post):
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
        deba.data("match/assumption_so_2022_2026_v_post_pprr_2025_08_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    uof.loc[:, "uid"] = uof.uid.map(lambda x: match_dict.get(x, x))
    return uof

if __name__ == "__main__":
    uof = pd.read_csv(deba.data("clean/uof_assumption_so_2022_2026.csv"))
    agency = uof.agency[0]
    post = load_for_agency(agency)
    uof = assign_uid_from_post_25(uof, post)
    uof.to_csv(deba.data("match/uof_assumption_so_2022_2026.csv"), index=False)
