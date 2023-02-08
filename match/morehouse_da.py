
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
import pandas as pd

from lib.post import load_for_agency


def match_brady_to_post(brady, post):
    dfa = brady[["uid", "first_name", "last_name", "agency"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name", "agency"]]
    dfb.loc[:, "first_name"] = dfb.first_name.str.lower().str.strip()
    dfb.loc[:, "last_name"] = dfb.last_name.str.lower().str.strip()
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "agency"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/brady_morehouse_da_2022_v_personnel.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    brady.loc[:, "uid"] = brady.uid.map(lambda x: match_dict.get(x, x))
    return brady


if __name__ == "__main__":
    personnel = pd.read_csv(deba.data("fuse/personnel_pre_post.csv"))
    brady = pd.read_csv(deba.data("clean/brady_morehouse_da_2022.csv"))
    brady = match_brady_to_post(brady, personnel)
    brady.to_csv(deba.data("match/brady_morehouse_da_2022.csv"), index=False)



