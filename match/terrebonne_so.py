import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.post import load_for_agency
import deba


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
    decision = 0
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_terrebonne_2019_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


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
    decision = .953
    matcher.save_pairs_to_excel(
        deba.data("match/uof_terrebonne_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    uof.loc[:, "uid"] = uof.uid.map(lambda x: match_dict.get(x, x))
    return uof

def match_uof_22_and_post(uof22, post):
    dfa = (
        uof22.loc[uof22.uid.notna(), ["uid", "first_name", "last_name"]]
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
    decision = .96
    matcher.save_pairs_to_excel(
        deba.data("match/uof_terrebonne_2022_2024_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    uof22.loc[:, "uid"] = uof22.uid.map(lambda x: match_dict.get(x, x))
    return uof22

if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_terrebonne_so_2019_2021.csv"))
    uof = pd.read_csv(deba.data("clean/uof_terrebonne_so_2021.csv"))
    uof22 = pd.read_csv(deba.data("clean/uof_terrebonne_so_2022_2024.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = assign_uid_from_post(cprr, post)
    uof = match_uof_and_post(uof, post)
    uof22 = match_uof_22_and_post(uof22, post)
    cprr.to_csv(deba.data("match/cprr_terrebonne_so_2019_2021.csv"), index=False)
    uof.to_csv(deba.data("match/uof_terrebonne_so_2021.csv"), index=False)
    uof22.to_csv(deba.data("match/uof_terrebonne_so_2022_2024.csv"), index=False)
