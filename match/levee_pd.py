from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba
from lib.post import load_for_agency
import pandas as pd


def match_cprr_post(cprr, post, agency, year, decision):
    dfa = cprr[["uid", "first_name", "last_name"]].drop_duplicates()
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.set_index("uid", drop=True)

    dfb = post[["uid", "first_name", "last_name"]].drop_duplicates()
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    matcher.save_pairs_to_excel(
        deba.data(
            "match/%s_levee_pd_cprr_%d_v_post_pprr_2020_11_06.xlsx" % (agency, year)
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    uid_dict = dict(matches)
    cprr.loc[cprr.uid.notna(), "uid"] = cprr.loc[cprr.uid.notna(), "uid"].map(
        lambda x: uid_dict.get(x, x)
    )
    return cprr

def match_pprr_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates().set_index("uid")
    dfa.loc[:,"fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates().set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.90
    matcher.save_pairs_to_excel(
        deba.data("match/levee_pd_pprr_1980_2025_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    pprr.loc[:, "uid"] = pprr.uid.map(lambda x: match_dict.get(x, x))
    return pprr 


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
    decision = .95
    matcher.save_pairs_to_excel(
        deba.data("match/uof_levee_2020_2025_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    uof.loc[:, "uid"] = uof.uid.map(lambda x: match_dict.get(x, x))
    return uof


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_levee_pd.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_levee_pd_1980_2025.csv"))
    uof = pd.read_csv(deba.data("clean/uof_levee_pd_2020_2025.csv"))

    post_east_jefferson = load_for_agency("east-jefferson-levee-pd")
    post_orleans = load_for_agency("orleans-levee-pd")


    matched_cprr = pd.concat(
        [
            match_cprr_post(
                cprr[cprr.agency == "east-jefferson-levee-pd"],
                post_east_jefferson,
                "east_jefferson",
                2020,
                0.89,
            ),
            match_cprr_post(
                cprr[cprr.agency == "orleans-levee-pd"],
                post_orleans,
                "orleans",
                2020,
                0.9,
            ),
        ]
    )
    matched_cprr.to_csv(deba.data("match/cprr_levee_pd.csv"), index=False)
    pprr = match_pprr_post(pprr, post_orleans)
    uof = match_uof_and_post(uof, post_orleans)
    pprr.to_csv(deba.data("match/pprr_levee_pd_1980_2025.csv"), index=False)
    uof.to_csv(deba.data("match/uof_levee_pd_2020_2025.csv"), index=False)
