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
    #return extract_events_from_post(post, matches,"shreveport-pd")


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_levee_pd.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_levee_pd_1980_2025.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)

    matched_cprr = pd.concat(
        [
            match_cprr_post(
                cprr[cprr.agency == "East Jefferson Levee PD"],
                post,
                "east_jefferson",
                2020,
                0.89,
            ),
            match_cprr_post(
                cprr[cprr.agency == "New Orleans Levee PD"],
                post,
                "orleans",
                2020,
                0.9,
            ),
        ]
    )
    matched_cprr.to_csv(deba.data("match/cprr_levee_pd.csv"), index=False)
    pprr = match_pprr_post(pprr, post)
    pprr.to_csv(deba.data("match/pprr_levee_pd_1980_2025.csv"), index=False)
