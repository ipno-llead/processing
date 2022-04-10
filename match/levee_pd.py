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


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_levee_pd.csv"))
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
