from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import sys

sys.path.append("../")


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
        data_file_path(
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
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    cprr = pd.read_csv(data_file_path("clean/cprr_levee_pd.csv"))
    ensure_data_dir("match")
    pd.concat(
        [
            match_cprr_post(
                cprr[cprr.agency == "East Jefferson Levee PD"],
                post[post.agency == "e. jefferson levee pd"],
                "east_jefferson",
                2020,
                0.89,
            ),
            match_cprr_post(
                cprr[cprr.agency == "Orleans Levee PD"],
                post[post.agency == "orleans levee pd"],
                "orleans",
                2020,
                0.9,
            ),
        ]
    ).to_csv(data_file_path("match/cprr_levee_pd.csv"), index=False)
