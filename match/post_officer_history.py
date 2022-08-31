import warnings
import deba
import pandas as pd
from datamatch import (
    JaroWinklerSimilarity,
    ThresholdMatcher,
    ColumnsIndex,
)
from lib.clean import clean_dates


def match_post_to_personnel(post, personnel):
    dfa = post[["uid", "first_name", "last_name", "middle_name", "agency"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = personnel[["uid", "first_name", "last_name", "middle_name", "agency"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "agency"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "middle_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.95
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_post_ohr_personnel.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    post.loc[:, "uid"] = post.uid.map(lambda x: match_dict.get(x, x))
    return post


def post_agency_is_per_agency_subset(personnel, post):
    missing_agency = post[~post["agency"].isin(personnel["agency"])]
    missing_agency = missing_agency[["agency"]].drop_duplicates().dropna()

    if len(missing_agency["agency"]) != 0:
        warnings.warn(
            "Agency not in Personnel DF: %s" % missing_agency["agency"].tolist()
        )
    return post


def drop_rows_missing_hire_dates(df):
    return df[~(df.hire_date.fillna("") == "")]


if __name__ == "__main__":
    post = pd.read_csv(deba.data("clean/post_officer_history.csv"))
    personnel = pd.read_csv(deba.data("fuse/personnel_pre_post.csv"))
    post = post_agency_is_per_agency_subset(personnel, post)
    post = match_post_to_personnel(post, personnel)
    post = post.pipe(drop_rows_missing_hire_dates).pipe(
        clean_dates, ["hire_date", "left_date"]
    )
    post.to_csv(deba.data("match/post_officer_history.csv"), index=False)
