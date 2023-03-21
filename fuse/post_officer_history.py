import pandas as pd
import deba
import warnings
from lib.columns import (
    rearrange_personnel_columns,
    rearrange_event_columns,
    rearrange_post_officer_history_columns,
)
from lib import events
from lib.personnel import fuse_personnel
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.clean import canonicalize_officers, names_to_title_case
from datamatch import (
    JaroWinklerSimilarity,
    ThresholdMatcher,
    ColumnsIndex,
)


def match_post_to_personnel(post, personnel):
    dfa = post[["uid", "first_name", "last_name", "middle_name", "agency"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = personnel[["uid", "first_name", "last_name", "middle_name", "agency"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc", "agency"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "middle_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = .804
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
    return df[~((df.hire_date.fillna("") == ""))]


def drop_rows_missing_agency(df):
    return df[~((df.agency.fillna("") == ""))]


def fuse_events(post):
    builder = events.Builder()
    builder.extract_events(
        post,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": [
                    "uid",
                    "agency",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "keep": [
                    "uid",
                    "left_reason",
                    "agency",
                ],
            },
        },
        ["uid"],
    )
    return builder.to_frame()


def deduplicate_personnel(personnel):
    df = personnel[["uid", "first_name", "middle_name", "last_name", "agency"]]
    df = df.drop_duplicates(subset=["uid"]).set_index("uid")
    df.loc[:, "fc"] = df.first_name.fillna("").map(lambda x: x[:1])
    df.loc[:, "lc"] = df.last_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc", "agency"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.986
    matcher.save_clusters_to_excel(
        deba.data("fuse/dedeuplicate_personnel.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(personnel, clusters)


if __name__ == "__main__":
    post = pd.read_csv(deba.data("clean/post_officer_history.csv"))
    per_pre_post = pd.read_csv(deba.data("fuse/personnel_pre_post.csv"))
    post = post_agency_is_per_agency_subset(per_pre_post, post)
    post = match_post_to_personnel(post, per_pre_post)
    post = post.pipe(drop_rows_missing_hire_dates).pipe(drop_rows_missing_agency)

    events_pre_post = pd.read_csv(deba.data("fuse/event_pre_post.csv"))

    post_events = fuse_events(post)
    
    event_df = pd.concat([post_events, events_pre_post], axis=0).drop_duplicates(
        subset=["event_uid"], keep="last"
    )
    
    per_df = fuse_personnel(per_pre_post, post)
    event_df = rearrange_event_columns(events_pre_post)
    per_df = rearrange_personnel_columns(per_df)

    per_df = per_df[~((per_df.last_name.fillna("") == ""))]
    per_df = per_df[~((per_df.agency.fillna("") == ""))]

    per_df = deduplicate_personnel(per_df)
    per_df = per_df.drop_duplicates(subset=["first_name", "middle_name", "last_name", "uid"])
    per_df = per_df.pipe(names_to_title_case, ["race", "sex"])

    event_df = event_df[~((event_df.agency.fillna("") == ""))]
    post = rearrange_post_officer_history_columns(post)

    per_df.to_csv(deba.data("fuse/personnel.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event.csv"), index=False)
    post.to_csv(deba.data("fuse/post_officer_history.csv"), index=False)
