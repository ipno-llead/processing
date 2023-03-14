import pandas as pd
import deba
from lib.columns import (
    rearrange_personnel_columns,
    rearrange_event_columns,
    rearrange_post_officer_history_columns,
)
from lib import events

from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.clean import canonicalize_officers, names_to_title_case

def fuse_events(post):
    builder = events.Builder()
    builder.extract_events(
        post,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                ],
            },
            events.OFFICER_LEFT: {
                "prefix": "left",
                "parse_date": True,
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
    per_df = pd.read_csv(deba.data("fuse/personnel.csv"))
    allegation_df = pd.read_csv(deba.data("fuse/allegation.csv"))
    uof_df = pd.read_csv(deba.data("fuse/use_of_force.csv"))
    event_df = pd.read_csv(deba.data("fuse/event.csv"))
    sas_df = pd.read_csv(deba.data("fuse/stop_and_search.csv"))
    appeals_df = pd.read_csv(deba.data("fuse/appeals.csv"))
    
    allegation_df = allegation_df[allegation_df["uid"].isin(per_df["uid"])]
    uof_df = uof_df[uof_df["uid"].isin(per_df["uid"])]
    event_df = event_df[event_df["uid"].isin(per_df["uid"])]
    sas_df = sas_df[sas_df["uid"].isin(per_df["uid"])]
    appeals_df = appeals_df[appeals_df["uid"].isin(per_df["uid"])]

    event_df = event_df[~((event_df.agency.fillna("") == ""))]
    # post = rearrange_post_officer_history_columns(post)

    per_df.to_csv(deba.data("consolidate/personnel.csv"), index=False)
    event_df.to_csv(deba.data("consolidate/event.csv"), index=False)
    uof_df.to_csv(deba.data("consolidate/use_of_force.csv"), index=False)
    event_df.to_csv(deba.data("consolidate/event.csv"), index=False)
    sas_df.to_csv(deba.data("consolidate/stop_and_search.csv"), index=False)
    appeals_df.to_csv(deba.data("consolidate/appeals.csv"), index=False)
