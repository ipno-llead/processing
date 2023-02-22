import argparse
from datetime import datetime
import pathlib

import numpy as np
import pandas as pd
from datamatch import (
    ThresholdMatcher,
    DissimilarFilter,
    NonOverlappingFilter,
    ColumnsIndex,
    JaroWinklerSimilarity,
    MaxScorer,
    SimSumScorer,
    AlterScorer,
    MultiIndex,
    AbsoluteScorer,
)
from datavalid.spinner import Spinner
import deba

from lib.date import combine_date_columns

common_names = [
    "Michael Smith",
]


def discard_rows(
    events: pd.DataFrame, bool_index: pd.Series, desc: str, reset_index: bool = False
) -> pd.DataFrame:
    before = events.shape[0]
    events = events[bool_index]
    if reset_index:
        events = events.reset_index(drop=True)
    after = events.shape[0]
    if before > after:
        print(
            "discarded %d %s (%.1f%%)"
            % (before - after, desc, (before - after) / before * 100)
        )
    return events


def assign_min_col(events: pd.DataFrame, per: pd.DataFrame, col: str):
    min_dict = events[col].min(level="uid").to_dict()
    per.loc[:, "min_" + col] = per.index.map(lambda x: min_dict.get(x, np.NaN))


def assign_max_col(events: pd.DataFrame, per: pd.DataFrame, col: str):
    max_dict = events[col].max(level="uid").to_dict()
    per.loc[:, "max_" + col] = per.index.map(lambda x: max_dict.get(x, np.NaN))


def read_constraints():
    # TODO: replace this line with the real constraints data
    constraints = pd.DataFrame([], columns=["uids", "kind"])
    print("read constraints (%d rows)" % constraints.shape[0])
    records = dict()
    for idx, row in constraints.iterrows():
        uids = row.uids.split(",")
        for uid in uids:
            if row.kind == "attract":
                records.setdefault(uid, dict())["attract_id"] = idx
            elif row.kind == "repell":
                records.setdefault(uid, dict())["repell_id"] = idx
    return pd.DataFrame.from_records(records)


def read_post():
    post = pd.read_csv(deba.data("match/post_officer_history.csv"))
    post = post.drop_duplicates(subset=["uid"])
    print("read post officer history file (%d rows)" % post.shape[0])
    return post


def cross_match_officers_between_agencies(personnel, events, constraints, post):
    events = discard_rows(
        events, events.uid.notna(), "events with empty uid column", reset_index=True
    )
    events = discard_rows(
        events, events.day.notna(), "events with empty day column", reset_index=True
    )
    events = discard_rows(
        events, events.day <= 31, "events with impossible day column", reset_index=True
    )
    for col in ["year", "month", "day"]:
        events.loc[:, col] = events[col].astype(int)
    events.loc[:, "date"] = combine_date_columns(events, "year", "month", "day")
    events = discard_rows(
        events, events.date.notna(), "events with empty date", reset_index=True
    )
    events.loc[:, "timestamp"] = events["date"].map(lambda x: x.timestamp())

    per = personnel[["uid", "first_name", "last_name"]]
    per = discard_rows(
        per,
        per.first_name.notna() & per.last_name.notna(),
        "officers without either first name or last name",
        reset_index=True,
    )
    per.loc[:, "fc"] = per.first_name.map(lambda x: x[:1])
    per.loc[:, "lc"] = per.last_name.map(lambda x: x[:1])
    agency_dict = (
        events.loc[:, ["uid", "agency"]]
        .drop_duplicates()
        .set_index("uid")
        .agency.to_dict()
    )
    per.loc[:, "agency"] = per.uid.map(lambda x: agency_dict.get(x, ""))
    per = discard_rows(
        per, per.agency != "", "officers not linked to any event", reset_index=True
    )

    per = pd.concat([per, post], axis=0)

    per = discard_rows(
        per, per.switched_job.fillna(True), "officers who have not switched jobs"
    )
    per = per.drop(columns=["switched_job"]).drop_duplicates(
        subset=["uid"], keep="last"
    )

    per = per.set_index("uid")
    per = per.join(constraints)

    # aggregating min/max date
    events = events.set_index(["uid", "event_uid"])
    assign_min_col(events, per, "date")
    assign_max_col(events, per, "date")
    assign_min_col(events, per, "timestamp")
    assign_max_col(events, per, "timestamp")
    per = discard_rows(per, per.min_date.notna(), "officers with no event")

    # concatenate first name and last name to get a series of full names
    full_names = per.first_name.str.cat(per.last_name, sep=" ")
    # filter down the full names to only those that are common
    common_names_sr = full_names[full_names.isin(common_names)]

    excel_path = deba.data("match/cross_agency_officers.xlsx")
    matcher = ThresholdMatcher(
        index=MultiIndex(
            [
                # or if they are in the same attract constraint
                ColumnsIndex("attract_id", ignore_key_error=True),
                # or if they are in the same history constraingt
                ColumnsIndex("history_id", ignore_key_error=True),
            ]
        ),
        scorer=MaxScorer(
            [
                # but if two officers belong to the same attract constraint then give them the highest score regardless
                AbsoluteScorer("attract_id", 1, ignore_key_error=True),
                AbsoluteScorer("history_id", 1, ignore_key_error=True),
            ]
        ),
        dfa=per,
        filters=[
            # don't match officers who belong in the same agency
            DissimilarFilter("agency"),
            # don't match officers who are in the same repell constraint
            DissimilarFilter("repell_id", ignore_key_error=True),
            # don't match officers who appear in overlapping time ranges
            NonOverlappingFilter("min_timestamp", "max_timestamp"),
        ],
        show_progress=True,
    )
    decision = 0.98
    with Spinner("saving matched clusters to Excel file"):
        matcher.save_clusters_to_excel(excel_path, decision, lower_bound=decision)
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    print("saved %d clusters to %s" % (len(clusters), excel_path))

    return clusters, per[["max_timestamp", "agency"]]


def create_person_table(clusters, personnel, personnel_event):
    # add back unmatched officers into clusters list
    matched_uids = frozenset().union(*[s for s in clusters])

    clusters = [sorted(list(cluster)) for cluster in clusters] + [
        [uid]
        for uid in personnel.loc[~personnel.uid.isin(matched_uids), "uid"].tolist()
    ]
    print(
        "added back unmatched officers into list of clusters (now total %d)"
        % len(clusters)
    )

    # clusters are sorted by the smallest uid
    clusters = sorted(clusters, key=lambda x: x[0])

    # assign canonical_uid and person_id
    person_df = (
        pd.DataFrame.from_records(
            [
                {"canonical_uid": uid, "uids": cluster, "person_id": idx + 1}
                for idx, cluster in enumerate(clusters)
                for uid in cluster
            ]
        )
        .merge(personnel_event, how="left", left_on="canonical_uid", right_index=True)
        .sort_values(
            ["person_id", "max_timestamp", "agency"], ascending=[True, False, True]
        )
        .drop_duplicates(subset=["person_id"], keep="first")
    )

    # join uids with comma
    person_df.loc[:, "uids"] = person_df.uids.str.join(",")

    return person_df[["person_id", "canonical_uid", "uids"]]


def entity_resolution(
    old_person: pd.DataFrame, new_person: pd.DataFrame
) -> pd.DataFrame:
    dfa = new_person.set_index("person_id", drop=True)
    dfa.loc[:, "uids"] = dfa.uids.str.split(pat=",").map(lambda x: set(x))

    dfb = old_person.set_index("person_id", drop=True)
    dfb.loc[:, "uids"] = dfb.uids.str.split(pat=",").map(lambda x: set(x))

    def person_scorer(a: pd.Series, b: pd.Series) -> float:
        if a.canonical_uid == b.canonical_uid:
            return 1
        x_len = len(a.uids & b.uids)
        return x_len * 2 / (len(a.uids) + len(b.uids))

    print("matching old and new person table...")
    matcher = ThresholdMatcher(
        index=ColumnsIndex("uids", index_elements=True),
        scorer=person_scorer,
        dfa=dfa,
        dfb=dfb,
        show_progress=True,
    )
    decision = 0.001
    with Spinner("saving person entity resolution to Excel file"):
        matcher.save_pairs_to_excel(
            name=deba.data(
                "match/person_entity_resolution_%s.xlsx"
                % datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            ),
            match_threshold=decision,
            lower_bound=0,
            include_exact_matches=False,
        )
    pairs = matcher.get_index_pairs_within_thresholds(decision)
    pairs_dict = dict(pairs)

    def new_person_ids():
        per_id = old_person.person_id.max()
        while True:
            per_id += 1
            yield per_id

    # get person_id from the old table, if not found, assign a completely new id
    gen = new_person_ids()
    new_person.loc[:, "person_id"] = new_person.person_id.map(
        lambda x: pairs_dict.get(x, next(gen))
    )
    return new_person


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Match officer profiles cross-agency to produce person table"
    )
    parser.add_argument(
        "person_csv",
        type=pathlib.Path,
        metavar="PERSON_CSV",
        help="The previous person data",
    )
    parser.add_argument(
        "--new-person-csv",
        type=pathlib.Path,
        metavar="NEW_PERSON_CSV",
        default=None,
        help="The current person data (specifying this skip the clustering)",
    )
    args = parser.parse_args()

    old_person_df = pd.read_csv(args.person_csv)
    if args.new_person_csv is not None:
        new_person_df = pd.read_csv(args.new_person_csv)
        person_df = entity_resolution(
            old_person=old_person_df, new_person=new_person_df
        )
        person_df.to_csv(deba.data("fuse/person.csv"), index=False)
    else:
        personnel = pd.read_csv(deba.data("fuse/personnel.csv"))
        print("read personnel file (%d x %d)" % personnel.shape)
        events = pd.read_csv(deba.data("fuse/event.csv"))
        print("read events file (%d x %d)" % events.shape)
        constraints = read_constraints()
        post = read_post()
        clusters, personnel_event = cross_match_officers_between_agencies(
            personnel, events, constraints, post
        )
        new_person_df = create_person_table(clusters, personnel, personnel_event)
        new_person_df.to_csv(deba.data("fuse/person.csv"), index=False)
