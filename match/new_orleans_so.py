import pandas as pd
from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    ColumnsIndex,
    Swap,
    DateSimilarity,
)
import dirk
from lib.post import extract_events_from_post, load_for_agency
from lib.date import combine_date_columns


def deduplicate_cprr_19_personnel(cprr):
    df = (
        cprr.loc[
            cprr.uid.notna(),
            ["employee_id", "first_name", "last_name", "middle_initial", "uid"],
        ]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )

    matcher = ThresholdMatcher(
        ColumnsIndex("employee_id"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
        variator=Swap("first_name", "last_name"),
    )
    decision = 0.9
    matcher.save_clusters_to_excel(
        dirk.data("match/new_orleans_so_cprr_19_dedup.xlsx"), decision, decision
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    # canonicalize name and uid
    for cluster in clusters:
        uid, first_name, last_name, middle_initial = None, "", "", ""
        for idx in cluster:
            row = df.loc[idx]
            if (
                uid is None
                or len(row.first_name) > len(first_name)
                or (
                    len(row.first_name) == len(first_name)
                    and len(row.last_name) > len(last_name)
                )
            ):
                uid, first_name, last_name, middle_initial = (
                    idx,
                    row.first_name,
                    row.last_name,
                    row.middle_initial,
                )
        cprr.loc[cprr.uid.isin(cluster), "uid"] = uid
        cprr.loc[cprr.uid == uid, "first_name"] = first_name
        cprr.loc[cprr.uid == uid, "last_name"] = last_name
        cprr.loc[cprr.uid == uid, "middle_initial"] = middle_initial
    return cprr


def deduplicate_cprr_20_personnel(cprr):
    df = (
        cprr.loc[
            cprr.uid.notna(),
            ["employee_id", "first_name", "last_name", "middle_name", "uid"],
        ]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )

    matcher = ThresholdMatcher(
        ColumnsIndex("employee_id"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
        variator=Swap("first_name", "last_name"),
    )
    decision = 0.9
    matcher.save_clusters_to_excel(
        dirk.data("match/new_orleans_so_cprr_20_dedup.xlsx"), decision, decision
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    # canonicalize name and uid
    for cluster in clusters:
        uid, first_name, last_name, middle_name = None, "", "", ""
        for idx in cluster:
            row = df.loc[idx]
            if (
                uid is None
                or len(row.first_name) > len(first_name)
                or (
                    len(row.first_name) == len(first_name)
                    and len(row.last_name) > len(last_name)
                )
            ):
                uid, first_name, last_name, middle_name = (
                    idx,
                    row.first_name,
                    row.last_name,
                    row.middle_name,
                )
        cprr.loc[cprr.uid.isin(cluster), "uid"] = uid
        cprr.loc[cprr.uid == uid, "first_name"] = first_name
        cprr.loc[cprr.uid == uid, "last_name"] = last_name
        cprr.loc[cprr.uid == uid, "middle_name"] = middle_name
    return cprr


def assign_uid_19_from_pprr(cprr, pprr):
    dfa = (
        cprr.loc[cprr.uid.notna(), ["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.921
    matcher.save_pairs_to_excel(
        dirk.data("match/new_orleans_so_cprr_19_officer_v_noso_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def assign_uid_20_from_pprr(cprr, pprr):
    dfa = (
        cprr.loc[cprr.uid.notna(), ["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.953
    matcher.save_pairs_to_excel(
        dirk.data("match/new_orleans_so_cprr_20_officer_v_noso_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def assign_supervisor_19_uid_from_pprr(cprr, pprr):
    dfa = cprr.loc[
        cprr.supervisor_first_name.notna(),
        ["supervisor_first_name", "supervisor_last_name"],
    ].drop_duplicates()
    dfa = dfa.rename(
        columns={
            "supervisor_first_name": "first_name",
            "supervisor_last_name": "last_name",
        }
    )
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.958
    matcher.save_pairs_to_excel(
        dirk.data("match/new_orleans_so_cprr_19_supervisor_v_noso_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "supervisor_uid"] = cprr.index.map(lambda x: match_dict.get(x, ""))
    return cprr


def assign_supervisor_20_uid_from_pprr(cprr, pprr):
    dfa = cprr.loc[
        cprr.supervisor_first_name.notna(),
        ["supervisor_first_name", "supervisor_last_name"],
    ].drop_duplicates()
    dfa = dfa.rename(
        columns={
            "supervisor_first_name": "first_name",
            "supervisor_last_name": "last_name",
        }
    )
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.948
    matcher.save_pairs_to_excel(
        dirk.data("match/new_orleans_so_cprr_20_supervisor_v_noso_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "supervisor_uid"] = cprr.index.map(lambda x: match_dict.get(x, ""))
    return cprr


def match_pprr_against_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day"
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "hire_date": DateSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.894
    matcher.save_pairs_to_excel(
        dirk.data("match/new_orleans_so_pprr_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )

    matches = matcher.get_index_pairs_within_thresholds(decision)
    return extract_events_from_post(post, matches, "New Orleans SO")


if __name__ == "__main__":
    cprr19 = pd.read_csv(dirk.data("clean/cprr_new_orleans_so_2019.csv"))
    cprr20 = pd.read_csv(dirk.data("clean/cprr_new_orleans_so_2020.csv"))
    agency = cprr20.agency[0]
    post = load_for_agency(agency)
    pprr = pd.read_csv(dirk.data("clean/pprr_new_orleans_so_2021.csv"))

    cprr19 = deduplicate_cprr_19_personnel(cprr19)
    cprr20 = deduplicate_cprr_20_personnel(cprr20)
    cprr19 = assign_uid_19_from_pprr(cprr19, pprr)
    cprr20 = assign_uid_20_from_pprr(cprr20, pprr)
    cprr19 = assign_supervisor_19_uid_from_pprr(cprr19, pprr)
    cprr20 = assign_supervisor_20_uid_from_pprr(cprr20, pprr)
    post_events = match_pprr_against_post(pprr, post)
    cprr19.to_csv(dirk.data("match/cprr_new_orleans_so_2019.csv"), index=False)
    cprr20.to_csv(dirk.data("match/cprr_new_orleans_so_2020.csv"), index=False)
    post_events.to_csv(dirk.data("match/post_event_new_orleans_so.csv"), index=False)
