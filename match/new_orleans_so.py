import pandas as pd
from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    ColumnsIndex,
    DateSimilarity,
)
import deba
from lib.post import extract_events_from_post, load_for_agency
from lib.date import combine_date_columns
from lib.clean import canonicalize_officers


def deduplicate_cprr_19_personnel(cprr):
    df = (
        cprr.loc[
            cprr.uid.notna(),
            ["employee_id", "first_name", "last_name", "middle_name", "uid"],
        ]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )

    matcher = ThresholdMatcher(
        ColumnsIndex("employee_id"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.9
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_new_orleans_so_cprr_19.xlsx"), decision, decision
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def deduplicate_cprr_20_personnel(cprr):
    df = (
        cprr.loc[
            cprr.uid.notna(),
            ["employee_id", "first_name", "last_name", "middle_name", "uid"],
        ]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )

    matcher = ThresholdMatcher(
        ColumnsIndex("employee_id"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.9
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_new_orleans_so_cprr_20.xlsx"), decision, decision
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    # canonicalize name and uid
    return canonicalize_officers(cprr, clusters)


def assign_uid_19_from_pprr(cprr, pprr):
    dfa = (
        cprr.loc[cprr.uid.notna(), ["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
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
    decision = 0.921
    matcher.save_pairs_to_excel(
        deba.data("match/new_orleans_so_cprr_19_officer_v_noso_pprr_2021.xlsx"),
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
        .drop_duplicates(subset=["uid"])
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
        deba.data("match/new_orleans_so_cprr_20_officer_v_noso_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def assign_supervisor_19_uid_from_pprr(cprr, pprr):
    dfa = cprr.loc[
        cprr.investigating_supervisor_first_name.notna(),
        ["investigating_supervisor_first_name", "investigating_supervisor_last_name"],
    ].drop_duplicates()
    dfa = dfa.rename(
        columns={
            "investigating_supervisor_first_name": "first_name",
            "investigating_supervisor_last_name": "last_name",
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
        deba.data("match/new_orleans_so_cprr_19_supervisor_v_noso_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "investigating_supervisor_uid"] = cprr.index.map(lambda x: match_dict.get(x, ""))
    return cprr


def assign_supervisor_20_uid_from_pprr(cprr, pprr):
    dfa = cprr.loc[
        cprr.investigating_supervisor_first_name.notna(),
        ["investigating_supervisor_first_name", "investigating_supervisor_last_name"],
    ].drop_duplicates()
    dfa = dfa.rename(
        columns={
            "investigating_supervisor_first_name": "first_name",
            "investigating_supervisor_last_name": "last_name",
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
        deba.data("match/new_orleans_so_cprr_20_supervisor_v_noso_pprr_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "investigating_supervisor_uid"] = cprr.index.map(lambda x: match_dict.get(x, ""))
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
        deba.data("match/new_orleans_so_pprr_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )

    matches = matcher.get_index_pairs_within_thresholds(decision)
    return extract_events_from_post(post, matches, "new-orleans-so")


def match_cprr_21_w_pprr(cprr, pprr):
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
    decision = 0.931
    matcher.save_pairs_to_excel(
        deba.data("match/cprr_orleans_so_2021_v_pprr.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def deduplicate_cprr_21_officers(cprr):
    df = cprr[["uid", "first_name", "last_name", "rank_desc"]]
    df = df.drop_duplicates(subset=["uid"]).set_index("uid")
    df.loc[:, "fc"] = df.first_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.950
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_orleans_so_cprr_21.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(cprr, clusters)


def match_overtime_w_pprr(cprr, pprr):
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
    decision = 0.952
    matcher.save_pairs_to_excel(
        deba.data("match/pprr_overtime_new_orleans_so_2020_v_pprr.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    cprr19 = pd.read_csv(deba.data("clean/cprr_new_orleans_so_2019.csv"))
    cprr20 = pd.read_csv(deba.data("clean/cprr_new_orleans_so_2020.csv"))
    cprr21 = pd.read_csv(deba.data("clean/cprr_new_orleans_so_2021.csv"))
    agency = cprr20.agency[0]
    post = load_for_agency(agency)
    pprr = pd.read_csv(deba.data("clean/pprr_new_orleans_so_2021.csv"))
    overtime20 = pd.read_csv(deba.data("clean/pprr_overtime_new_orleans_so_2020.csv"))
    cprr19 = deduplicate_cprr_19_personnel(cprr19)
    cprr20 = deduplicate_cprr_20_personnel(cprr20)
    cprr21 = deduplicate_cprr_21_officers(cprr21)
    cprr19 = assign_uid_19_from_pprr(cprr19, pprr)
    cprr20 = assign_uid_20_from_pprr(cprr20, pprr)
    cprr21 = match_cprr_21_w_pprr(cprr21, pprr)
    cprr19 = assign_supervisor_19_uid_from_pprr(cprr19, pprr)
    cprr20 = assign_supervisor_20_uid_from_pprr(cprr20, pprr)
    overtime20 = match_overtime_w_pprr(overtime20, pprr)
    post_events = match_pprr_against_post(pprr, post)
    cprr19.to_csv(deba.data("match/cprr_new_orleans_so_2019.csv"), index=False)
    cprr20.to_csv(deba.data("match/cprr_new_orleans_so_2020.csv"), index=False)
    cprr21.to_csv(deba.data("match/cprr_new_orleans_so_2021.csv"), index=False)
    post_events.to_csv(deba.data("match/post_event_new_orleans_so.csv"), index=False)
    overtime20.to_csv(deba.data("match/pprr_overtime_new_orleans_so_2020.csv"))
