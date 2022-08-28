from lib.date import combine_date_columns
import deba
from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    DateSimilarity,
    ColumnsIndex,
)
from lib.post import extract_events_from_post, load_for_agency
import pandas as pd
from lib.clean import canonicalize_officers, float_to_int_str


def deduplicate_pprr(pprr):
    df = (
        pprr.loc[
            pprr.uid.notna(),
            ["employee_id", "first_name", "last_name", "uid"],
        ]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    df.loc[:, "fc"] = df.first_name.fillna().map(lambda x: x[:1])

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
        deba.data("match/deduplicate_pprr_new_orleans_pd.xlsx"), decision, decision
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(pprr, clusters)


def deduplicate_award(award):
    df = award[["uid", "first_name", "last_name", "middle_name"]]
    df = df.drop_duplicates(subset=["uid"]).set_index("uid")
    df.loc[:, "fc"] = df.first_name.fillna("").map(lambda x: x[:1])
    df.loc[:, "mc"] = df.middle_name.fillna("").map(lambda x: x[:1])
    df.loc[:, "lc"] = df.last_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc", "mc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.983
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_award_new_orleans_pd.xlsx"), decision, decision
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(award, clusters)


def match_pprr_against_post(pprr_ipm, post):
    dfa = pprr_ipm[["uid", "first_name", "last_name"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr_ipm, "hire_year", "hire_month", "hire_day"
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
    decision = 0.803
    matcher.save_pairs_to_excel(
        deba.data(
            "match/pppr_ipm_new_orleans_pd_1946_2018_v_post_pprr_2020_11_06.xlsx"
        ),
        decision,
    )

    matches = matcher.get_index_pairs_within_thresholds(decision)
    return extract_events_from_post(post, matches, "New Orleans PD")


def match_award_to_pprr_ipm(award, pprr_ipm):
    dfa = (
        award[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr_ipm[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.93
    matcher.save_pairs_to_excel(
        deba.data(
            "match/new_orleans_pd_award_2016_2021_v_pprr_ipm_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    match_dict = dict(matches)
    award.loc[:, "uid"] = award.uid.map(lambda x: match_dict.get(x, x))
    return award


def match_lprr_to_pprr_ipm(lprr, pprr_ipm):
    dfa = lprr[["uid", "first_name", "last_name", "middle_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name", "middle_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "middle_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.80
    matcher.save_pairs_to_excel(
        deba.data(
            "match/new_orleans_lprr_2000_2016_v_pprr_ipm_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_clusters_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, "uid"] = lprr.uid.map(lambda x: match_dict.get(x, x))
    return lprr


def match_lprr_transcripts_to_pprr_ipm(lprr, pprr_ipm):
    dfa = lprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.80
    matcher.save_pairs_to_excel(
        deba.data(
            "match/new_orleans_lprr_transcripts_2015_2021_v_pprr_ipm_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_clusters_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, "uid"] = lprr.uid.map(lambda x: match_dict.get(x, x))
    return lprr


def match_pprr_csd_to_pprr_ipm(pprr_csd, pprr_ipm):
    dfa = pprr_csd[["uid", "first_name", "last_name", "agency"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr_csd, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name", "agency"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        pprr_ipm, "hire_year", "hire_month", "hire_day"
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc", "agency"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "hire_date": DateSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data(
            "match/pprr_new_orleans_csd_2014_v_pprr_ipm_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr_csd.loc[:, "uid"] = pprr_csd.uid.map(lambda x: match_dict.get(x, x))
    return pprr_csd


def match_stop_and_search_to_pprr(sas, pprr_ipm):
    dfa = sas[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.95

    matcher.save_pairs_to_excel(
        deba.data(
            "match/stop_and_search_new_orleans_pd_v_pprr_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    sas.loc[:, "uid"] = sas.uid.map(lambda x: match_dict.get(x, x))
    return sas


def match_use_of_force_to_pprr(uof, pprr_ipm):
    dfa = uof[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.958

    matcher.save_pairs_to_excel(
        deba.data(
            "match/uof_new_orleans_pd_2016_2021_v_pprr_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    uof.loc[:, "uid"] = uof.uid.map(lambda x: match_dict.get(x, x))
    return uof


def match_cprr_to_pprr(cprr, pprr_ipm):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.958

    matcher.save_pairs_to_excel(
        deba.data("match/cprr_new_orleans_pd_v_pprr_new_orleans_pd_1946_2018.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_pclaims20_to_pprr(pclaims, pprr_ipm):
    dfa = pclaims[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.931

    matcher.save_pairs_to_excel(
        deba.data(
            "match/pclaims_new_orleans_pd_2020_v_pprr_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pclaims.loc[:, "uid"] = pclaims.uid.map(lambda x: match_dict.get(x, x))
    return pclaims


def match_pclaims21_to_pprr(pclaims, pprr_ipm):
    dfa = pclaims[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.931

    matcher.save_pairs_to_excel(
        deba.data(
            "match/pclaims_new_orleans_pd_2020_v_pprr_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pclaims.loc[:, "uid"] = pclaims.uid.map(lambda x: match_dict.get(x, x))
    return pclaims


def match_pprr_separations_to_pprr(pprr_seps, pprr_ipm):
    dfa = pprr_seps[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr_ipm[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.985

    matcher.save_pairs_to_excel(
        deba.data(
            "match/pprr_seps_new_orleans_pd_2019-22_v_pprr_new_orleans_pd_1946_2018.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr_seps.loc[:, "uid"] = pprr_seps.uid.map(lambda x: match_dict.get(x, x))
    return pprr_seps


def join_pib_and_ipm():
    pib = pd.read_csv(
        deba.data("clean/cprr_new_orleans_pd_pib_reports_2014_2020.csv")
    ).drop_duplicates(subset=["tracking_id"], keep=False)
    ipm = pd.read_csv(
        deba.data("clean/cprr_new_orleans_pd_1931_2020.csv")
    ).drop_duplicates(subset=["tracking_id"], keep=False)

    df = pd.merge(pib, ipm, on="tracking_id", how="outer")
    df = df[~((df.allegation_desc.fillna("") == ""))]
    return (
        df[~((df.officer_primary_key.fillna("") == ""))]
        .drop(columns=["allegation_x", "disposition_y", "agency_y"])
        .rename(
            columns={
                "agency_x": "agency",
                "disposition_x": "disposition",
                "allegation_y": "allegation",
            }
        )
        .pipe(float_to_int_str, ["officer_primary_key"])
    )


if __name__ == "__main__":
    pprr_ipm = pd.read_csv(deba.data("clean/pprr_new_orleans_ipm_iapro_1946_2018.csv"))
    pprr_csd = pd.read_csv(deba.data("clean/pprr_new_orleans_csd_2014.csv"))
    pclaims20 = pd.read_csv(deba.data("clean/pclaims_new_orleans_pd_2020.csv"))
    pclaims21 = pd.read_csv(deba.data("clean/pclaims_new_orleans_pd_2021.csv"))
    agency = pprr_ipm.agency[0]
    post = load_for_agency(agency)
    award = pd.read_csv(deba.data("clean/award_new_orleans_pd_2016_2021.csv"))
    lprr = pd.read_csv(deba.data("clean/lprr_new_orleans_csc_2000_2016.csv"))
    lprr_transcripts = pd.read_csv(deba.data("clean/lprr_appeal_transcripts_new_orleans_csc_2000_2021.csv"))
    sas = pd.read_csv(deba.data("clean/sas_new_orleans_pd_2017_2021.csv"))
    uof_officers = pd.read_csv(
        deba.data("clean/uof_officers_new_orleans_pd_2016_2021.csv")
    )
    pprr_separations = pd.read_csv(
        deba.data("clean/pprr_seps_new_orleans_pd_2018_2022.csv")
    )
    pib = join_pib_and_ipm()
    award = deduplicate_award(award)
    event_df = match_pprr_against_post(pprr_ipm, post)
    award = match_award_to_pprr_ipm(award, pprr_ipm)
    lprr = match_lprr_to_pprr_ipm(lprr, pprr_ipm)
    lprr_transcripts = match_lprr_transcripts_to_pprr_ipm(lprr_transcripts, pprr_ipm)
    sas = match_stop_and_search_to_pprr(sas, pprr_ipm)
    pprr_csd_matched_with_ipm = match_pprr_csd_to_pprr_ipm(pprr_csd, pprr_ipm)
    uof_officers = match_use_of_force_to_pprr(uof_officers, pprr_ipm)
    pclaims20 = match_pclaims20_to_pprr(pclaims20, pprr_ipm)
    pclaims21 = match_pclaims21_to_pprr(pclaims21, pprr_ipm)
    pprr_separations = match_pprr_separations_to_pprr(pprr_separations, pprr_ipm)
    award.to_csv(deba.data("match/award_new_orleans_pd_2016_2021.csv"), index=False)
    event_df.to_csv(deba.data("match/post_event_new_orleans_pd.csv"), index=False)
    lprr.to_csv(deba.data("match/lprr_new_orleans_csc_2000_2016.csv"), index=False)
    pprr_csd_matched_with_ipm.to_csv(
        deba.data("match/pprr_new_orleans_csd_2014.csv"), index=False
    )
    sas.to_csv(deba.data("match/sas_new_orleans_pd_2017_2021.csv"), index=False)
    uof_officers.to_csv(
        deba.data("match/uof_new_orleans_pd_2016_2021.csv"), index=False
    )
    pprr_ipm.to_csv(
        deba.data("match/pprr_new_orleans_ipm_iapro_1946_2018.csv"), index=False
    )
    pclaims20.to_csv(deba.data("match/pclaims_new_orleans_pd_2020.csv"), index=False)
    pclaims21.to_csv(deba.data("match/pclaims_new_orleans_pd_2021.csv"), index=False)
    pprr_separations.to_csv(
        deba.data("match/pprr_seps_new_orleans_pd_2018_2022.csv"), index=False
    )
    pib.to_csv(
        deba.data("match/cprr_new_orleans_pib_reports_2014_2020.csv"), index=False
    )
    lprr_transcripts.to_csv(deba.data("match/lprr_appeal_transcripts_new_orleans_csc_2000_2021.csv"), index=False)
