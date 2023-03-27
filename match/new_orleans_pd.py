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
from lib.clean import canonicalize_officers, float_to_int_str, standardize_desc_cols
from lib.uid import gen_uid


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


def deduplicate_pprr_iapro(pprr):
    df = pprr[["uid", "first_name", "last_name", "middle_name"]]
    df = df.drop_duplicates(subset=["uid"]).set_index("uid")
    df.loc[:, "fc"] = df.first_name.fillna("").map(lambda x: x[:1])
    df.loc[:, "mc"] = df.middle_name.fillna("").map(lambda x: x[:1])
    df.loc[:, "lc"] = df.last_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.985
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_pprr_new_orleans_pd.xlsx"), decision, decision
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(pprr, clusters)


def match_pprr_against_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day"
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
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
        deba.data("match/pppr_new_orleans_pd_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )

    matches = matcher.get_index_pairs_within_thresholds(decision)
    return extract_events_from_post(post, matches, "new-orleans-pd")


def match_award_to_pprr(award, pprr):
    dfa = (
        award[["uid", "first_name", "last_name"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
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
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/new_orleans_pd_award_2016_2021_v_pprr_nopd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    match_dict = dict(matches)
    award.loc[:, "uid"] = award.uid.map(lambda x: match_dict.get(x, x))
    return award


def match_lprr_to_pprr(lprr, pprr):
    dfa = lprr[["uid", "first_name", "last_name", "middle_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name", "middle_name"]]
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
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/new_orleans_lprr_2000_2016_v_pprr_nopd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_clusters_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    lprr.loc[:, "uid"] = lprr.uid.map(lambda x: match_dict.get(x, x))
    return lprr


def match_pprr_csd_to_pprr(pprr_csd, pprr):
    dfa = pprr_csd[["uid", "first_name", "last_name", "agency"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr_csd, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name", "agency"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
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
        deba.data("match/pprr_new_orleans_csd_2014_v_pprr_nopd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr_csd.loc[:, "uid"] = pprr_csd.uid.map(lambda x: match_dict.get(x, x))
    return pprr_csd


def match_stop_and_search_to_pprr(sas, pprr):
    dfa = sas[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name"]]
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
    decision = 1

    matcher.save_pairs_to_excel(
        deba.data(
            "match/stop_and_search_new_orleans_pd_v_pprr_new_orleans_pd_2020.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    sas.loc[:, "uid"] = sas.uid.map(lambda x: match_dict.get(x, x))
    return sas


def match_use_of_force_to_pprr(uof, pprr):
    dfa = uof[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name"]]
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
    decision = .935

    matcher.save_pairs_to_excel(
        deba.data("match/uof_new_orleans_pd_2016_2022_v_pprr_new_orleans_pd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    uof.loc[:, "uid"] = uof.uid.map(lambda x: match_dict.get(x, x))
    return uof


def match_cprr_to_pprr(cprr, pprr):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name"]]
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
    decision = 0.943

    matcher.save_pairs_to_excel(
        deba.data("match/cprr_new_orleans_da_v_pprr_new_orleans_pd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_pclaims20_to_pprr(pclaims, pprr):
    dfa = pclaims[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name"]]
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
    decision = 1

    matcher.save_pairs_to_excel(
        deba.data("match/pclaims_new_orleans_pd_2020_v_pprr_new_orleans_pd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pclaims.loc[:, "uid"] = pclaims.uid.map(lambda x: match_dict.get(x, x))
    return pclaims


def match_pclaims21_to_pprr(pclaims, pprr):
    dfa = pclaims[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name"]]
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
    decision = 1

    matcher.save_pairs_to_excel(
        deba.data("match/pclaims_new_orleans_pd_2020_v_pprr_new_orleans_pd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pclaims.loc[:, "uid"] = pclaims.uid.map(lambda x: match_dict.get(x, x))
    return pclaims


def match_pprr_separations_to_pprr(pprr_seps, pprr):
    dfa = pprr_seps[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "last_name"]]
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
    decision = 1

    matcher.save_pairs_to_excel(
        deba.data(
            "match/pprr_seps_new_orleans_pd_2019-22_v_pprr_new_orleans_pd_2020.xlsx"
        ),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr_seps.loc[:, "uid"] = pprr_seps.uid.map(lambda x: match_dict.get(x, x))
    return pprr_seps


def join_pib_and_da():
    pib = pd.read_csv(
        deba.data("clean/cprr_new_orleans_pd_pib_reports_2014_2020.csv")
    ).drop_duplicates(subset=["tracking_id"], keep=False)
    pib = pib[["tracking_id", "allegation_desc"]].pipe(
        standardize_desc_cols, ["tracking_id"]
    )
    da = pd.read_csv(
        deba.data("clean/cprr_new_orleans_da_2016_2020.csv")
    ).drop_duplicates(subset=["tracking_id"], keep=False)
    df = pd.merge(pib, da, on="tracking_id", how="outer")
    df = df[~((df.allegation_desc.fillna("") == ""))]
    df = df[~((df.uid.fillna("") == ""))]
    return df


def match_pr_to_pprr(pr, pprr):
    dfa = pr[["uid", "first_name", "middle_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "middle_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "middle_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.772

    matcher.save_pairs_to_excel(
        deba.data("match/police_reports_nopd_v_pprr_new_orleans_pd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pr.loc[:, "uid"] = pr.uid.map(lambda x: match_dict.get(x, x))
    return pr


def deduplicate_pr_officers(pr):
    df = pr[["uid", "first_name", "middle_name", "last_name"]]
    df = df.drop_duplicates(subset=["uid"]).set_index("uid")
    df.loc[:, "fc"] = df.first_name.fillna("").map(lambda x: x[:1])
    df.loc[:, "lc"] = df.last_name.fillna("").map(lambda x: x[:1])
    df.loc[:, "middle_name"] = df.middle_name.fillna("")
    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {
            "first_name": JaroWinklerSimilarity(),
            "middle_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        df,
    )
    decision = 0.953
    matcher.save_clusters_to_excel(
        deba.data("match/deduplicate_epr_nopd_2010_2022.xlsx"),
        decision,
        decision,
    )
    clusters = matcher.get_index_clusters_within_thresholds(decision)
    return canonicalize_officers(pr, clusters)


def match_pprr_iapro_to_pprr(pprr_iapro, pprr):
    dfa = pprr_iapro[["uid", "first_name", "middle_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = pprr[["uid", "first_name", "middle_name", "last_name"]]
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
    decision = 0.772

    matcher.save_pairs_to_excel(
        deba.data("match/pprr_iapro_nopd_v_pprr_new_orleans_pd_2020.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr_iapro.loc[:, "uid"] = pprr_iapro.uid.map(lambda x: match_dict.get(x, x))
    return pprr_iapro


if __name__ == "__main__":
    pprr_iapro = pd.read_csv(deba.data("clean/pprr_new_orleans_pd_1946_2018.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_new_orleans_pd_2020.csv"))
    pprr_csd = pd.read_csv(deba.data("clean/pprr_new_orleans_csd_2014.csv"))
    pclaims20 = pd.read_csv(deba.data("clean/pclaims_new_orleans_pd_2020.csv"))
    pclaims21 = pd.read_csv(deba.data("clean/pclaims_new_orleans_pd_2021.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    award = pd.read_csv(deba.data("clean/award_new_orleans_pd_2016_2021.csv"))
    lprr = pd.read_csv(deba.data("clean/lprr_new_orleans_csc_2000_2016.csv"))
    sas = pd.read_csv(deba.data("clean/sas_new_orleans_pd_2010_2023.csv"))
    uof = pd.read_csv(deba.data("clean/uof_new_orleans_pd_2016_2022.csv"))
    cprr = pd.read_csv(deba.data("clean/cprr_new_orleans_da_2016_2020.csv"))
    pprr_separations = pd.read_csv(
        deba.data("clean/pprr_seps_new_orleans_pd_2018_2022.csv")
    )
    pr = pd.read_csv(deba.data("clean/pr_new_orleans_pd_2010_2022.csv"))
    pr = deduplicate_pr_officers(pr)
    pprr_iapro = deduplicate_pprr_iapro(pprr_iapro)
    pprr_iapro = match_pprr_iapro_to_pprr(pprr_iapro, pprr)
    pr = match_pr_to_pprr(pr, pprr)
    # pib = join_pib_and_da()
    award = deduplicate_award(award)
    event_df = match_pprr_against_post(pprr, post)
    award = match_award_to_pprr(award, pprr)
    lprr = match_lprr_to_pprr(lprr, pprr)
    sas = match_stop_and_search_to_pprr(sas, pprr)
    pprr_csd_matched_with_ipm = match_pprr_csd_to_pprr(pprr_csd, pprr)
    uof = match_use_of_force_to_pprr(uof, pprr)
    pclaims20 = match_pclaims20_to_pprr(pclaims20, pprr)
    pclaims21 = match_pclaims21_to_pprr(pclaims21, pprr)
    pprr_separations = match_pprr_separations_to_pprr(pprr_separations, pprr)
    cprr = match_cprr_to_pprr(cprr, pprr)
    award.to_csv(deba.data("match/award_new_orleans_pd_2016_2021.csv"), index=False)
    event_df.to_csv(deba.data("match/post_event_new_orleans_pd.csv"), index=False)
    lprr.to_csv(deba.data("match/lprr_new_orleans_csc_2000_2016.csv"), index=False)
    pprr_csd_matched_with_ipm.to_csv(
        deba.data("match/pprr_new_orleans_csd_2014.csv"), index=False
    )
    sas.to_csv(deba.data("match/sas_new_orleans_pd_2010_2023.csv"), index=False)
    uof.to_csv(deba.data("match/uof_new_orleans_pd_2016_2022.csv"), index=False)
    pclaims20.to_csv(deba.data("match/pclaims_new_orleans_pd_2020.csv"), index=False)
    pclaims21.to_csv(deba.data("match/pclaims_new_orleans_pd_2021.csv"), index=False)
    cprr.to_csv(deba.data("match/cprr_new_orleans_da_2016_2020.csv"), index=False)
    pprr_separations.to_csv(
        deba.data("match/pprr_seps_new_orleans_pd_2018_2022.csv"), index=False
    )
    # pib.to_csv(
    #     deba.data("match/cprr_new_orleans_pib_reports_2014_2020.csv"), index=False
    # )
    pr.to_csv(deba.data("match/pr_new_orleans_pd_2010_2022.csv"))
    pprr_iapro.to_csv(deba.data("match/pprr_new_orleans_pd_1946_2018.csv"), index=False)
