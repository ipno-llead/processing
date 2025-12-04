import deba
from lib.post import load_for_agency
from datamatch import ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
import pandas as pd


def match_cprr_18_against_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_so_cprr_2018_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_20_against_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_so_cprr_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_cprr_15_against_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_so_cprr_2011_2015_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

def match_uof_against_post(uof, post):
    dfa = uof[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = .877
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_so_uof_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    uof.loc[:, "uid"] = uof.uid.map(lambda x: match_dict.get(x, x))
    return uof

def match_settlements_v_post(settlements, post):
    dfa = (
        settlements[["first_name", "last_name", "uid"]]
        .drop_duplicates("uid")
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        post[["first_name", "last_name", "uid"]]
        .drop_duplicates("uid")
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = .80
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_so_settlements_2021_2023_v_post_pprr_2020_11_06.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    matches = dict(matches)
    settlements.loc[:, "uid"] = settlements.uid.map(lambda x: matches.get(x, x))
    return settlements

def match_sas_against_post(sas, post):
    dfa = sas[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = .947
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_so_sas_2023_2025_v_post_pprr_08_25_2025.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    sas.loc[:, "uid"] = sas.uid.map(lambda x: match_dict.get(x, x))
    return sas

def match_cprr_21_against_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = .92
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_so_cprr_2021_v_post_pprr_2025_08_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

def match_cprr_25_against_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "last_name": JaroWinklerSimilarity(),
            "first_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = .96
    matcher.save_pairs_to_excel(
        deba.data("match/baton_rouge_so_cprr_2025_v_post_pprr_2025_08_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

if __name__ == "__main__":
    cprr_15 = pd.read_csv(deba.data("clean/cprr_baton_rouge_so_2011_2015.csv"))
    cprr18 = pd.read_csv(deba.data("clean/cprr_baton_rouge_so_2018.csv"))
    cprr20 = pd.read_csv(deba.data("clean/cprr_baton_rouge_so_2016_2020.csv"))
    cprr21 = pd.read_csv(deba.data("clean/cprr_baton_rouge_so_2021_2023.csv"))
    cprr25 = pd.read_csv(deba.data("clean/cprr_baton_rouge_so_2024_2025.csv"))
    uof = pd.read_csv(deba.data("clean/uof_baton_rouge_so_2020.csv"))
    sas = pd.read_csv(deba.data("clean/sas_baton_rouge_so_2023_2025.csv"))
    settlements = pd.read_csv(deba.data("clean/settlements_baton_rouge_so_2021_2023.csv"))
    agency = cprr20.agency[0]
    post = load_for_agency(agency)
    cprr_15 = match_cprr_15_against_post(cprr_15, post)
    cprr18 = match_cprr_18_against_post(cprr18, post)
    cprr20 = match_cprr_20_against_post(cprr20, post)
    cprr21 = match_cprr_21_against_post(cprr21, post)
    cprr25 = match_cprr_25_against_post(cprr25, post)
    uof = match_uof_against_post(uof, post)
    settlements = match_settlements_v_post(settlements, post)
    sas = match_sas_against_post(sas, post)
    cprr_15.to_csv(deba.data("match/cprr_baton_rouge_so_2011_2015.csv"), index=False)
    cprr18.to_csv(deba.data("match/cprr_baton_rouge_so_2018.csv"), index=False)
    cprr20.to_csv(deba.data("match/cprr_baton_rouge_so_2016_2020.csv"), index=False)
    cprr21.to_csv(deba.data("match/cprr_baton_rouge_so_2021_2023.csv"), index=False)
    uof.to_csv(deba.data("match/uof_baton_rouge_so_2020.csv"), index=False)
    settlements.to_csv(
        deba.data("match/settlements_baton_rouge_so_2021_2023.csv"), index=False
    )
    sas.to_csv(deba.data("match/sas_baton_rouge_so_2023_2025.csv"), index=False)
    cprr25.to_csv(deba.data("match/cprr_baton_rouge_so_2024_2025.csv"), index=False)
