from lib.path import data_file_path
from lib.post import load_for_agency
from datamatch import ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
import pandas as pd
import sys

sys.path.append("../")


def match_against_baton_rouge_csd_pprr(df, pprr, year, decision):
    dfa = df.loc[
        df.agency == "Baton Rouge PD", ["uid", "first_name", "last_name"]
    ].drop_duplicates()
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.set_index("uid")

    dfb = (
        pprr[["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid")
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

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
        data_file_path("match/brady_baton_rouge_da_2021_v_csd_pprr_%d.xlsx" % year),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    match_dict = dict(matches)
    df.loc[:, "uid"] = df.uid.map(lambda x: match_dict.get(x, x))
    return df


def match_brady_and_baton_rouge_pd(cprr, pprr):
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
    decision = 0.96
    matcher.save_pairs_to_excel(
        data_file_path("match/brady_baton_rouge_2021_v_pprr_baton_rouge_pd_2021.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_brady_and_post(cprr, post):
    dfa = (
        cprr.loc[cprr.uid.notna(), ["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = (
        post[["uid", "first_name", "last_name"]]
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
    decision = 0.94
    matcher.save_pairs_to_excel(
        data_file_path("match/brady_baton_rouge_2021_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    pprr19 = pd.read_csv(data_file_path("match/pprr_baton_rouge_csd_2019.csv"))
    pprr17 = pd.read_csv(data_file_path("match/pprr_baton_rouge_csd_2017.csv"))
    pprr_pd = pd.read_csv(data_file_path("clean/pprr_baton_rouge_pd_2021.csv"))
    brady = pd.read_csv(data_file_path("clean/cprr_baton_rouge_da_2021.csv"))
    agency = brady.agency[0]
    post = load_for_agency("clean/pprr_post_2020_11_06.csv", agency)
    brady = match_against_baton_rouge_csd_pprr(brady, pprr17, 2017, 0.94)
    brady = match_against_baton_rouge_csd_pprr(brady, pprr19, 2019, 0.97)
    brady = match_brady_and_baton_rouge_pd(brady, pprr_pd)
    brady = match_brady_and_post(brady, post)
    brady.to_csv(data_file_path("match/cprr_baton_rouge_da_2021.csv"), index=False)
