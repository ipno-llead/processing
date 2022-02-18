import bolo
from datamatch import ColumnsIndex, JaroWinklerSimilarity, ThresholdMatcher
import pandas as pd


def match_against_baton_rouge_csd_pprr(df, pprr, year, decision):
    dfa = df.loc[
        df.agency == "Baton Rouge Police Department", ["uid", "first_name", "last_name"]
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
        bolo.data("match/baton_rouge_da_cprr_2018_v_csd_pprr_%d.xlsx" % year),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    match_dict = dict(matches)
    df.loc[:, "uid"] = df.uid.map(lambda x: match_dict.get(x, x))
    return df


def match_against_baton_rouge_so_personnel(df, per):
    dfa = df.loc[
        df.agency == "East Baton Rouge Sheriff's Office",
        ["uid", "first_name", "last_name"],
    ].drop_duplicates()
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])
    dfa = dfa.set_index("uid")

    dfb = (
        per[["uid", "first_name", "last_name"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid")
    )
    dfb.loc[:, "first_name"] = dfb.first_name.str.lower()
    dfb.loc[:, "last_name"] = dfb.last_name.str.lower()
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
    decision = 1
    matcher.save_pairs_to_excel(
        bolo.data("match/baton_rouge_da_cprr_2018_v_baton_rouge_so_personnel.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    match_dict = dict(matches)
    df.loc[:, "uid"] = df.uid.map(lambda x: match_dict.get(x, x))
    return df


if __name__ == "__main__":
    pprr19 = pd.read_csv(bolo.data("match/pprr_baton_rouge_csd_2019.csv"))
    pprr17 = pd.read_csv(bolo.data("match/pprr_baton_rouge_csd_2017.csv"))
    df = pd.read_csv(bolo.data("clean/cprr_baton_rouge_da_2021.csv"))
    df = match_against_baton_rouge_csd_pprr(df, pprr17, 2017, 0.97)
    df = match_against_baton_rouge_csd_pprr(df, pprr19, 2019, 0.97)
    df.to_csv(bolo.data("match/cprr_baton_rouge_da_2021.csv"), index=False)
