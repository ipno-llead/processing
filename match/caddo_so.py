import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex, DateSimilarity
import deba
from lib.post import load_for_agency
from lib.date import combine_date_columns


def assign_uid_from_post_21(cprr, post):
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
    decision = 0.958
    matcher.save_pairs_to_excel(
        deba.data("match/caddo_so_2022_2023_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def match_pprr_against_post(pprr, post):
    dfa = pprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"], ignore_index=True).set_index(
        "uid", drop=True
    )

    dfb = post[["first_name", "last_name", "uid"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day"
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"], ignore_index=True).set_index(
        "uid", drop=True
    )

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "hire_date": DateSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.793
    matcher.save_pairs_to_excel(
        deba.data("match/caddo_parish_so_pprr_2020_v_post.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr.loc[:, "uid"] = pprr.uid.map(lambda x: match_dict.get(x, x))
    return pprr

def assign_uid_from_post_25(cprr, post):
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
        deba.data("match/caddo_so_2015_2019_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

def match_pprr_21_against_post(pprr, post):
    dfa = pprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates(subset=["uid"], ignore_index=True).set_index(
        "uid", drop=True
    )

    dfb = post[["first_name", "last_name", "uid"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day"
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"], ignore_index=True).set_index(
        "uid", drop=True
    )

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "hire_date": DateSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9287
    matcher.save_pairs_to_excel(
        deba.data("match/caddo_parish_so_pprr_2021_v_post.xlsx"), decision
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    pprr.loc[:, "uid"] = pprr.uid.map(lambda x: match_dict.get(x, x))
    return pprr

def assign_uid_from_post_21(cprr, post):
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
    decision = 0.98
    matcher.save_pairs_to_excel(
        deba.data("match/caddo_so_2020_2021_v_post_pprr_2025_8_25.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_caddo_so_2022_2023.csv"))
    cprr19 = pd.read_csv(deba.data("clean/cprr_caddo_so_2015_2019.csv"))
    cprr21 = pd.read_csv(deba.data("clean/cprr_caddo_so_2020_2021.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_caddo_parish_so_2020.csv"))
    pprr21 = pd.read_csv(deba.data("clean/pprr_caddo_parish_so_2021.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = assign_uid_from_post_21(cprr, post)
    cprr19 = assign_uid_from_post_25(cprr19, post)
    cprr21 = assign_uid_from_post_21(cprr21, post)
    pprr = match_pprr_against_post(pprr, post)
    pprr21 = match_pprr_21_against_post(pprr21, post)
    cprr.to_csv(deba.data("match/cprr_caddo_so_2022_2023.csv"), index=False)
    cprr19.to_csv(deba.data("match/cprr_caddo_so_2015_2019.csv"), index=False)
    cprr21.to_csv(deba.data("match/cprr_caddo_so_2020_2021.csv"), index=False)
    pprr.to_csv(deba.data("match/pprr_caddo_parish_so_2020.csv"), index=False)
    pprr21.to_csv(deba.data("match/pprr_caddo_parish_so_2021.csv"), index=False)
