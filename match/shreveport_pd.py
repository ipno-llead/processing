import pandas as pd
from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    ColumnsIndex,
    StringSimilarity,
)

import deba
from lib.uid import gen_uid
from lib.post import load_for_agency


def match_cprr_codebook(cprr, cb):
    dfa = cprr[["allegation"]]
    allegation_df = cprr.allegation.str.extract(r"(\d{3}\.\d{1,2})(.+)$")
    dfa.loc[:, "name"] = allegation_df.loc[:, 1].str.strip()
    dfa.loc[:, "code"] = allegation_df.loc[:, 0]
    dfa = dfa[dfa.code.notna()].drop_duplicates().set_index("allegation", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex("code"), {"name": StringSimilarity()}, dfa, cb
    )
    decision = 0.541
    lower_bound = 0.5
    matcher.save_pairs_to_excel(
        deba.data("match/shreveport_pd_cprr_2018_2019_v_codebook.xlsx"),
        decision,
        lower_bound=lower_bound,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    code_n_name = cb.code.str.cat(cb.name, sep=" ")
    cprr.loc[:, "allegation"] = cprr.allegation.map(
        lambda x: x if x not in match_dict else code_n_name[match_dict[x]]
    )
    return cprr


def match_cprr_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates().set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates().set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.95
    matcher.save_pairs_to_excel(
        deba.data("match/shreveport_pd_cprr_2018_2019_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr

def match_pprr_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates().set_index("uid")
    dfa.loc[:,"fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates().set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    decision = 0.95
    matcher.save_pairs_to_excel(
        deba.data("match/shreveport_pd_pprr_1999_2001_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)
    pprr.loc[:, "uid"] = pprr.uid.map(lambda x: match_dict.get(x, x))
    return pprr 
    #return extract_events_from_post(post, matches,"shreveport-pd")

if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/cprr_shreveport_pd_2018_2019.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_shreveport_pd_1990_2001.csv"))
    cb = pd.read_csv(deba.data("clean/cprr_codebook_shreveport_pd.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = match_cprr_codebook(cprr, cb).pipe(
        gen_uid, ["agency", "tracking_id", "allegation"], "allegation_uid"
    )
    cprr = match_cprr_post(cprr, post)
    pprr = match_pprr_post(pprr, post)
    cprr.to_csv(deba.data("match/cprr_shreveport_pd_2018_2019.csv"), index=False)
    pprr.to_csv(deba.data("match/pprr_shreveport_pd_1990_2001.csv"), index=False)
