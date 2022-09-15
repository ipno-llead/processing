import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

import deba
from lib.post import extract_events_from_post, load_for_agency


def match_and_concat_pprr(df1, df2, year1, year2, decision):
    dfa = (
        df1[["first_name", "last_name", "uid"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid")
    )
    dfa.loc[:, "fc"] = dfa.first_name.map(lambda x: x[:1])

    dfb = (
        df2[["first_name", "last_name", "uid"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid")
    )
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("fc"),
        {"first_name": JaroWinklerSimilarity(), "last_name": JaroWinklerSimilarity()},
        dfa,
        dfb,
    )
    matcher.save_pairs_to_excel(
        deba.data("match/youngsville_pd_pprr_%s_v_%s.xlsx" % (year1, year2)),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    match_dict = dict(matches)

    # match and concat
    df1.loc[:, "uid"] = df1.uid.map(lambda x: match_dict.get(x, x))
    df = pd.concat([df1, df2]).set_index("uid", drop=False)

    # canonicalize first name and last name (keep the longest)
    for uid in df.index:
        rows = df.loc[uid]
        if isinstance(rows, pd.DataFrame):
            df.loc[uid, "first_name"] = rows.first_name[
                rows.first_name.str.len() == rows.first_name.str.len().max()
            ].iloc[0]
            df.loc[uid, "last_name"] = rows.last_name[
                rows.last_name.str.len() == rows.last_name.str.len().max()
            ].iloc[0]
    return df


def match_pprr_and_post(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
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
    decision = 0.9
    matcher.save_pairs_to_excel(
        deba.data("match/youngsville_pd_pprr_2017_2019_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)
    return extract_events_from_post(post, matches, "youngsville-pd")


if __name__ == "__main__":
    pprr17 = pd.read_csv(deba.data("clean/pprr_youngsville_csd_2017.csv"))
    pprr18 = pd.read_csv(deba.data("clean/pprr_youngsville_csd_2018.csv"))
    pprr19 = pd.read_csv(deba.data("clean/pprr_youngsville_csd_2019.csv"))
    pprr = match_and_concat_pprr(
        match_and_concat_pprr(pprr17, pprr18, "2017", "2018", 0.9),
        pprr19,
        "2017_2018",
        "2019",
        0.9,
    )
    agency = pprr17.agency[0]
    post = load_for_agency(agency)
    post_event = match_pprr_and_post(pprr, post)
    pprr.to_csv(deba.data("match/pprr_youngsville_pd_2017_2019.csv"), index=False)
    post_event.to_csv(
        deba.data("match/post_event_youngsville_pd_2020.csv"), index=False
    )
