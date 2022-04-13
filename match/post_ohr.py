import deba
import pandas as pd
from datamatch import (
    JaroWinklerSimilarity,
    ThresholdMatcher,
    ColumnsIndex,
)


def match_uid_and_personnel(cprr, pprr):
    dfa = cprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid"] = cprr.uid.map(lambda x: match_dict.get(x))
    return cprr


def match_uid_1_and_personnel(cprr, pprr):
    dfa = cprr[["uid_1", "first_name", "last_name", "agency_1", "middle_name"]]
    dfa = dfa.rename(columns={"agency_1": "agency"})
    dfa = dfa.drop_duplicates(subset=["uid_1"]).set_index("uid_1")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid_1"] = cprr.uid_1.map(lambda x: match_dict.get(x))
    return cprr


def match_uid_2_and_personnel(cprr, pprr):
    dfa = cprr[["uid_2", "first_name", "last_name", "agency_2", "middle_name"]]
    dfa = dfa.rename(columns={"agency_2": "agency"})
    dfa = dfa.drop_duplicates(subset=["uid_2"]).set_index("uid_2")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid_2"] = cprr.uid_2.map(lambda x: match_dict.get(x))
    return cprr


def match_uid_3_and_personnel(cprr, pprr):
    dfa = cprr[["uid_3", "first_name", "last_name", "agency_3", "middle_name"]]
    dfa = dfa.rename(columns={"agency_3": "agency"})
    dfa = dfa.drop_duplicates(subset=["uid_3"]).set_index("uid_3")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid_3"] = cprr.uid_3.map(lambda x: match_dict.get(x))
    return cprr


def match_uid_4_and_personnel(cprr, pprr):
    dfa = cprr[["uid_4", "first_name", "last_name", "agency_4", "middle_name"]]
    dfa = dfa.rename(columns={"agency_4": "agency"})

    dfa = dfa.drop_duplicates(subset=["uid_4"]).set_index("uid_4")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid_4"] = cprr.uid_4.map(lambda x: match_dict.get(x))
    return cprr


def match_uid_5_and_personnel(cprr, pprr):
    dfa = cprr[["uid_5", "first_name", "last_name", "agency_5", "middle_name"]]
    dfa = dfa.rename(columns={"agency_5": "agency"})
    dfa = dfa.drop_duplicates(subset=["uid_5"]).set_index("uid_5")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid_5"] = cprr.uid_5.map(lambda x: match_dict.get(x))
    return cprr


def match_uid_6_and_personnel(cprr, pprr):
    dfa = cprr[["uid_6", "first_name", "last_name", "agency_6", "middle_name"]]
    dfa = dfa.rename(columns={"agency_6": "agency"})
    dfa = dfa.drop_duplicates(subset=["uid_6"]).set_index("uid_6")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.950

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid_6"] = cprr.uid_6.map(lambda x: match_dict.get(x))
    return cprr


def match_uid_7_and_personnel(cprr, pprr):
    dfa = cprr[["uid_7", "first_name", "last_name", "agency_7", "middle_name"]]
    dfa = dfa.rename(columns={"agency_7": "agency"})
    dfa = dfa.drop_duplicates(subset=["uid_7"]).set_index("uid_7")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.950

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid_7"] = cprr.uid_7.map(lambda x: match_dict.get(x))
    return cprr


def match_uid_8_and_personnel(cprr, pprr):
    dfa = cprr[["uid_8", "first_name", "last_name", "agency_8", "middle_name"]]
    dfa = dfa.rename(columns={"agency_8": "agency"})
    dfa = dfa.drop_duplicates(subset=["uid_8"]).set_index("uid_8")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name", "agency", "middle_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.9

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "matched_uid_8"] = cprr.uid_8.map(lambda x: match_dict.get(x))
    return cprr


def join_matched_uids(df):
    df.loc[:, "post_id"] = df.matched_uid.fillna("").str.cat(
        df.matched_uid_1.fillna(""), sep=","
    )
    df.loc[:, "post_id"] = df.post_id.str.cat(df.matched_uid_2.fillna(""), sep=",")
    df.loc[:, "post_id"] = df.post_id.str.cat(df.matched_uid_3.fillna(""), sep=",")
    df.loc[:, "post_id"] = df.post_id.str.cat(df.matched_uid_4.fillna(""), sep=",")
    df.loc[:, "post_id"] = df.post_id.str.cat(df.matched_uid_5.fillna(""), sep=",")
    df.loc[:, "post_id"] = df.post_id.str.cat(df.matched_uid_6.fillna(""), sep=",")
    df.loc[:, "post_id"] = df.post_id.str.cat(df.matched_uid_7.fillna(""), sep=",")
    df.loc[:, "post_id"] = df.post_id.str.cat(df.matched_uid_8.fillna(""), sep=",")

    df.loc[:, "post_id"] = (
        df.post_id.fillna("")
        .str.replace(r"\,\, ?(.+)?", "", regex=True)
        .str.replace(r"^\,", "", regex=True)
    )
    df = df.fillna("")
    df = df[["post_id"]]
    return df[~((df.post_id == ""))]


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/post_officer_history.csv"))
    pprr = pd.read_csv(deba.data("raw/fused/event.csv"))
    cprr = match_uid_and_personnel(cprr, pprr)
    cprr = match_uid_1_and_personnel(cprr, pprr)
    cprr = match_uid_2_and_personnel(cprr, pprr)
    cprr = match_uid_3_and_personnel(cprr, pprr)
    cprr = match_uid_4_and_personnel(cprr, pprr)
    cprr = match_uid_5_and_personnel(cprr, pprr)
    cprr = match_uid_6_and_personnel(cprr, pprr)
    cprr = match_uid_7_and_personnel(cprr, pprr)
    cprr = match_uid_8_and_personnel(cprr, pprr)
    cprr = cprr.pipe(join_matched_uids)
    cprr.to_csv(deba.data("match/post_officer_history.csv"), index=False)
