import deba
import pandas as pd
from lib.clean import (
    names_to_title_case,
    clean_names,
    clean_post_agency_column,
    clean_sexes,
    clean_agency_row,
)
from lib.uid import gen_uid
from datamatch import (
    JaroWinklerSimilarity,
    ThresholdMatcher,
    ColumnsIndex,
)


def join_events_and_personnel():
    dfa = pd.read_csv(deba.data("fuse/event.csv"))
    dfb = pd.read_csv(deba.data("fuse/personnel.csv"))

    df = pd.merge(
        dfa[["agency", "uid"]],
        dfb[["first_name", "last_name", "middle_name", "uid"]],
        on="uid",
        how="outer",
    )

    df = df.dropna().drop_duplicates(subset=["uid"])
    df = df.pipe(clean_names, ["first_name", "middle_name", "last_name"])
    return df


def split_names(df):
    names = (
        df.officer_name.str.lower()
        .str.strip()
        .str.extract(r"(\w+(?:'\w+)?),? (\w+)(?: (\w+))?")
    )

    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    return df.drop(columns=["officer_name"])


def extract_agency(df):
    for col in df.columns:
        if col.startswith("agency"):
            df[col] = (
                df[col]
                .fillna("")
                .str.lower()
                .str.strip()
                .str.replace(r"(\w{1}? ?\w+ ?\w+? ?\w+) (.+)", r"\1", regex=True)
            )
    return df


def drop_rows_missing_agency(df):
    return df[~((df.agency.fillna("") == ""))]


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean_post():
    df = (
        pd.read_csv(deba.data("raw/post/post_officer_history.csv"))
        .rename(columns={"officer_sex": "sex"})
        .pipe(clean_sexes, ["sex"])
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(
            clean_agency_row,
            [
                "agency",
                "agency_1",
                "agency_2",
                "agency_3",
                "agency_4",
                "agency_5",
                "agency_6",
                "agency_7",
                "agency_8",
            ],
        )
        .pipe(extract_agency)
        .pipe(
            names_to_title_case,
            [
                "agency",
                "agency_1",
                "agency_2",
                "agency_3",
                "agency_4",
                "agency_5",
                "agency_6",
                "agency_7",
                "agency_8",
            ],
        )
        .pipe(
            clean_post_agency_column,
            [
                "agency",
                "agency_1",
                "agency_2",
                "agency_3",
                "agency_4",
                "agency_5",
                "agency_6",
                "agency_7",
                "agency_8",
            ],
        )
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency"])
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_1"], "uid_1")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_2"], "uid_2")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_3"], "uid_3")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_4"], "uid_4")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_5"], "uid_5")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_6"], "uid_6")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_7"], "uid_7")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_8"], "uid_8")
        .pipe(drop_rows_missing_agency)
        .pipe(drop_rows_missing_names)
    )
    return df


def set_post_uid(post):
    dfa = post[["uid", "first_name", "last_name", "middle_name", "agency"]]
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_post_uid_1(post):
    dfa = post[["uid_1", "first_name", "last_name", "middle_name", "agency_1"]]
    dfa = dfa.rename(columns={"agency_1": "agency"})
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_post_uid_2(post):
    dfa = post[["uid_2", "first_name", "last_name", "middle_name", "agency_2"]]
    dfa = dfa.rename(columns={"agency_2": "agency"})
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_post_uid_3(post):
    dfa = post[["uid_3", "first_name", "last_name", "middle_name", "agency_2"]]
    dfa = dfa.rename(columns={"agency_3": "agency"})
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_post_uid_4(post):
    dfa = post[["uid_4", "first_name", "last_name", "middle_name", "agency_2"]]
    dfa = dfa.rename(columns={"agency_4": "agency"})
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_post_uid_5(post):
    dfa = post[["uid_5", "first_name", "last_name", "middle_name", "agency_2"]]
    dfa = dfa.rename(columns={"agency_5": "agency"})
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_post_uid_6(post):
    dfa = post[["uid_6", "first_name", "last_name", "middle_name", "agency_2"]]
    dfa = dfa.rename(columns={"agency_6": "agency"})
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_post_uid_7(post):
    dfa = post[["uid_7", "first_name", "last_name", "middle_name", "agency_2"]]
    dfa = dfa.rename(columns={"agency_7": "agency"})
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_post_uid_8(post):
    dfa = post[["uid_8", "first_name", "last_name", "middle_name", "agency_2"]]
    dfa = dfa.rename(columns={"agency_8": "agency"})
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])
    return dfa


def set_personnel_uid(personnel):
    dfb = personnel[["uid", "first_name", "last_name", "middle_name", "agency"]]
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])
    return dfb


def match_uid(dfb, post, dfa=None):
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

    post.loc[:, "matched_uid"] = post.uid.map(lambda x: match_dict.get(x))
    post.loc[:, "matched_uid_1"] = post.uid_1.map(lambda x: match_dict.get(x))
    post.loc[:, "matched_uid_2"] = post.uid_2.map(lambda x: match_dict.get(x))
    post.loc[:, "matched_uid_3"] = post.uid_3.map(lambda x: match_dict.get(x))
    post.loc[:, "matched_uid_4"] = post.uid_4.map(lambda x: match_dict.get(x))
    post.loc[:, "matched_uid_5"] = post.uid_5.map(lambda x: match_dict.get(x))
    post.loc[:, "matched_uid_6"] = post.uid_6.map(lambda x: match_dict.get(x))
    post.loc[:, "matched_uid_7"] = post.uid_7.map(lambda x: match_dict.get(x))
    post.loc[:, "matched_uid_8"] = post.uid_8.map(lambda x: match_dict.get(x))
    return post


def matches(post, personnel):
    dfb = set_personnel_uid(personnel)
    for col in post.columns:
        if col.startswith("uid"):
            if col == "uid":
                df = match_uid(dfb, post, dfa=set_post_uid(post))
            if col != "uid":
                continue
            if col == "uid_1":
                df = match_uid(dfb, post, dfa=set_post_uid_1(post))
            if col != "uid_1":
                continue
            if col == "uid_2":
                df = match_uid(dfb, post, dfa=set_post_uid_2(post))
            if col != "uid_2":
                continue
            if col == "uid_3":
                df = match_uid(dfb, post, dfa=set_post_uid_3(post))
            if col != "uid_3":
                continue
            if col == "uid_4":
                df = match_uid(dfb, post, dfa=set_post_uid_4(post))
            if col != "uid_4":
                continue
            if col == "uid_5":
                df = match_uid(dfb, post, dfa=set_post_uid_5(post))
            if col != "uid_5":
                continue
            if col == "uid_6":
                df = match_uid(dfb, post, dfa=set_post_uid_6(post))
            if col != "uid_6":
                continue
            if col == "uid_7":
                df = match_uid(dfb, post, dfa=set_post_uid_7(post))
            if col != "uid_7":
                continue
            if col == "uid_8":
                df = match_uid(dfb, post, dfa=set_post_uid_8(post))
            if col != "uid_8":
                break
    return df


# def matches(post, personnel):
#     dfb = set_personnel_uid(personnel)
#     for col in post.columns:
#         if col == "uid":
#             dfa = post[["uid", "first_name", "last_name", "middle_name", "agency", "fc", "lc"]]
#         elif col == "uid_1":
#             dfb = post[["uid_1", "first_name","last_name", "middle_name", "agency", "fc", "lc"]]
#         elif col == "uid_2":
#             dfc = post[["uid_2", "first_name","last_name", "middle_name", "agency", "fc", "lc"]]
#         elif col == "uid_3":
#             dfd = post[["uid_3", "first_name","last_name", "middle_name", "agency", "fc", "lc"]]
#         elif col == "uid_4":
#             dfe = post[["uid_1", "first_name","last_name", "middle_name", "agency", "fc", "lc"]]
#         elif col == "uid_6":
#             dff = post[["uid_1", "first_name","last_name", "middle_name", "agency",  "fc", "lc"]]
#         elif col == "uid_7":
#             dfg = post[["uid_1", "first_name","last_name", "middle_name", "agency",  "fc", "lc"]]
#         elif col == "uid_8":
#             dfh = post[["uid_1", "first_name","last_name", "middle_name", "agency",  "fc", "lc"]]
#         for c in col:
#             df = match_uid(c, dfb, post)
# #     return df
#    AttributeError: 'builtin_function_or_method' object has no attribute 'duplicated'

if __name__ == "__main__":
    post = clean_post()
    personnel = join_events_and_personnel()
    post = matches(post, personnel)
    post.to_csv(deba.data("fuse/post_officer_history.csv"), index=False)
