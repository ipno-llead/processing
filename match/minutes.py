import operator
from functools import reduce
import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex
import deba


def match_minutes_accused_to_personnel(
    mins: pd.DataFrame, per: pd.DataFrame
) -> pd.DataFrame:
    dfa = (
        mins[["docid", "hrg_no", "agency", "accused"]]
        .set_index(["agency", "docid", "hrg_no"], drop=True)
        .sort_index()
        .accused.str.split(" & ")
        .groupby(["agency", "docid", "hrg_no"])
        .apply(lambda x: pd.Series(reduce(operator.add, x)))
        .reset_index(level=0)
    )
    names = (
        dfa.accused.str.replace(r"^(lt|sgt). ", "", regex=True)
        .str.replace("alvinlangsford", "alvin langsford", regex=False)
        .str.replace(r"[\.,] *", " ", regex=True)
        .str.extract(r"([-\w]+)(?: (?:(\w+) )?([\w’]+(?: (?:jr|sr))?))?")
    )
    names.columns = ["first_name", "middle_name", "last_name"]
    dfa = dfa.drop(columns="accused").join(names)
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = per.loc[
        per.agency.isin(dfa.agency.to_list()),
        ["uid", "last_name", "middle_name", "first_name", "agency"],
    ].set_index("uid")
    dfb.loc[:, "first_name"] = dfb.first_name.str.lower()
    dfb.loc[:, "middle_name"] = dfb.middle_name.str.lower()
    dfb.loc[:, "last_name"] = dfb.last_name.str.lower()
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["agency", "fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "middle_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        show_progress=True,
    )
    decision = 0.76
    matcher.save_pairs_to_excel(
        deba.data("match/minutes_accused_v_personnel.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    uids = (
        pd.DataFrame(
            [(k[0], k[1], v) for k, v in matches],
            columns=["docid", "hrg_no", "uids"],
        )
        .groupby(["docid", "hrg_no"])
        .uids.aggregate(lambda x: ",".join(x))
    )
    return mins.set_index(["docid", "hrg_no"]).join(uids).reset_index()


if __name__ == "__main__":
    per = pd.read_csv(deba.data(r"fuse/personnel.csv"))
    mins = pd.read_csv(deba.data(r"features/minutes_hearing_text.csv"))
    matched_mins = match_minutes_accused_to_personnel(mins, per)
    matched_mins.to_csv(deba.data(r"match/minutes_accused.csv"), index=False)
