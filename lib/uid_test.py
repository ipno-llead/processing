from uid import gen_uid, gen_uid_from_dict, ensure_uid_unique
from pandas.testing import assert_series_equal
import pandas as pd
import unittest


class UIDTestCase(unittest.TestCase):
    def test_gen_uid(self):
        dfa = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["a", "b", "c"])
        dfb = gen_uid(dfa, ["a", "b"])
        assert_series_equal(
            dfb.uid,
            pd.Series(
                [
                    "92963defdbac8bd4a8b7a2cefacc9a9d",
                    "fdda700dba3bbc3d030bf3346b490794",
                ],
                name="uid",
            ),
        )

    def test_gen_uid_from_dict(self):
        obj = {"a": 1, "b": 2, "c": 3}
        self.assertEqual(
            gen_uid_from_dict(obj, ["a", "b"]), "92963defdbac8bd4a8b7a2cefacc9a9d"
        )

    def test_ensure_uid_unique(self):
        dfa = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["a", "b", "c"])
        dfb = pd.DataFrame([[1, 2, 3], [1, 5, 6]], columns=["a", "b", "c"])
        ensure_uid_unique(dfa, ["a"])
        with self.assertRaisesRegex(Exception, r"rows are not unique:"):
            ensure_uid_unique(dfb, ["a"])

import unittest
import pandas as pd
from pandas.testing import assert_series_equal
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

class TestCprrPostUidMatching(unittest.TestCase):
    def test_common_last_name_and_missing_first_name_uid_unchanged(self):
        natchitoches_df = pd.DataFrame({
            "uid": ["u1", "u2", "u3", "u4", "u5", "u6"],
            "first_name": [None, None, "John", "Sarah", None, "Anna"],
            "last_name": ["smith", "johnson", "smith", "lee", "johnson", "brown"],
            "agency": ["natchitoches-so"] * 6,
        })

        post_df = pd.DataFrame({
            "uid": ["u1", "u2", "u3", "u4", "u5", "u6"],
            "first_name": [None, None, "John", "Sarah", None, "Anna"],
            "last_name": ["smith", "johnson", "smith", "lee", "johnson", "brown"],
            "agency": ["natchitoches-so"] * 6,
        })

        common_last_names = (
            post_df["last_name"]
            .value_counts()
            .pipe(lambda x: x[x > 1])
            .index.tolist()
        )

        to_match = natchitoches_df[
            (~natchitoches_df["last_name"].isin(common_last_names)) |
            (natchitoches_df["first_name"].notna())
        ]

        post_to_match = post_df[post_df["uid"].isin(to_match["uid"])]

        dfa = to_match[["uid", "agency", "first_name", "last_name"]].drop_duplicates().set_index("uid")
        dfb = post_to_match[["uid", "agency", "first_name", "last_name"]].drop_duplicates().set_index("uid")

        matcher = ThresholdMatcher(
            ColumnsIndex("agency", "last_name"),
            {
                "agency": JaroWinklerSimilarity(),
                "last_name": JaroWinklerSimilarity(),
                "first_name": JaroWinklerSimilarity(),
            },
            dfa,
            dfb,
            show_progress=True,
        )

        matches = matcher.get_index_pairs_within_thresholds(0.92)
        match_dict = dict(matches)

        df_matched = natchitoches_df.copy()
        df_matched.loc[:, "uid"] = df_matched["uid"].map(lambda x: match_dict.get(x, x))

        skip_mask = (natchitoches_df["last_name"].isin(common_last_names)) & (natchitoches_df["first_name"].isna())
        original_skip_uids = natchitoches_df[skip_mask]["uid"]
        matched_skip_uids = df_matched[skip_mask]["uid"]
        assert_series_equal(matched_skip_uids.reset_index(drop=True), original_skip_uids.reset_index(drop=True), check_names=False)

        matchable_mask = ~skip_mask
        self.assertNotEqual(
            list(df_matched[matchable_mask]["uid"]),
            list(natchitoches_df[matchable_mask]["uid"]),
            msg="Some matchable UIDs should change"
        )

