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

class TestCprrPostUidMatching(unittest.TestCase):
    def test_common_last_name_uid_unchanged(self):
        natchitoches_df = pd.DataFrame({
            "uid": ["u1", "u2", "u3", "u4"],
            "last_name": ["smith", "johnson", "smith", "lee"],
            "agency": ["natchitoches-so"] * 4,
        })

        post_df = pd.DataFrame({
            "uid": ["u1", "u2", "u3", "u4"],
            "last_name": ["smith", "johnson", "smith", "lee"],
            "agency": ["natchitoches-so"] * 4,
        })

        common_last_names = (
            post_df["last_name"]
            .value_counts()
            .pipe(lambda x: x[x > 1])
            .index.tolist()
        )
        uncommon_last_names = (
            post_df["last_name"]
            .value_counts()
            .pipe(lambda x: x[x == 1])
            .index.tolist()
        )

        common_last_name_natchitoches_df = natchitoches_df[natchitoches_df["last_name"].isin(common_last_names)]
        common_last_name_post_df = post_df[post_df["last_name"].isin(common_last_names)]

        common_last_name_natchitoches_df = common_last_name_natchitoches_df.sort_values("uid").reset_index(drop=True)
        common_last_name_post_df = common_last_name_post_df.sort_values("uid").reset_index(drop=True)


        uncommon_last_name_natchitoches_df = natchitoches_df[~natchitoches_df["last_name"].isin(uncommon_last_names)]
        uncommon_last_name_post_df = post_df[~post_df["last_name"].isin(uncommon_last_names)]

        uncommon_last_name_natchitoches_df = uncommon_last_name_natchitoches_df[["uid", "agency", "last_name"]].drop_duplicates(subset=["uid"]).set_index("uid")
        uncommon_last_name_post_df = uncommon_last_name_post_df[["uid", "agency", "last_name"]].drop_duplicates(subset=["uid"]).set_index("uid")

        matcher = ThresholdMatcher(
            ColumnsIndex("agency", "last_name"),
            {
                "agency": JaroWinklerSimilarity(),
                "last_name": JaroWinklerSimilarity(),
            },
            uncommon_last_name_natchitoches_df,
            uncommon_last_name_post_df,
            show_progress=True,
        )

        decision = 0.92
        matches = matcher.get_index_pairs_within_thresholds(decision)

        match_dict = dict(matches)

        natchitoches_df_with_matches = natchitoches_df.copy()

        natchitoches_df_with_matches.loc[:, "uid"] = natchitoches_df_with_matches["uid"].map(lambda x: match_dict.get(x, x))

        natchitoches_df = natchitoches_df[natchitoches_df.last_name.isin(common_last_names)]
        natchitoches_df_with_matches_common_last_names = natchitoches_df_with_matches[natchitoches_df_with_matches.last_name.isin(common_last_names)]

        assert_series_equal(natchitoches_df_with_matches_common_last_names["uid"], natchitoches_df["uid"], check_names=False)

