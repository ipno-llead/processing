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
