import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
import itertools

from .indices import NoopIndex, ColumnsIndex


class BaseIndexTestCase(unittest.TestCase):
    def assert_pairs_equal(self, pair_a, pair_b):
        df1, df2 = pair_a
        df3, df4 = pair_b
        assert_frame_equal(df1, df3)
        assert_frame_equal(df2, df4)

    def assert_pairs_list_equal(self, list_a, list_b):
        self.assertEqual(len(list_a), len(list_b))
        for pair_a, pair_b in itertools.zip_longest(list_a, list_b):
            self.assert_pairs_equal(pair_a, pair_b)


class TestNoopIndex(BaseIndexTestCase):
    def test_index(self):
        dfa = pd.DataFrame([[1, 2], [3, 4]])
        dfb = pd.DataFrame([[5, 6], [7, 8]])
        idx = NoopIndex()
        idx.index(dfa, dfb)
        self.assertEqual(list(idx), [(dfa, dfb)])


class TestColumnsIndex(BaseIndexTestCase):
    def test_index(self):
        cols = ["c", "d"]
        dfa = pd.DataFrame(
            [[1, 2], [2, 4], [3, 4]], index=["x", "y", "z"], columns=cols)
        dfb = pd.DataFrame(
            [[1, 6], [7, 8], [2, 7]], index=["u", "v", "w"], columns=cols)
        idx = ColumnsIndex(["c"])
        idx.index(dfa, dfb)
        self.assert_pairs_list_equal(list(idx), [
            (pd.DataFrame([[1, 2]], index=["x"], columns=cols),
             pd.DataFrame([[1, 6]], index=["u"], columns=cols)),
            (pd.DataFrame([[2, 4]], index=["y"], columns=cols),
             pd.DataFrame([[2, 7]], index=["w"], columns=cols))
        ])

    def test_multi_index(self):
        cols = ["c", "d"]
        dfa = pd.DataFrame(
            [[1, 2], [2, 4], [3, 4], [7, 8]], index=["z", "x", "c", "v"], columns=cols)
        dfb = pd.DataFrame(
            [[1, 6], [3, 4], [7, 8], [2, 7]], index=["q", "w", "e", "r"], columns=cols)
        idx = ColumnsIndex(["c", "d"])
        idx.index(dfa, dfb)
        self.assert_pairs_list_equal(list(idx), [
            (pd.DataFrame([[3, 4]], index=["c"], columns=cols),
             pd.DataFrame([[3, 4]], index=["w"], columns=cols)),
            (pd.DataFrame([[7, 8]], index=["v"], columns=cols),
             pd.DataFrame([[7, 8]], index=["e"], columns=cols))
        ])
