import unittest
import pandas as pd

from .match_records import match_records
from .indices import NoopIndex
from .classifiers import ThresholdClassifier
from .similarities import StringSimilarity


class TestMatchRecords(unittest.TestCase):
    def test_match_records(self):
        cols = ["a", "b"]
        dfa = pd.DataFrame(
            [
                ["ab", "cd"],
            ],
            columns=cols
        )
        dfb = pd.DataFrame(
            [
                ["ab", "cd"],
                ["ae", "vb"],
                ["rt", "qw"]
            ],
            columns=cols
        )
        matches, potential_matches, non_matches = match_records(
            NoopIndex(),
            ThresholdClassifier({"a": StringSimilarity()}),
            dfa,
            dfb
        )
        self.assertEqual(matches, [(0, 0)])
        self.assertEqual(potential_matches, [(0, 1)])
        self.assertEqual(non_matches, [(0, 2)])

    def test_ensure_unique_index(self):
        dfa = pd.DataFrame(
            [[1, 2], [3, 4]], index=["a", "a"]
        )
        dfb = pd.DataFrame(
            [[5, 6], [7, 8]], index=["a", "b"]
        )
        with self.assertRaisesRegex(
                ValueError,
                "Dataframe index contains duplicates. Both frames need to have index free of duplicates."):
            match_records(
                NoopIndex(),
                ThresholdClassifier({"a": StringSimilarity()}),
                dfa,
                dfb
            )
