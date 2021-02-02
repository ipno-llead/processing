import unittest
import pandas as pd

from .classifiers import ThresholdClassifier, MatchStatus
from .similarities import StringSimilarity


class TestThresholdClassifier(unittest.TestCase):
    def test_classify(self):
        classifier = ThresholdClassifier({"a": StringSimilarity()})
        self.assertEqual(
            classifier.classify(
                pd.Series(["abc", 123], index=["a", "b"]),
                pd.Series(["abc", 456], index=["a", "b"]),),
            MatchStatus.YES)
        self.assertEqual(
            classifier.classify(
                pd.Series(["abd", 123], index=["a", "b"]),
                pd.Series(["abc", 456], index=["a", "b"]),),
            MatchStatus.MAYBE)
        self.assertEqual(
            classifier.classify(
                pd.Series(["abc", 123], index=["a", "b"]),
                pd.Series(["def", 456], index=["a", "b"]),),
            MatchStatus.NO)
