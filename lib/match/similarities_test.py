import unittest
from datetime import date

from .similarities import StringSimilarity, DateSimilarity, JaroWinklerSimilarity


class TestStringSimilarity(unittest.TestCase):
    def test_sim(self):
        obj = StringSimilarity()
        self.assertEqual(obj.sim("abc", "abc"), 1)
        self.assertEqual(obj.sim("abc", "123"), 0)
        self.assertEqual(obj.sim("abce", "abcd"), 0.75)
        self.assertEqual(obj.sim("thang", "thăng"), 1)


class TestJaroWinklerSimilarity(unittest.TestCase):
    def test_sim(self):
        obj = JaroWinklerSimilarity(0.2)
        self.assertEqual(obj.sim("abc", "abc"), 1)
        self.assertEqual(obj.sim("abc", "123"), 0)
        self.assertEqual(obj.sim("abce", "abcd"), 0.9333333333333333)
        self.assertEqual(obj.sim("wbcd", "abcd"), 0.8333333333333334)


class TestDateSimilarity(unittest.TestCase):
    def test_sim(self):
        obj = DateSimilarity()
        self.assertEqual(obj.sim(date(2000, 10, 11), date(2000, 10, 11)), 1)
        # within 30 days difference
        self.assertEqual(obj.sim(date(2000, 10, 11), date(2000, 10, 5)), 0.8)
        self.assertEqual(
            obj.sim(date(2000, 10, 11), date(2000, 11, 5)), 0.16666666666666663)
        # completely different days
        self.assertEqual(obj.sim(date(2000, 10, 11), date(2001, 3, 15)), 0)
        # day & month is swapped
        self.assertEqual(obj.sim(date(2000, 9, 11), date(2000, 11, 9)), 0.5)
        # same year and day but month is different
        self.assertEqual(obj.sim(date(2000, 3, 20), date(2000, 8, 20)), 0.875)
