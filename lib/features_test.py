from typing import Dict
import unittest
from black import Line

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from lib.features import (
    ColumnsAccessor,
    LinenoAccessor,
    MaxlineAccessor,
    TextAccessor,
    extract_features,
    Condition,
    Column,
)


class ConditionTestCase(unittest.TestCase):
    def test_run(self):
        self.assertTrue(Condition(lambda: True)())
        self.assertFalse(Condition(lambda: False)())
        self.assertTrue(
            Condition(
                lambda row, text, lineno, maxline: len(row) == 0
                and text == "abc"
                and lineno == 1
                and maxline == 3
            )(pd.Series(), "abc", 1, 3)
        )

    def test_and(self):
        self.assertTrue((Condition(lambda: True) & Condition(lambda: True))())
        self.assertFalse((Condition(lambda: True) & Condition(lambda: False))())
        self.assertFalse((Condition(lambda: False) & Condition(lambda: False))())
        self.assertFalse((Condition(lambda: False) & Condition(lambda: True))())

    def test_or(self):
        self.assertTrue((Condition(lambda: True) | Condition(lambda: True))())
        self.assertTrue((Condition(lambda: True) | Condition(lambda: False))())
        self.assertFalse((Condition(lambda: False) | Condition(lambda: False))())
        self.assertTrue((Condition(lambda: False) | Condition(lambda: True))())

    def test_xor(self):
        self.assertFalse((Condition(lambda: True) ^ Condition(lambda: True))())
        self.assertTrue((Condition(lambda: True) ^ Condition(lambda: False))())
        self.assertFalse((Condition(lambda: False) ^ Condition(lambda: False))())
        self.assertTrue((Condition(lambda: False) ^ Condition(lambda: True))())

    def test_invert(self):
        self.assertFalse((~Condition(lambda: True))())
        self.assertTrue((~Condition(lambda: False))())


class ColumnTestCase(unittest.TestCase):
    def test_of(self):
        self.assertEqual(
            Column("name").of(
                pd.Series([1, "dave", np.NaN], index=["id", "name", "age"])
            ),
            "dave",
        )

    def assert_condition(self, cond: Condition, series_values: Dict, result: bool):
        self.assertEqual(cond(pd.Series(series_values), None, None, None), result)

    def test_eq(self):
        cond = Column("name") == "dave"
        self.assert_condition(cond, {"name": "dave"}, True)
        self.assert_condition(cond, {"name": "john"}, False)

    def test_ne(self):
        cond = Column("name") != "dave"
        self.assert_condition(cond, {"name": "dave"}, False)
        self.assert_condition(cond, {"name": "john"}, True)

    def test_lt(self):
        cond = Column("age") < 10
        self.assert_condition(cond, {"age": 3}, True)
        self.assert_condition(cond, {"age": 10}, False)
        self.assert_condition(cond, {"age": 11}, False)

    def test_gt(self):
        cond = Column("age") > 3
        self.assert_condition(cond, {"age": 2}, False)
        self.assert_condition(cond, {"age": 3}, False)
        self.assert_condition(cond, {"age": 11}, True)

    def test_ge(self):
        cond = Column("age") >= 10
        self.assert_condition(cond, {"age": 2}, False)
        self.assert_condition(cond, {"age": 10}, True)
        self.assert_condition(cond, {"age": 11}, True)

    def test_le(self):
        cond = Column("age") <= 10
        self.assert_condition(cond, {"age": 2}, True)
        self.assert_condition(cond, {"age": 10}, True)
        self.assert_condition(cond, {"age": 11}, False)

    def test_match(self):
        cond = Column("name").match(r"lt\.")
        self.assert_condition(cond, {"name": "lt. John"}, True)
        self.assert_condition(cond, {"name": "John"}, False)

    def test_isna(self):
        cond = Column("name").isna()
        self.assert_condition(cond, {"name": np.NaN}, True)
        self.assert_condition(cond, {"name": "John"}, False)

    def test_notna(self):
        cond = Column("name").notna()
        self.assert_condition(cond, {"name": np.NaN}, False)
        self.assert_condition(cond, {"name": "John"}, True)


class ColumnsAccessorTestCase(unittest.TestCase):
    def test_getattr(self):
        columns = ColumnsAccessor()
        self.assertEqual(
            columns.name.of(
                pd.Series([1, "dave", np.NaN], index=["id", "name", "age"])
            ),
            "dave",
        )


class TextAccessorTestCase(unittest.TestCase):
    def test_eq(self):
        cond = TextAccessor() == "abc"
        self.assertTrue(cond(None, "abc", None, None))
        self.assertFalse(cond(None, "def", None, None))

    def test_ne(self):
        cond = TextAccessor() != "abc"
        self.assertFalse(cond(None, "abc", None, None))
        self.assertTrue(cond(None, "def", None, None))

    def test_lt(self):
        cond = TextAccessor() < "abc"
        self.assertTrue(cond(None, "aaa", None, None))
        self.assertFalse(cond(None, "abc", None, None))
        self.assertFalse(cond(None, "def", None, None))

    def test_gt(self):
        cond = TextAccessor() > "abc"
        self.assertFalse(cond(None, "aaa", None, None))
        self.assertFalse(cond(None, "abc", None, None))
        self.assertTrue(cond(None, "def", None, None))

    def test_ge(self):
        cond = TextAccessor() >= "abc"
        self.assertFalse(cond(None, "aaa", None, None))
        self.assertTrue(cond(None, "abc", None, None))
        self.assertTrue(cond(None, "def", None, None))

    def test_le(self):
        cond = TextAccessor() <= "abc"
        self.assertTrue(cond(None, "aaa", None, None))
        self.assertTrue(cond(None, "abc", None, None))
        self.assertFalse(cond(None, "def", None, None))

    def test_match(self):
        cond = TextAccessor().match(r"^a")
        self.assertTrue(cond(None, "abc", None, None))
        self.assertFalse(cond(None, "def", None, None))


class LinenoAccessorTestCase(unittest.TestCase):
    def test_eq(self):
        cond = LinenoAccessor() == 10
        self.assertTrue(cond(None, None, 10, None))
        self.assertFalse(cond(None, None, 11, None))

    def test_ne(self):
        cond = LinenoAccessor() != 11
        self.assertTrue(cond(None, None, 10, None))
        self.assertFalse(cond(None, None, 11, None))

    def test_lt(self):
        cond = LinenoAccessor() < 10
        self.assertTrue(cond(None, None, 5, None))
        self.assertFalse(cond(None, None, 10, None))
        self.assertFalse(cond(None, None, 15, None))

    def test_gt(self):
        cond = LinenoAccessor() > 10
        self.assertFalse(cond(None, None, 5, None))
        self.assertFalse(cond(None, None, 10, None))
        self.assertTrue(cond(None, None, 15, None))

    def test_ge(self):
        cond = LinenoAccessor() >= 10
        self.assertFalse(cond(None, None, 5, None))
        self.assertTrue(cond(None, None, 10, None))
        self.assertTrue(cond(None, None, 15, None))

    def test_le(self):
        cond = LinenoAccessor() <= 10
        self.assertTrue(cond(None, None, 5, None))
        self.assertTrue(cond(None, None, 10, None))
        self.assertFalse(cond(None, None, 15, None))

    def test_add(self):
        cond = (LinenoAccessor() + 5) == 15
        self.assertTrue(cond(None, None, 10, None))

    def test_sub(self):
        cond = (LinenoAccessor() - 5) == 5
        self.assertTrue(cond(None, None, 10, None))

    def test_mul(self):
        cond = (LinenoAccessor() * 5) == 50
        self.assertTrue(cond(None, None, 10, None))

    def test_truediv(self):
        cond = (LinenoAccessor() / 5) == 2
        self.assertTrue(cond(None, None, 10, None))

    def test_mod(self):
        cond = (LinenoAccessor() % 3) == 1
        self.assertTrue(cond(None, None, 10, None))

    def test_floordiv(self):
        cond = (LinenoAccessor() // 3) == 3
        self.assertTrue(cond(None, None, 10, None))


class MaxlineAccessorTestCase(unittest.TestCase):
    def test_eq(self):
        cond = MaxlineAccessor() == 10
        self.assertTrue(cond(None, None, None, 10))
        self.assertFalse(cond(None, None, None, 11))

    def test_ne(self):
        cond = MaxlineAccessor() != 11
        self.assertTrue(cond(None, None, None, 10))
        self.assertFalse(cond(None, None, None, 11))

    def test_lt(self):
        cond = MaxlineAccessor() < 10
        self.assertTrue(cond(None, None, None, 5))
        self.assertFalse(cond(None, None, None, 10))
        self.assertFalse(cond(None, None, None, 15))

    def test_gt(self):
        cond = MaxlineAccessor() > 10
        self.assertFalse(cond(None, None, None, 5))
        self.assertFalse(cond(None, None, None, 10))
        self.assertTrue(cond(None, None, None, 15))

    def test_ge(self):
        cond = MaxlineAccessor() >= 10
        self.assertFalse(cond(None, None, None, 5))
        self.assertTrue(cond(None, None, None, 10))
        self.assertTrue(cond(None, None, None, 15))

    def test_le(self):
        cond = MaxlineAccessor() <= 10
        self.assertTrue(cond(None, None, None, 5))
        self.assertTrue(cond(None, None, None, 10))
        self.assertFalse(cond(None, None, None, 15))

    def test_add(self):
        cond = (MaxlineAccessor() + 5) == 15
        self.assertTrue(cond(None, None, None, 10))

    def test_sub(self):
        cond = (MaxlineAccessor() - 5) == 5
        self.assertTrue(cond(None, None, None, 10))

    def test_mul(self):
        cond = (MaxlineAccessor() * 5) == 50
        self.assertTrue(cond(None, None, None, 10))

    def test_truediv(self):
        cond = (MaxlineAccessor() / 5) == 2
        self.assertTrue(cond(None, None, None, 10))

    def test_mod(self):
        cond = (MaxlineAccessor() % 3) == 1
        self.assertTrue(cond(None, None, None, 10))

    def test_floordiv(self):
        cond = (MaxlineAccessor() // 3) == 3
        self.assertTrue(cond(None, None, None, 10))


class ExtractFeaturesTestCase(unittest.TestCase):
    def test_run(self):
        assert_frame_equal(
            extract_features(
                pd.DataFrame.from_records(
                    [
                        {
                            "sex": "M",
                            "text": "Prof. John\nChristian",
                            "years": 20,
                            "vehicle": "Tesla",
                        },
                        {
                            "sex": "F",
                            "text": "Asst. Jane\nHindu",
                            "years": 30,
                            "vehicle": np.NaN,
                        },
                        {
                            "sex": "U",
                            "text": "Chancellor Dave",
                            "years": 40,
                            "vehicle": np.NaN,
                        },
                    ],
                ),
                get_instructions=lambda columns, text, lineno, maxline: {
                    "gender": {
                        "unknown": columns.sex.match(r"^U"),
                        "male": columns.sex == "M",
                        "female": columns.sex != "M",
                    },
                    "age": {
                        "young": columns.years < 21,
                        "middle": (columns.years >= 21) & (columns.years <= 35),
                        "old": columns.years > 35,
                    },
                    "has_car": {
                        True: columns.vehicle.notna(),
                        False: columns.vehicle.isna(),
                    },
                    "title": {
                        "professor": text.match(r"^Prof\. "),
                        "not_assistant": (lineno == 0) & ~text.match(r"^Asst\. "),
                    },
                    "religion": {
                        "christian": text == "Christian",
                        "atheist": (lineno == maxline) & (text != "Buddhist"),
                    },
                },
            ).sort_index(axis=1),
            pd.DataFrame.from_records(
                [
                    {
                        "sex": "M",
                        "text": "Prof. John\nChristian",
                        "years": 20,
                        "vehicle": "Tesla",
                        "gender": "male",
                        "age": "young",
                        "title": "professor",
                        "has_car": True,
                        "religion": "christian",
                    },
                    {
                        "sex": "F",
                        "text": "Asst. Jane\nHindu",
                        "years": 30,
                        "vehicle": np.NaN,
                        "gender": "female",
                        "age": "middle",
                        "title": np.NaN,
                        "has_car": False,
                        "religion": "atheist",
                    },
                    {
                        "sex": "U",
                        "text": "Chancellor Dave",
                        "years": 40,
                        "vehicle": np.NaN,
                        "gender": "unknown",
                        "age": "old",
                        "title": "not_assistant",
                        "has_car": False,
                        "religion": "atheist",
                    },
                ],
            ).sort_index(axis=1),
            check_dtype=False,
        )
