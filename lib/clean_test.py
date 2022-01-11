import sys
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from clean import remove_future_dates
from lib.clean import canonicalize_officers, float_to_int_str
import numpy as np

sys.path.append("./")


class RemoveFutureDatesTestCase(unittest.TestCase):
    def test_remove_future_dates(self):
        columns = ["name", "birth_year", "birth_month", "birth_day"]
        assert_frame_equal(
            remove_future_dates(
                pd.DataFrame(
                    [
                        ["john", "2000", "10", "1"],
                        ["cate", "2002", "", ""],
                        ["tate", "2001", "3", ""],
                        ["dave", "2001", "2", "4"],
                    ],
                    columns=columns,
                ),
                "2001-02-03",
                ["birth"],
            ),
            pd.DataFrame(
                [
                    ["john", "2000", "10", "1"],
                    ["cate", "", "", ""],
                    ["tate", "", "", ""],
                    ["dave", "", "", ""],
                ],
                columns=columns,
            ),
        )


class FloatToIntStrTestCase(unittest.TestCase):
    def test_float_to_int_str(self):
        """
        This is short but it tests multiple things:
        - float_to_int_str works on multiple columns
        - float_to_int_str only works on specified columns
        - float_to_int_str transforms float columns to integer strs
        - float_to_int_str leave int64 columns alone
        - float_to_int_str transforms "mixed" column (dtype object)
        """
        columns = ["id", "name", "age", "mixed"]
        assert_frame_equal(
            float_to_int_str(
                pd.DataFrame(
                    [
                        [3, "john", 27.0, 1973.0],
                        [4, "anne", 24.0, np.nan],
                        [5, "bill", np.nan, "abc"],
                    ],
                    columns=columns,
                ),
                ["id", "age", "mixed"],
            ),
            pd.DataFrame(
                [
                    [3, "john", "27", "1973"],
                    [4, "anne", "24", ""],
                    [5, "bill", "", "abc"],
                ],
                columns=columns,
            ),
        )


class CanonicalizeNamesTestCase(unittest.TestCase):
    def test_canonicalize_names(self):
        assert_frame_equal(
            canonicalize_officers(
                pd.DataFrame(
                    [
                        {
                            "uid": "1e65c0807675ee3f25f6a6bf25eb121b",
                            "first_name": "patrick",
                            "last_name": "peterman",
                            "agency": "Baton Rouge PD",
                            "rank_desc": "sergeant",
                            "id": 10,
                        },
                        {
                            "uid": "db3d392b404754b2d4a127ca0922d2f8",
                            "first_name": "patrick",
                            "last_name": "petermann",
                            "agency": "Baton Rouge PD",
                            "rank_desc": "sergeant",
                            "id": 9,
                        },
                        {
                            "uid": "92d3d9c79a3eb44ece5c83e62e55b91d",
                            "first_name": "thoma",
                            "last_name": "ferguson",
                            "agency": "New Orleans PD",
                            "rank_desc": "commander",
                            "id": 8,
                        },
                        {
                            "uid": "a5f3c016d4c3373aa74dc15c0638362e",
                            "first_name": "thomas",
                            "last_name": "ferguson",
                            "agency": "New Orleans PD",
                            "rank_desc": "commander",
                            "id": 7,
                        },
                        {
                            "uid": "233418697784e972144523ad8cc4ed9",
                            "first_name": "arthur",
                            "middle_name": "h",
                            "last_name": "schopenhauer",
                            "agency": "Ye PD",
                            "rank_desc": "chief",
                            "id": 6,
                        },
                        {
                            "uid": "d59c942cdd7e211bf7c76f20501d657c",
                            "first_name": "arthur",
                            "middle_name": "hesse",
                            "last_name": "schopenhauer",
                            "agency": "Ye PD",
                            "rank_desc": "chief",
                            "id": 5,
                        },
                    ]
                ),
                clusters=[
                    (
                        "1e65c0807675ee3f25f6a6bf25eb121b",
                        "db3d392b404754b2d4a127ca0922d2f8",
                    ),
                    (
                        "a5f3c016d4c3373aa74dc15c0638362e",
                        "92d3d9c79a3eb44ece5c83e62e55b91d",
                    ),
                    (
                        "d59c942cdd7e211bf7c76f20501d657c",
                        "233418697784e972144523ad8cc4ed9",
                    ),
                ],
            ),
            pd.DataFrame(
                [
                    {
                        "uid": "1e65c0807675ee3f25f6a6bf25eb121b",
                        "first_name": "patrick",
                        "last_name": "peterman",
                        "agency": "Baton Rouge PD",
                        "rank_desc": "sergeant",
                        "id": 10,
                    },
                    {
                        "uid": "1e65c0807675ee3f25f6a6bf25eb121b",
                        "first_name": "patrick",
                        "last_name": "peterman",
                        "agency": "Baton Rouge PD",
                        "rank_desc": "sergeant",
                        "id": 9,
                    },
                    {
                        "uid": "a5f3c016d4c3373aa74dc15c0638362e",
                        "first_name": "thomas",
                        "last_name": "ferguson",
                        "agency": "New Orleans PD",
                        "rank_desc": "commander",
                        "id": 8,
                    },
                    {
                        "uid": "a5f3c016d4c3373aa74dc15c0638362e",
                        "first_name": "thomas",
                        "last_name": "ferguson",
                        "agency": "New Orleans PD",
                        "rank_desc": "commander",
                        "id": 7,
                    },
                    {
                        "uid": "d59c942cdd7e211bf7c76f20501d657c",
                        "first_name": "arthur",
                        "middle_name": "hesse",
                        "last_name": "schopenhauer",
                        "agency": "Ye PD",
                        "rank_desc": "chief",
                        "id": 6,
                    },
                    {
                        "uid": "d59c942cdd7e211bf7c76f20501d657c",
                        "first_name": "arthur",
                        "middle_name": "hesse",
                        "last_name": "schopenhauer",
                        "agency": "Ye PD",
                        "rank_desc": "chief",
                        "id": 5,
                    },
                ]
            ),
        )
