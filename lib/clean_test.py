import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from lib.clean import canonicalize_officers, float_to_int_str, remove_future_dates


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


class CanonicalizeOfficersTestCase(unittest.TestCase):
    def test_canonicalize_officers(self):
        assert_frame_equal(
            canonicalize_officers(
                pd.DataFrame.from_records(
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
                            "first_name": "t",
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
                            "uid": "495e30344d6f5913ac814caba56abb5d",
                            "first_name": "michael",
                            "last_name": "dephillips",
                            "agency": "YMCMB",
                            "rank_desc": "commander",
                            "id": 5,
                        },
                        {
                            "uid": "dd9d72b5a514b416c15486a9b759b51b",
                            "first_name": "micheal",
                            "last_name": "dephillips",
                            "agency": "YMCMB",
                            "rank_desc": "commander",
                            "id": 5,
                        },
                        {
                            "uid": "553baca6957acfbbf57832d22fabc4e2",
                            "first_name": "mike",
                            "last_name": "dephillips",
                            "agency": "YMCMB",
                            "rank_desc": "commander",
                            "id": 5,
                        },
                    ]
                ).fillna(""),
                clusters=[
                    (
                        "1e65c0807675ee3f25f6a6bf25eb121b",
                        "db3d392b404754b2d4a127ca0922d2f8",
                    ),
                    (
                        "92d3d9c79a3eb44ece5c83e62e55b91d",
                        "a5f3c016d4c3373aa74dc15c0638362e",
                    ),
                    (
                        "495e30344d6f5913ac814caba56abb5d",
                        "dd9d72b5a514b416c15486a9b759b51b",
                        "553baca6957acfbbf57832d22fabc4e2",
                    ),
                ],
            ),
            pd.DataFrame.from_records(
                [
                    {
                        "uid": "db3d392b404754b2d4a127ca0922d2f8",
                        "first_name": "patrick",
                        "last_name": "petermann",
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
                        "uid": "495e30344d6f5913ac814caba56abb5d",
                        "first_name": "michael",
                        "last_name": "dephillips",
                        "agency": "YMCMB",
                        "rank_desc": "commander",
                        "id": 5,
                    },
                    {
                        "uid": "495e30344d6f5913ac814caba56abb5d",
                        "first_name": "michael",
                        "last_name": "dephillips",
                        "agency": "YMCMB",
                        "rank_desc": "commander",
                        "id": 5,
                    },
                    {
                        "uid": "495e30344d6f5913ac814caba56abb5d",
                        "first_name": "michael",
                        "last_name": "dephillips",
                        "agency": "YMCMB",
                        "rank_desc": "commander",
                        "id": 5,
                    },
                ]
            ),
        )


def test_middle_name(self):
    assert_frame_equal(
        canonicalize_officers(
            pd.DataFrame.from_records(
                [
                    {
                        "uid": "1e65c0807675ee3f25f6a6bf25eb121b",
                        "first_name": "patrick",
                        "last_name": "peterman",
                        "middle_name": "m",
                        "agency": "Baton Rouge PD",
                        "rank_desc": "sergeant",
                        "id": 10,
                    },
                    {
                        "uid": "db3d392b404754b2d4a127ca0922d2f8",
                        "first_name": "patrick",
                        "last_name": "peterman",
                        "middle_name": "matt",
                        "agency": "Baton Rouge PD",
                        "rank_desc": "sergeant",
                        "id": 10,
                    },
                ]
            ),
            clusters=[
                (
                    "1e65c0807675ee3f25f6a6bf25eb121b",
                    "db3d392b404754b2d4a127ca0922d2f8",
                ),
            ],
        ),
        pd.DataFrame.from_records(
            [
                {
                    "uid": "db3d392b404754b2d4a127ca0922d2f8",
                    "first_name": "patrick",
                    "last_name": "peterman",
                    "middle_name": "matt",
                    "agency": "Baton Rouge PD",
                    "rank_desc": "sergeant",
                    "id": 10,
                },
                {
                    "uid": "db3d392b404754b2d4a127ca0922d2f8",
                    "first_name": "patrick",
                    "last_name": "peterman",
                    "middle_name": "matt",
                    "agency": "Baton Rouge PD",
                    "rank_desc": "sergeant",
                    "id": 10,
                },
            ]
        ),
    )
