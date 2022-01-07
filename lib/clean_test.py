import sys
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from clean import remove_future_dates
from lib.clean import canonicalize_names, float_to_int_str

sys.path.append("./")


class RemoveFutureDates(unittest.TestCase):
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


# class FltIntStr(unittest.TestCase):
#     def test_float_to_int_str(self):
#         data = {"dates": ["1994", "2005", "2011", "1955"]}
#         df = pd.DataFrame(data)

#         df.loc[:, "dates"] = df.dates.astype(float)
#         df = df.pipe(float_to_int_str, ["dates"])

#         dfa = pd.DataFrame(data)
#         dfa.loc[:, "dates"] = dfa.dates.astype(str)

#         df = df[["dates"]]

#         dfa = dfa[["dates"]]

#         assert_frame_equal(df, dfa)


class CanonicalizeNames(unittest.TestCase):
    def test_canonicalize_names(self):
        # officers = {
        #     "uid": [
        #         ("1e65c0807675ee3f25f6a6bf25eb121b"),
        #         ("db3d392b404754b2d4a127ca0922d2f8"),
        #         ("db3d392b404754b2d4a127ca0922d2f8"),
        #         ("db3d392b404754b2d4a1fwrwaf3r3223"),
        #     ],
        #     "first_name": [("patrick"), ("patrick"), ("mariscos"), ("jimbo")],
        #     "last_name": [("star"), ("starr"), ("consome"), ("cruz")],
        # }

        # clusters = {
        #     "uid": [
        #         ("1e65c0807675ee3f25f6a6bf25eb121b"),
        #         ("db3d392b404754b2d4a127ca0922d2f8"),
        #     ],
        #     "first_name": [("patrick"), ("patrick")],
        #     "last_name": [("star"), ("starr")],
        # }

        officers = [
            ("92d3d9c79a3eb44ece5c83e62e55b91d", "thomas", "paine"),
            ("1e65c0807675ee3f25f6a6bf25eb121b", "patrick", "peterman"),
            ("db3d392b404754b2d4a127ca0922d2f8", "patrick", "petermann"),
        ]
        clusters = [
            ("1e65c0807675ee3f25f6a6bf25eb121b", "patrick", "peterman"),
            ("db3d392b404754b2d4a127ca0922d2f8", "patrick", "petermann"),
        ]

        data = pd.DataFrame(officers, columns=["uid", "first_name", "last_name"])

        dfa = canonicalize_names(data, clusters)

        canonicalized_officers = [
            ("92d3d9c79a3eb44ece5c83e62e55b91d", "thomas", "paine"),
            ("1e65c0807675ee3f25f6a6bf25eb121b", "patrick", "petermann"),
            ("db3d392b404754b2d4a127ca0922d2f8", "patrick", "petermann"),
        ]

        dfb = pd.DataFrame(canonicalized_officers, columns=["uid", "first_name", "last_name"])

        assert_frame_equal(dfa, dfb)
