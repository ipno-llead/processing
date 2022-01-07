import sys
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from clean import remove_future_dates
from lib.clean import float_to_int_str

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


class FltIntStr(unittest.TestCase):
    def test_float_to_int_str(self):
        data = {"dates": ["1994", "2005", "2011", "1955"]}
        df = pd.DataFrame(data)

        df.loc[:, "dates"] = df.dates.astype(float)
        df = df.pipe(float_to_int_str, ["dates"])

        dfa = pd.DataFrame(data)
        dfa.loc[:, "dates"] = dfa.dates.astype(str)

        df = df[["dates"]]

        dfa = dfa[["dates"]]

        assert_frame_equal(df, dfa)
