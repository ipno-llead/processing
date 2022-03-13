from io import StringIO
import unittest
from contextlib import redirect_stdout

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from events import (
    Builder,
    event_cat_type,
    OFFICER_HIRE,
    OFFICER_LEFT,
    COMPLAINT_INCIDENT,
    COMPLAINT_RECEIVE,
    OFFICER_PAY_EFFECTIVE,
    OFFICER_RANK,
    discard_events_occur_more_than_once_every_30_days,
)
import salary


class EventsBuilderTestCase(unittest.TestCase):
    def assert_events_frame_equal(self, builder: Builder, df: pd.DataFrame):
        df.loc[:, "kind"] = df.kind.astype(event_cat_type)
        if "salary_freq" in df.columns:
            df.loc[:, "salary_freq"] = df.salary_freq.astype(salary.cat_type)
        frame = builder.to_frame()
        right_frame = df.sort_values(["agency", "kind", "event_uid"]).reset_index(
            drop=True
        )
        try:
            assert_frame_equal(frame, right_frame)
        except AssertionError as e:
            e.args = (
                "%s\n\nleft frame:\n%s\n\nright frame:\n%s"
                % (
                    e.args[0],
                    frame,
                    right_frame,
                ),
            )
            raise

    def test_append_record(self):
        builder = Builder()
        builder.append_record(
            OFFICER_HIRE,
            ["uid"],
            year=2020,
            month=5,
            day=21,
            uid="1234",
            agency="Baton Rouge PD",
        )
        builder.append_record(
            OFFICER_LEFT,
            ["uid"],
            year=2021,
            month=5,
            day=21,
            uid="1234",
            agency="Baton Rouge PD",
        )

        with self.assertRaisesRegex(Exception, r"year column cannot be empty"):
            builder.append_record(
                OFFICER_HIRE, ["uid"], uid="1235", agency="Baton Rouge PD"
            )
        builder.append_record(
            OFFICER_HIRE,
            ["uid"],
            ignore_bad_date=True,
            year=None,
            uid="1235",
            agency="Baton Rouge PD",
        )

        self.assert_events_frame_equal(
            builder,
            pd.DataFrame.from_records(
                [
                    {
                        "event_uid": "da55b73e88980c20f781be16724f1369",
                        "kind": OFFICER_HIRE,
                        "year": "2020",
                        "month": "5",
                        "day": "21",
                        "uid": "1234",
                        "agency": "Baton Rouge PD",
                    },
                    {
                        "event_uid": "e1be7c84c5caa8fe5a56d15e62b83d59",
                        "kind": OFFICER_LEFT,
                        "year": "2021",
                        "month": "5",
                        "day": "21",
                        "uid": "1234",
                        "agency": "Baton Rouge PD",
                    },
                ]
            ),
        )

    def test_append_record_raw_date_str(self):
        builder = Builder()
        builder.append_record(
            OFFICER_HIRE,
            ["uid"],
            raw_date_str="7/20/2020",
            uid="1234",
            agency="Baton Rouge PD",
        )
        builder.append_record(
            OFFICER_LEFT,
            ["uid"],
            raw_date_str="2020-11-03",
            strptime_format="%Y-%m-%d",
            uid="1234",
            agency="Baton Rouge PD",
        )
        self.assert_events_frame_equal(
            builder,
            pd.DataFrame.from_records(
                [
                    {
                        "event_uid": "579bbcf931a66812b94670f2d5aa20d7",
                        "kind": OFFICER_HIRE,
                        "year": "2020",
                        "month": "7",
                        "day": "20",
                        "raw_date": "7/20/2020",
                        "uid": "1234",
                        "agency": "Baton Rouge PD",
                    },
                    {
                        "event_uid": "4b39634714c6561bf2a3caff0e14be16",
                        "kind": OFFICER_LEFT,
                        "year": "2020",
                        "month": "11",
                        "day": "3",
                        "raw_date": "2020-11-03",
                        "uid": "1234",
                        "agency": "Baton Rouge PD",
                    },
                ]
            ),
        )

    def test_append_record_raw_datetime_str(self):
        builder = Builder()
        builder.append_record(
            OFFICER_HIRE,
            ["uid"],
            raw_datetime_str="7/20/2020 09:00",
            uid="1234",
            agency="Baton Rouge PD",
        )
        builder.append_record(
            OFFICER_LEFT,
            ["uid"],
            raw_datetime_str="20201103 1230",
            strptime_format="%Y%m%d %H%M",
            uid="1234",
            agency="Baton Rouge PD",
        )
        self.assert_events_frame_equal(
            builder,
            pd.DataFrame.from_records(
                [
                    {
                        "event_uid": "a2b284a69e82637b5a8c56c29c828e60",
                        "kind": OFFICER_HIRE,
                        "year": "2020",
                        "month": "7",
                        "day": "20",
                        "time": "09:00",
                        "raw_date": "7/20/2020 09:00",
                        "uid": "1234",
                        "agency": "Baton Rouge PD",
                    },
                    {
                        "event_uid": "7a61bff9982b66cd6e813374339fc439",
                        "kind": OFFICER_LEFT,
                        "year": "2020",
                        "month": "11",
                        "day": "3",
                        "time": "12:30",
                        "raw_date": "20201103 1230",
                        "uid": "1234",
                        "agency": "Baton Rouge PD",
                    },
                ]
            ),
        )

    def test_extract_events(self):
        builder = Builder()

        df1 = pd.DataFrame(
            [
                ["1234", "Brusly PD", 2020, 5, 3, 2021, 2, 6],
                ["1235", "Brusly PD", 2019, 10, 12, np.NaN, np.NaN, np.NaN],
            ],
            columns=[
                "uid",
                "agency",
                "hire_year",
                "hire_month",
                "hire_day",
                "left_year",
                "left_month",
                "left_day",
            ],
        )
        builder.extract_events(
            df1,
            {
                OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
                OFFICER_LEFT: {"prefix": "left", "keep": ["uid", "agency"]},
            },
            ["uid"],
        )

        df2 = pd.DataFrame(
            [
                ["C1122", "1234", "Brusly PD", "6/15/2020 12:30", "6/18/2020"],
                ["C2233", "1235", "Brusly PD", "3/20/2020 10:00", "3/28/2020"],
            ],
            columns=[
                "allegation_uid",
                "uid",
                "agency",
                "occur_datetime",
                "receive_date",
            ],
        )
        builder.extract_events(
            df2,
            {
                COMPLAINT_INCIDENT: {
                    "prefix": "occur",
                    "parse_datetime": True,
                    "keep": ["allegation_uid", "uid", "agency"],
                },
                COMPLAINT_RECEIVE: {
                    "prefix": "receive",
                    "parse_date": True,
                    "keep": ["allegation_uid", "uid", "agency"],
                },
            },
            ["allegation_uid", "uid"],
        )

        df3 = pd.DataFrame(
            [
                [
                    "1236",
                    "Brusly PD",
                    "20200720",
                    "43210.98",
                    "yearly",
                    "police officer",
                    "20200311 1000",
                ],
                [
                    "1236",
                    "Brusly PD",
                    "20210112",
                    "54321.76",
                    "yearly",
                    "sergeant",
                    "20200311 1000",
                ],
            ],
            columns=[
                "uid",
                "agency",
                "effective_date",
                "salary",
                "salary_freq",
                "rank_desc",
                "hire_datetime",
            ],
        )
        builder.extract_events(
            df3,
            {
                OFFICER_HIRE: {
                    "prefix": "hire",
                    "parse_datetime": "%Y%m%d %H%M",
                    "drop": ["rank_desc", "salary", "salary_freq"],
                },
                OFFICER_PAY_EFFECTIVE: {
                    "prefix": "effective",
                    "parse_date": "%Y%m%d",
                    "drop": ["rank_desc"],
                },
                OFFICER_RANK: {
                    "prefix": "effective",
                    "parse_date": "%Y%m%d",
                    "drop": ["salary", "salary_freq"],
                    "id_cols": ["uid", "rank_desc"],
                },
            },
            ["uid"],
        )

        self.assert_events_frame_equal(
            builder,
            pd.DataFrame.from_records(
                [
                    {
                        "event_uid": "ad25f5c06789fa96291a9b9c384a4e52",
                        "kind": OFFICER_HIRE,
                        "year": "2020",
                        "month": "5",
                        "day": "3",
                        "time": np.NaN,
                        "raw_date": np.NaN,
                        "uid": "1234",
                        "allegation_uid": np.NaN,
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "1f79d27b602f0d8b9751f69ee2abf865",
                        "kind": OFFICER_LEFT,
                        "year": "2021",
                        "month": "2",
                        "day": "6",
                        "time": np.NaN,
                        "raw_date": np.NaN,
                        "uid": "1234",
                        "allegation_uid": np.NaN,
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "cb0c3e5a857bc0c96706777faf5c44fb",
                        "kind": OFFICER_HIRE,
                        "year": "2019",
                        "month": "10",
                        "day": "12",
                        "time": np.NaN,
                        "raw_date": np.NaN,
                        "uid": "1235",
                        "allegation_uid": np.NaN,
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "e6ffcd655260bd8f9914bd4aec566cec",
                        "kind": COMPLAINT_INCIDENT,
                        "year": "2020",
                        "month": "6",
                        "day": "15",
                        "time": "12:30",
                        "raw_date": "6/15/2020 12:30",
                        "uid": "1234",
                        "allegation_uid": "C1122",
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "f1d6ade08cff774817e2ffba94badb4c",
                        "kind": COMPLAINT_RECEIVE,
                        "year": "2020",
                        "month": "6",
                        "day": "18",
                        "time": np.NaN,
                        "raw_date": "6/18/2020",
                        "uid": "1234",
                        "allegation_uid": "C1122",
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "ec6db5f177e2077d5bb278c9c9b4b0bb",
                        "kind": COMPLAINT_INCIDENT,
                        "year": "2020",
                        "month": "3",
                        "day": "20",
                        "time": "10:00",
                        "raw_date": "3/20/2020 10:00",
                        "uid": "1235",
                        "allegation_uid": "C2233",
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "781bb698a6cb3fde10bc904d54353b31",
                        "kind": COMPLAINT_RECEIVE,
                        "year": "2020",
                        "month": "3",
                        "day": "28",
                        "time": np.NaN,
                        "raw_date": "3/28/2020",
                        "uid": "1235",
                        "allegation_uid": "C2233",
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "c948a2e1571d10af2b46f9de33c3354f",
                        "kind": OFFICER_HIRE,
                        "year": "2020",
                        "month": "3",
                        "day": "11",
                        "time": "10:00",
                        "raw_date": "20200311 1000",
                        "uid": "1236",
                        "allegation_uid": np.NaN,
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "b7a8d5dba5195dcc4a395fba74c8bbec",
                        "kind": OFFICER_PAY_EFFECTIVE,
                        "year": "2020",
                        "month": "7",
                        "day": "20",
                        "time": np.NaN,
                        "raw_date": "20200720",
                        "uid": "1236",
                        "allegation_uid": np.NaN,
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": "43210.98",
                        "salary_freq": "yearly",
                    },
                    {
                        "event_uid": "bf4c14ab4059054b56a2c3bdc07ffbc8",
                        "kind": OFFICER_RANK,
                        "year": "2020",
                        "month": "7",
                        "day": "20",
                        "time": np.NaN,
                        "raw_date": "20200720",
                        "uid": "1236",
                        "allegation_uid": np.NaN,
                        "agency": "Brusly PD",
                        "rank_desc": "police officer",
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                    {
                        "event_uid": "21c2d069f6276c756534b89b80d2f653",
                        "kind": OFFICER_PAY_EFFECTIVE,
                        "year": "2021",
                        "month": "1",
                        "day": "12",
                        "time": np.NaN,
                        "raw_date": "20210112",
                        "uid": "1236",
                        "allegation_uid": np.NaN,
                        "agency": "Brusly PD",
                        "rank_desc": np.NaN,
                        "salary": "54321.76",
                        "salary_freq": "yearly",
                    },
                    {
                        "event_uid": "0ce57935f21186e98875e0b12d479545",
                        "kind": OFFICER_RANK,
                        "year": "2021",
                        "month": "1",
                        "day": "12",
                        "time": np.NaN,
                        "raw_date": "20210112",
                        "uid": "1236",
                        "allegation_uid": np.NaN,
                        "agency": "Brusly PD",
                        "rank_desc": "sergeant",
                        "salary": np.NaN,
                        "salary_freq": np.NaN,
                    },
                ]
            ),
        )

    def test_deduplicate_with_merge_cols(self):
        builder = Builder()

        df = pd.DataFrame(
            [
                ["1234", "Brusly PD", 2020, 5, 3, "321"],
                ["1234", "Brusly PD", 2020, 5, 3, ""],
                ["1235", "Brusly PD", 2019, 10, 12, "445"],
                ["1235", "Brusly PD", 2019, 10, 12, "442"],
            ],
            columns=[
                "uid",
                "agency",
                "hire_year",
                "hire_month",
                "hire_day",
                "badge_no",
            ],
        )
        builder.extract_events(
            df,
            {
                OFFICER_HIRE: {
                    "prefix": "hire",
                    "keep": ["uid", "agency", "badge_no"],
                    "merge_cols": ["badge_no"],
                }
            },
            ["uid", "agency"],
        )

        with self.assertRaises(Exception) as cm:
            builder.to_frame()
        self.assertEqual(
            cm.exception.args[0],
            "\n".join(
                [
                    "event_uid: failed \x1b[35munique\x1b[0m check. \x1b[36m2\x1b[0m offending values:",
                    "  0    b731096b58cfd4d6f113bb85aeb3eaa9",
                    "  1    ee9b823279d8f0a9595567012a81a7f1",
                ]
            ),
        )

    def test_warn_duplications(self):
        self.maxDiff = None
        builder = Builder()

        df = pd.DataFrame(
            [
                ["1234", "Brusly PD", 2020, 5, 3, "321"],
                ["1234", "Brusly PD", 2020, 5, 3, ""],
                ["1235", "Brusly PD", 2019, 10, 12, "445"],
                ["1235", "Brusly PD", 2019, 10, 12, "442"],
            ],
            columns=[
                "uid",
                "agency",
                "hire_year",
                "hire_month",
                "hire_day",
                "badge_no",
            ],
        )
        with redirect_stdout(StringIO()) as f:
            builder.extract_events(
                df,
                {
                    OFFICER_HIRE: {
                        "prefix": "hire",
                        "keep": ["uid", "agency", "badge_no"],
                    }
                },
                ["uid", "agency"],
                warn_duplications=True,
            )
            self.assertEqual(
                f.getvalue(),
                "\n".join(
                    [
                        "WARNING: ignoring duplicated event:",
                        (
                            '    old: {"uid": "1234", "agency": "Brusly PD", "badge_no": "321", "year": 2020, "month": 5, '
                            '"day": 3, "kind": "officer_hire", "event_uid": "b731096b58cfd4d6f113bb85aeb3eaa9"}'
                        ),
                        (
                            '    new: {"uid": "1234", "agency": "Brusly PD", "badge_no": "", "year": 2020, '
                            '"month": 5, "day": 3, "kind": "officer_hire", "event_uid": "b731096b58cfd4d6f113bb85aeb3eaa9"}'
                        ),
                        "WARNING: ignoring duplicated event:",
                        (
                            '    old: {"uid": "1235", "agency": "Brusly PD", "badge_no": "445", "year": 2019, '
                            '"month": 10, "day": 12, "kind": "officer_hire", "event_uid": "ee9b823279d8f0a9595567012a81a7f1"}'
                        ),
                        (
                            '    new: {"uid": "1235", "agency": "Brusly PD", "badge_no": "442", "year": 2019, '
                            '"month": 10, "day": 12, "kind": "officer_hire", "event_uid": "ee9b823279d8f0a9595567012a81a7f1"}'
                        ),
                        "",
                    ]
                ),
            )
        df2 = pd.DataFrame(
            [
                [
                    "b731096b58cfd4d6f113bb85aeb3eaa9",
                    "officer_hire",
                    "2020",
                    "5",
                    "3",
                    "1234",
                    "Brusly PD",
                    "321",
                ],
                [
                    "ee9b823279d8f0a9595567012a81a7f1",
                    "officer_hire",
                    "2019",
                    "10",
                    "12",
                    "1235",
                    "Brusly PD",
                    "445",
                ],
            ],
            columns=[
                "event_uid",
                "kind",
                "year",
                "month",
                "day",
                "uid",
                "agency",
                "badge_no",
            ],
        )
        df2.loc[:, "kind"] = df2["kind"].astype(event_cat_type)
        assert_frame_equal(
            builder.to_frame(),
            df2,
            check_dtype=False,
            check_column_type=False,
            check_categorical=False,
        )


class DiscardEventsOccurMoreThanOnceEvery30DaysTestCase(unittest.TestCase):
    def test_discard(self):
        columns = ["kind", "event_uid", "uid", "year", "month", "day"]
        df = pd.DataFrame(
            [
                ["left", "01", "abc", "2000", "5", "4"],
                ["left", "02", "abc", "2000", "4", "28"],
                ["left", "03", "abc", "2000", "5", "24"],
                ["left", "04", "def", "2000", "5", "4"],
                ["left", "05", "def", "2000", "3", "28"],
                ["left", "06", "qwe", "2000", "4", "10"],
                ["hire", "07", "abc", "1990", "5", "4"],
                ["hire", "08", "abc", "1990", "4", "28"],
                ["hire", "09", "abc", "1990", "5", "24"],
                ["hire", "10", "def", "1990", "5", "4"],
                ["hire", "11", "def", "1990", "3", "28"],
                ["hire", "12", "qwe", "1990", "4", "10"],
            ],
            columns=columns,
        )
        assert_frame_equal(
            discard_events_occur_more_than_once_every_30_days(df, "left", ["uid"]),
            pd.DataFrame(
                [
                    ["left", "03", "abc", "2000", "5", "24"],
                    ["left", "04", "def", "2000", "5", "4"],
                    ["left", "05", "def", "2000", "3", "28"],
                    ["left", "06", "qwe", "2000", "4", "10"],
                    ["hire", "07", "abc", "1990", "5", "4"],
                    ["hire", "08", "abc", "1990", "4", "28"],
                    ["hire", "09", "abc", "1990", "5", "24"],
                    ["hire", "10", "def", "1990", "5", "4"],
                    ["hire", "11", "def", "1990", "3", "28"],
                    ["hire", "12", "qwe", "1990", "4", "10"],
                ],
                columns=columns,
            ),
        )
