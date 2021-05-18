import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from events import (
    Builder, cat_type, OFFICER_HIRE, OFFICER_LEFT, COMPLAINT_INCIDENT, COMPLAINT_RECEIVE,
    OFFICER_PAY_EFFECTIVE, OFFICER_RANK, discard_events_occur_more_than_once_every_30_days
)
import salary
import sys
sys.path.append('./')


class EventsBuilderTestCase(unittest.TestCase):
    def assert_events_frame_equal(self, builder, data, columns):
        df = pd.DataFrame(data, columns=columns)
        df.loc[:, 'kind'] = df.kind.astype(cat_type)
        if 'salary_freq' in columns:
            df.loc[:, 'salary_freq'] = df.salary_freq.astype(salary.cat_type)
        frame = builder.to_frame()
        assert_frame_equal(frame, df)

    def test_append_record(self):
        builder = Builder()
        builder.append_record(
            OFFICER_HIRE, ['uid'], year=2020, month=5, day=21, uid='1234', agency='Baton Rouge PD')
        builder.append_record(
            OFFICER_LEFT, ['uid'], year=2021, month=5, day=21, uid='1234', agency='Baton Rouge PD')

        with self.assertRaisesRegex(Exception, r'year column cannot be empty'):
            builder.append_record(
                OFFICER_HIRE, ['uid'], uid='1235', agency='Baton Rouge PD')
        builder.append_record(
            OFFICER_HIRE, ['uid'], ignore_bad_date=True, year=None, uid='1235', agency='Baton Rouge PD')

        self.assert_events_frame_equal(builder, [
            ['da55b73e88980c20f781be16724f1369', OFFICER_HIRE,
             '2020', '5', '21', '1234', 'Baton Rouge PD'],
            ['e1be7c84c5caa8fe5a56d15e62b83d59', OFFICER_LEFT,
             '2021', '5', '21', '1234', 'Baton Rouge PD'],
        ], ['event_uid', 'kind', 'year', 'month', 'day', 'uid', 'agency'])

    def test_append_record_raw_date_str(self):
        builder = Builder()
        builder.append_record(
            OFFICER_HIRE, ['uid'], raw_date_str='7/20/2020', uid='1234', agency='Baton Rouge PD')
        builder.append_record(
            OFFICER_LEFT, ['uid'], raw_date_str='2020-11-03', strptime_format='%Y-%m-%d',
            uid='1234', agency='Baton Rouge PD')
        self.assert_events_frame_equal(builder, [
            ['579bbcf931a66812b94670f2d5aa20d7', OFFICER_HIRE,
             '2020', '7', '20', '7/20/2020', '1234', 'Baton Rouge PD'],
            ['4b39634714c6561bf2a3caff0e14be16', OFFICER_LEFT,
             '2020', '11', '3', '2020-11-03', '1234', 'Baton Rouge PD'],
        ], ['event_uid', 'kind', 'year', 'month', 'day', 'raw_date', 'uid', 'agency'])

    def test_append_record_raw_datetime_str(self):
        builder = Builder()
        builder.append_record(
            OFFICER_HIRE, ['uid'], raw_datetime_str='7/20/2020 09:00', uid='1234', agency='Baton Rouge PD')
        builder.append_record(
            OFFICER_LEFT, ['uid'], raw_datetime_str='20201103 1230', strptime_format='%Y%m%d %H%M',
            uid='1234', agency='Baton Rouge PD')
        self.assert_events_frame_equal(builder, [
            ['a2b284a69e82637b5a8c56c29c828e60', OFFICER_HIRE,
             '2020', '7', '20', '09:00', '7/20/2020 09:00', '1234', 'Baton Rouge PD'],
            ['7a61bff9982b66cd6e813374339fc439', OFFICER_LEFT,
             '2020', '11', '3', '12:30', '20201103 1230', '1234', 'Baton Rouge PD'],
        ], ['event_uid', 'kind', 'year', 'month', 'day', 'time', 'raw_date', 'uid', 'agency'])

    def test_extract_events(self):
        builder = Builder()

        df1 = pd.DataFrame([
            ['1234', 'Brusly PD', 2020, 5, 3, 2021, 2, 6],
            ['1235', 'Brusly PD', 2019, 10, 12, np.NaN, np.NaN, np.NaN],
        ], columns=['uid', 'agency', 'hire_year', 'hire_month', 'hire_day', 'left_year', 'left_month', 'left_day'])
        builder.extract_events(df1, {
            OFFICER_HIRE: {'prefix': 'hire', 'keep': ['uid', 'agency']},
            OFFICER_LEFT: {'prefix': 'left', 'keep': ['uid', 'agency']},
        }, ['uid'])

        df2 = pd.DataFrame([
            ['C1122', '1234', 'Brusly PD', '6/15/2020 12:30', '6/18/2020'],
            ['C2233', '1235', 'Brusly PD', '3/20/2020 10:00', '3/28/2020']
        ], columns=[
            'complaint_uid', 'uid', 'agency', 'occur_datetime', 'receive_date'])
        builder.extract_events(df2, {
            COMPLAINT_INCIDENT: {
                'prefix': 'occur', 'parse_datetime': True, 'keep': ['complaint_uid', 'uid', 'agency']},
            COMPLAINT_RECEIVE: {
                'prefix': 'receive', 'parse_date': True, 'keep': ['complaint_uid', 'uid', 'agency']}
        }, ['complaint_uid', 'uid'])

        df3 = pd.DataFrame([
            ['1236', 'Brusly PD', '20200720', '43210.98', 'yearly',
                'police officer', '20200311 1000'],
            ['1236', 'Brusly PD', '20210112',
                '54321.76', 'yearly', 'sergeant', '20200311 1000'],
        ], columns=['uid', 'agency', 'effective_date', 'salary', 'salary_freq', 'rank_desc', 'hire_datetime'])
        builder.extract_events(df3, {
            OFFICER_HIRE: {
                'prefix': 'hire', 'parse_datetime': '%Y%m%d %H%M', 'drop': ['rank_desc', 'salary', 'salary_freq']},
            OFFICER_PAY_EFFECTIVE: {
                'prefix': 'effective', 'parse_date': '%Y%m%d', 'drop': ['rank_desc']},
            OFFICER_RANK: {
                'prefix': 'effective', 'parse_date': '%Y%m%d', 'drop': ['salary', 'salary_freq'],
                'id_cols': ['uid', 'rank_desc']},
        }, ['uid'])

        self.assert_events_frame_equal(builder, [
            ['ad25f5c06789fa96291a9b9c384a4e52', OFFICER_HIRE,
                '2020', '5', '3', np.NaN, np.NaN, '1234', np.NaN, 'Brusly PD', np.NaN, np.NaN, np.NaN],
            ['1f79d27b602f0d8b9751f69ee2abf865', OFFICER_LEFT,
                '2021', '2', '6', np.NaN, np.NaN, '1234', np.NaN, 'Brusly PD', np.NaN, np.NaN, np.NaN],
            ['cb0c3e5a857bc0c96706777faf5c44fb', OFFICER_HIRE,
                '2019', '10', '12', np.NaN, np.NaN, '1235', np.NaN, 'Brusly PD', np.NaN, np.NaN, np.NaN],
            ['e6ffcd655260bd8f9914bd4aec566cec', COMPLAINT_INCIDENT,
                '2020', '6', '15', '12:30', '6/15/2020 12:30', '1234', 'C1122', 'Brusly PD', np.NaN, np.NaN, np.NaN],
            ['f1d6ade08cff774817e2ffba94badb4c', COMPLAINT_RECEIVE,
                '2020', '6', '18', np.NaN, '6/18/2020', '1234', 'C1122', 'Brusly PD', np.NaN, np.NaN, np.NaN],
            ['ec6db5f177e2077d5bb278c9c9b4b0bb', COMPLAINT_INCIDENT,
                '2020', '3', '20', '10:00', '3/20/2020 10:00', '1235', 'C2233', 'Brusly PD', np.NaN, np.NaN, np.NaN],
            ['781bb698a6cb3fde10bc904d54353b31', COMPLAINT_RECEIVE,
                '2020', '3', '28', np.NaN, '3/28/2020', '1235', 'C2233', 'Brusly PD', np.NaN, np.NaN, np.NaN],
            ['c948a2e1571d10af2b46f9de33c3354f', OFFICER_HIRE, '2020', '3', '11',
                '10:00', '20200311 1000', '1236', np.NaN, 'Brusly PD', np.NaN, np.NaN, np.NaN],
            ['b7a8d5dba5195dcc4a395fba74c8bbec', OFFICER_PAY_EFFECTIVE, '2020', '7',
                '20', np.NaN, '20200720', '1236', np.NaN, 'Brusly PD', np.NaN, '43210.98', 'yearly'],
            ['bf4c14ab4059054b56a2c3bdc07ffbc8', OFFICER_RANK, '2020', '7', '20', np.NaN,
                '20200720', '1236', np.NaN, 'Brusly PD', 'police officer', np.NaN, np.NaN],
            ['21c2d069f6276c756534b89b80d2f653', OFFICER_PAY_EFFECTIVE, '2021', '1',
                '12', np.NaN, '20210112', '1236', np.NaN, 'Brusly PD', np.NaN, '54321.76', 'yearly'],
            ['0ce57935f21186e98875e0b12d479545', OFFICER_RANK, '2021', '1', '12',
                np.NaN, '20210112', '1236', np.NaN, 'Brusly PD', 'sergeant', np.NaN, np.NaN],
        ], ['event_uid', 'kind', 'year', 'month', 'day', 'time', 'raw_date',
            'uid', 'complaint_uid', 'agency', 'rank_desc', 'salary', 'salary_freq'])


class DiscardEventsOccurMoreThanOnceEvery30DaysTestCase(unittest.TestCase):
    def test_discard(self):
        columns = ['kind', 'event_uid', 'uid', 'year', 'month', 'day']
        df = pd.DataFrame([
            ['left', '01', 'abc', '2000', '5', '4'],
            ['left', '02', 'abc', '2000', '4', '28'],
            ['left', '03', 'abc', '2000', '5', '24'],
            ['left', '04', 'def', '2000', '5', '4'],
            ['left', '05', 'def', '2000', '3', '28'],
            ['left', '06', 'qwe', '2000', '4', '10'],
            ['hire', '07', 'abc', '1990', '5', '4'],
            ['hire', '08', 'abc', '1990', '4', '28'],
            ['hire', '09', 'abc', '1990', '5', '24'],
            ['hire', '10', 'def', '1990', '5', '4'],
            ['hire', '11', 'def', '1990', '3', '28'],
            ['hire', '12', 'qwe', '1990', '4', '10'],
        ], columns=columns)
        assert_frame_equal(
            discard_events_occur_more_than_once_every_30_days(
                df, 'left', ['uid']
            ),
            pd.DataFrame([
                ['left', '03', 'abc', '2000', '5', '24'],
                ['left', '04', 'def', '2000', '5', '4'],
                ['left', '05', 'def', '2000', '3', '28'],
                ['left', '06', 'qwe', '2000', '4', '10'],
                ['hire', '07', 'abc', '1990', '5', '4'],
                ['hire', '08', 'abc', '1990', '4', '28'],
                ['hire', '09', 'abc', '1990', '5', '24'],
                ['hire', '10', 'def', '1990', '5', '4'],
                ['hire', '11', 'def', '1990', '3', '28'],
                ['hire', '12', 'qwe', '1990', '4', '10'],
            ], columns=columns)
        )
