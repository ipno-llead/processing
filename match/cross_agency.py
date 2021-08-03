import pathlib
import sys

import numpy as np
import pandas as pd
from datamatch import (
    ThresholdMatcher, DissimilarFilter, NonOverlappingFilter, ColumnsIndex,
    JaroWinklerSimilarity
)
from datavalid.spinner import Spinner

from lib.path import data_file_path
from lib.date import combine_date_columns

sys.path.append('../')


def discard_rows(events: pd.DataFrame, bool_index: pd.Series, desc: str, reset_index: bool = False) -> pd.DataFrame:
    before = events.shape[0]
    events = events[bool_index]
    if reset_index:
        events = events.reset_index(drop=True)
    after = events.shape[0]
    if before > after:
        print('discarded %d %s (%.1f%%)' % (
            before - after, desc, (before - after) / before * 100
        ))
    return events


def assign_min_col(events: pd.DataFrame, per: pd.DataFrame, col: str):
    min_dict = events[col].min(level='uid').to_dict()
    per.loc[:, 'min_'+col] = per.index.map(
        lambda x: min_dict.get(x, np.NaN)
    )


def assign_max_col(events: pd.DataFrame, per: pd.DataFrame, col: str):
    max_dict = events[col].max(level='uid').to_dict()
    per.loc[:, 'max_'+col] = per.index.map(
        lambda x: max_dict.get(x, np.NaN)
    )


def cross_match_officers_between_agencies(per, events):
    events = discard_rows(
        events, events.uid.notna(), 'events with empty uid column', reset_index=True
    )
    events = discard_rows(
        events, events.day.notna(), 'events with empty day column', reset_index=True
    )
    for col in ['year', 'month', 'day']:
        events.loc[:, col] = events[col].astype(int)
    events.loc[:, 'date'] = combine_date_columns(
        events, 'year', 'month', 'day'
    )
    events = discard_rows(
        events, events.date.notna(), 'events with empty date', reset_index=True
    )
    events.loc[:, 'timestamp'] = events['date'].map(
        lambda x: x.timestamp()
    )

    per = per[['uid', 'first_name', 'last_name']]
    per = discard_rows(
        per, per.first_name.notna() & per.last_name.notna(), 'officers with empty name', reset_index=True
    )
    per.loc[:, 'fc'] = per.first_name.map(lambda x: x[:1])
    agency_dict = events.loc[:, ['uid', 'agency']].drop_duplicates()\
        .set_index('uid').agency.to_dict()
    per.loc[:, 'agency'] = per.uid.map(lambda x: agency_dict.get(x, ''))
    per = discard_rows(
        per, per.agency != '', 'officers with empty agency', reset_index=True
    )
    per = per.set_index('uid')

    # aggregating min/max date
    events = events.set_index(['uid', 'event_uid'])
    assign_min_col(events, per, 'date')
    assign_max_col(events, per, 'date')
    assign_min_col(events, per, 'timestamp')
    assign_max_col(events, per, 'timestamp')
    per = discard_rows(
        per, per.min_date.notna(), 'officers with no event'
    )

    excel_path = data_file_path(
        "match/cross_agency_officers.xlsx"
    )
    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, per, filters=[
        DissimilarFilter('agency'),
        NonOverlappingFilter('min_timestamp', 'max_timestamp')
    ], show_progress=True)
    decision = 0.98
    with Spinner('saving pairs to Excel file'):
        matcher.save_pairs_to_excel(excel_path, decision)
    print('saved pairs to %s' % excel_path.relative_to(pathlib.Path.cwd()))


if __name__ == '__main__':
    per = pd.read_csv(data_file_path('fuse/personnel.csv'))
    print('read personnel file (%d x %d)' % per.shape)
    events = pd.read_csv(data_file_path('fuse/event.csv'))
    print('read events file (%d x %d)' % events.shape)
    cross_match_officers_between_agencies(per, events)
