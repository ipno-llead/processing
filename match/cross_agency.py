import sys

import pandas as pd
from datamatch import (
    ThresholdMatcher, DissimilarFilter, NonOverlappingFilter, ColumnsIndex,
    JaroWinklerSimilarity
)

from lib.path import data_file_path
from lib.date import combine_datetime_columns

sys.path.append('../')


def discard_rows(events: pd.DataFrame, bool_index: pd.Series, desc: str) -> pd.DataFrame:
    before = events.shape[0]
    events = events[bool_index].reset_index(drop=True)
    after = events.shape[0]
    if before > after:
        print('discarded %d %s (%.1f%%)' % (
            before - after, desc, (before - after) / before * 100
        ))
    return events


def cross_match_officers_between_agencies(per, events):
    events = discard_rows(
        events, events.uid.notna(), 'events with empty uid column'
    )
    events = discard_rows(
        events, events.day.notna(), 'events with empty day column'
    )
    for col in ['year', 'month', 'day']:
        events.loc[:, col] = events[col].astype(int)
    events.loc[:, 'datetime'] = combine_datetime_columns(
        events, 'year', 'month', 'day', 'time'
    )

    per = per[['uid', 'first_name', 'last_name']]
    per = discard_rows(
        per, per.first_name.notna() & per.last_name.notna(), 'officers with empty name'
    )
    per.loc[:, 'fc'] = per.first_name.map(lambda x: x[:1])
    agency_dict = events.loc[:, ['uid', 'agency']].drop_duplicates()\
        .set_index('uid').agency.to_dict()
    per.loc[:, 'agency'] = per.uid.map(lambda x: agency_dict.get(x, ''))
    per = discard_rows(
        per, per.agency != '', 'officers with empty agency'
    )
    per = per.set_index('uid')

    # aggregating min/max date
    min_dict = events.set_index(['uid', 'event_uid'])['datetime']\
        .min(level='uid').to_dict()
    max_dict = events.set_index(['uid', 'event_uid'])['datetime']\
        .max(level='uid').to_dict()
    per.loc[:, 'min_date'] = per.index.map(
        lambda x: min_dict.get(x, '')
    )
    per.loc[:, 'max_date'] = per.index.map(
        lambda x: max_dict.get(x, '')
    )
    per = discard_rows(
        per, per.min_date != '', 'officers with no event'
    )

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        'first_name': JaroWinklerSimilarity(),
        'last_name': JaroWinklerSimilarity(),
    }, per, filters=[
        DissimilarFilter('agency'),
        NonOverlappingFilter('min_date', 'max_date')
    ], show_progress=True)
    decision = 0.9
    matcher.save_pairs_to_excel(data_file_path(
        "match/cross_agency_officers.xlsx"
    ), decision)


if __name__ == '__main__':
    per = pd.read_csv(data_file_path('fuse/personnel.csv'))
    print('read personnel file (%d x %d)' % per.shape)
    events = pd.read_csv(data_file_path('fuse/event.csv'))
    print('read events file (%d x %d)' % events.shape)
    cross_match_officers_between_agencies(per, events)
