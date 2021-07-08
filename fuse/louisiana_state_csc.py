from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_appeal_hearing_columns,
    rearrange_event_columns
)
from lib.personnel import fuse_personnel
from lib.uid import ensure_uid_unique
from lib import events
import pandas as pd
import sys
sys.path.append('../')


def fuse_events(lprr, pprr, pprr_term):
    builder = events.Builder()
    builder.extract_events(lprr, {
        events.APPEAL_FILE: {
            'prefix': 'filed', 'keep': ['uid', 'agency', 'appeal_uid']
        },
        events.APPEAL_RENDER: {
            'prefix': 'rendered', 'keep': ['uid', 'agency', 'appeal_uid']
        }
    }, ['uid', 'appeal_uid'])
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {'prefix': 'hire', 'parse_date': True, 'keep': [
            'uid', 'agency', 'department_desc', 'rank_desc', 'salary', 'salary_freq'
        ]},
    }, ['uid'])
    builder.extract_events(pprr_term, {
        events.OFFICER_LEFT: {'prefix': 'left', 'parse_date': True, 'keep': [
            'uid', 'agency', 'department_desc', 'rank_desc', 'left_reason',
        ]}
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    lprr = pd.read_csv(data_file_path(
        'match/lprr_louisiana_state_csc_1991_2020.csv'))
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_louisiana_csd_2021.csv'
    ))
    pprr_term = pd.read_csv(data_file_path(
        'match/pprr_term_louisiana_csd_2021.csv'
    ))
    post_event = pd.read_csv(data_file_path(
        'match/post_event_louisiana_state_police_2020.csv'
    ))
    per_df = fuse_personnel(pprr, lprr, pprr_term)
    event_df = fuse_events(lprr, pprr, pprr_term)
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        fuse_events(lprr, pprr, pprr_term)
    ]))
    ensure_uid_unique(events_df, 'event_uid', True)
    ensure_data_dir('fuse')
    per_df.to_csv(data_file_path(
        'fuse/per_louisiana_state_police.csv'
    ), index=False)
    event_df.to_csv(data_file_path(
        'fuse/event_louisiana_state_police.csv'
    ), index=False)
    rearrange_appeal_hearing_columns(lprr).to_csv(data_file_path(
        'fuse/app_louisiana_state_police.csv'
    ), index=False)
