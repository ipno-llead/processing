import sys

import pandas as pd

from lib.path import data_file_path
from lib.columns import rearrange_event_columns
from lib.personnel import fuse_personnel
from lib import events

sys.path.append('../')


def fuse_events(ah, pprr):
    builder = events.Builder()

    # extract events from actions history
    employee_id = None
    last_rank = None
    for _, row in ah.iterrows():
        if row['employee_id'] != employee_id:
            employee_id = row['employee_id']
            last_rank = None
        if row['action_desc'] == 'terminate':
            builder.append_record(
                events.OFFICER_LEFT, ['uid'], row['eff_date'],
                employee_id=row['employee_id'], uid=row['uid'], agency=row['agency'], rank_desc=row['rank_desc']
            )
        if row['action_desc'] == 'new hire':
            builder.append_record(
                events.OFFICER_HIRE, ['uid'], row['eff_date'],
                employee_id=row['employee_id'], uid=row['uid'], agency=row['agency'], rank_desc=row['rank_desc']
            )
        if row['rank_desc'] != last_rank:
            last_rank = row['rank_desc']
            builder.append_record(
                events.OFFICER_RANK, ['uid', 'rank_desc'], row['eff_date'],
                employee_id=row['employee_id'], uid=row['uid'], agency=row['agency'], rank_desc=row['rank_desc']
            )

    builder.extract_events(pprr, {
        events.OFFICER_PAY_EFFECTIVE: {
            'prefix': 'salary', 'keep': ['employee_id', 'uid', 'agency', 'salary', 'salary_freq']
        }
    }, ['uid'])
    return builder.to_frame()


if __name__ == '__main__':
    post_event = pd.read_csv(data_file_path(
        'match/post_event_covington_pd_2020.csv'
    ))
    ah = pd.read_csv(data_file_path(
        'clean/actions_history_covington_pd_2021.csv'
    ))
    pprr = pd.read_csv(data_file_path(
        'clean/pprr_covington_pd_2020.csv'
    ))
    events_df = rearrange_event_columns(pd.concat([
        post_event,
        fuse_events(ah, pprr)
    ]))
    events_df.to_csv(data_file_path(
        "fuse/event_covington_pd.csv"), index=False)
    fuse_personnel(ah, pprr).to_csv(data_file_path(
        'fuse/per_covington_pd.csv'), index=False)
