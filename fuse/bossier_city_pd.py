import sys
sys.path.append('../')
import pandas as pd
from lib.personnel import fuse_personnel
from lib.columns import rearrange_event_columns
from lib.path import data_file_path
from lib import events


def fuse_events(pprr):
    builder = events.Builder()
    builder.extract_events(pprr, {
        events.OFFICER_HIRE: {
            'prefix': 'hire',
            'keep': ['uid', 'agency', 'rank_desc', 'salary', 'salary_freq']},
    }, ['uid'])


if __name__ == '__main__':
    pprr = pd.read_csv(data_file_path('clean/pprr_bossier_city_pd_2000_2019.csv'))
    post_event = pd.read_csv(data_file_path('match/post_event_bossier_city_pd.csv'))
    per_df = fuse_personnel(pprr)
    events_df = rearrange_event_columns(pd.concat([
        fuse_events(pprr),
        post_event
    ]))
    per_df.to_csv(data_file_path(
        'fuse/per_bossier_city_pd.csv'), index=False)
    events_df.to_csv(data_file_path(
        'fuse/event_bossier_city_pd.csv'), index=False)
