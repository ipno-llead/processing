import sys

from datamatch.indices import ColumnsIndex
sys.path.append("../")
from datamatch import ThresholdMatcher, NoopIndex, JaroWinklerSimilarity
import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.post import extract_events_from_post


def prepare_post_data():
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    return post


def extract_post_events(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=['uid']).set_index('uid')
    dfa.loc[:, 'fc'] = dfa.first_name.fillna('').map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=['uid']).set_index('uid')
    dfb.loc[:, 'fc'] = dfb.first_name.fillna('').map(lambda x: x[:1])

    matcher = ThresholdMatcher(ColumnsIndex('fc'), {
        "first_name": JaroWinklerSimilarity(),
        "last_name": JaroWinklerSimilarity()
    }, dfa, dfb)
    decision = 0.912
    matcher.save_pairs_to_excel(data_file_path(
        "match/pprr_jefferson_so_2020_pprr_v_post_11_06_2020.xlsx"), decision)
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Jefferson SO")


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_jefferson_so_2020.csv"))
    post = prepare_post_data()
    post_events = extract_post_events(pprr, post)
    post_events.to_csv(
        data_file_path("match/post_event_jefferson_so_2020.csv"),
        index=False)
