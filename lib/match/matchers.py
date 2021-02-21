import pandas as pd
import numpy as np
import math
from bisect import bisect_left, bisect
from operator import itemgetter


class ThresholdMatcher(object):
    """
    Match records between 2 dataframes using similarity score.
    Final match results can be retrieved with a similarity threshold.
    """

    def __init__(self, dfa, dfb, index, fields):
        if dfa.index.duplicated().any() or dfb.index.duplicated().any():
            raise ValueError(
                "Dataframe index contains duplicates. Both frames need to have index free of duplicates."
            )
        if set(dfa.columns) != set(dfb.columns):
            raise ValueError(
                "Dataframe columns are not equal."
            )

        self._fields = fields
        self._index = index
        self._dfa = dfa
        self._dfb = dfb

        self._score_all_pairs()

    def _score_pair(self, ser_a, ser_b):
        """
        Calculate similarity value (0 <= sim_val <= 1) for a pair of records
        """
        sim_vec = dict()
        for k, scls in self._fields.items():
            if pd.isnull(ser_a[k]) or pd.isnull(ser_b[k]):
                sim_vec[k] = 0
            else:
                sim_vec[k] = scls.sim(ser_a[k], ser_b[k])
        return math.sqrt(
            sum(v * v for v in sim_vec.values()) / len(self._fields))

    def _score_all_pairs(self):
        """
        Calculate similarity value for all pairs of records
        """
        self._index.index(self._dfa, self._dfb)
        pairs = []
        for rows_a, rows_b in self._index:
            for idx_a, ser_a in rows_a.iterrows():
                for idx_b, ser_b in rows_b.iterrows():
                    sim = self._score_pair(ser_a, ser_b)
                    pairs.append((sim, idx_a, idx_b))
        self._pairs = sorted(pairs, key=itemgetter(0))
        self._keys = [t[0] for t in self._pairs]

    def get_index_pairs_within_thresholds(self, lower_bound=0.7, upper_bound=1):
        """
        Returns index pairs with similarity score within specified thresholds
        """
        return [t[1:] for t in self._pairs[
            bisect_left(self._keys, lower_bound):
            bisect(self._keys, upper_bound)]]

    def get_sample_pairs(self, sample_counts=5, lower_bound=0.7, upper_bound=1, step=0.05):
        """
        Returns samples of record pairs for each range of similarity score
        """
        sample_records = []
        ranges = list(np.arange(upper_bound, lower_bound, -step)
                      ) + [lower_bound]
        for i, upper_val in enumerate(ranges[:-1]):
            lower_val = ranges[i+1]
            score_range = "%.2f-%.2f" % (upper_val, lower_val)
            pairs = self._pairs[
                bisect(self._keys, lower_val):
                bisect(self._keys, upper_val)
            ][:sample_counts]
            pairs.reverse()
            for pair_idx, pair in enumerate(pairs):
                sim_score, idx_a, idx_b = pair
                sample_records.append(dict([
                    ("score_range", score_range), ("pair_idx", pair_idx),
                    ("sim_score", sim_score), ("row_key", idx_a)
                ] + list(self._dfa.loc[idx_a].to_dict().items())))
                sample_records.append(dict([
                    ("score_range", score_range), ("pair_idx", pair_idx),
                    ("sim_score", sim_score), ("row_key", idx_b)
                ] + list(self._dfb.loc[idx_b, self._dfa.columns].to_dict().items())))
        return pd.DataFrame.from_records(
            sample_records,
            index=["score_range", "pair_idx", "sim_score", "row_key"])

    def get_all_pairs(self, lower_bound=0.7, upper_bound=1):
        records = []
        pairs = reversed(self._pairs[
            bisect_left(self._keys, lower_bound):
            bisect(self._keys, upper_bound)])
        for pair_idx, pair in enumerate(pairs):
            sim_score, idx_a, idx_b = pair
            records.append(dict([
                ("pair_idx", pair_idx), ("sim_score", sim_score), ("row_key", idx_a)
            ] + list(self._dfa.loc[idx_a].to_dict().items())))
            records.append(dict([
                ("pair_idx", pair_idx), ("sim_score", sim_score), ("row_key", idx_b)
            ] + list(self._dfb.loc[idx_b, self._dfa.columns].to_dict().items())))
        return pd.DataFrame.from_records(
            records,
            index=["pair_idx", "sim_score", "row_key"])

    def save_pairs_to_excel(self, name, sample_counts=5, lower_bound=0.7, upper_bound=1, step=0.05):
        samples = self.get_sample_pairs(
            sample_counts, lower_bound, upper_bound, step)
        pairs = self.get_all_pairs(lower_bound, upper_bound)
        with pd.ExcelWriter(name) as writer:
            samples.to_excel(writer, sheet_name='Sample records')
            pairs.to_excel(writer, sheet_name='All pairs')
