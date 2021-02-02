import math
from enum import Enum


class MatchStatus(Enum):
    NO = 1
    MAYBE = 2
    YES = 3


class ThresholdClassifier(object):
    def __init__(self, fields, thresholds=[1, 0.5]):
        self._fields = fields
        self._thresholds = thresholds

    def classify(self, rec_a, rec_b):
        sim_vec = dict()
        for k, scls in self._fields.items():
            sim_vec[k] = scls.sim(rec_a[k], rec_b[k])
        sim = math.sqrt(
            sum(v * v for v in sim_vec.values()) / len(self._fields))
        if sim >= self._thresholds[0]:
            return MatchStatus.YES
        if len(self._thresholds) > 1 and sim >= self._thresholds[1]:
            return MatchStatus.MAYBE
        return MatchStatus.NO
