from Levenshtein import ratio, jaro_winkler
from unidecode import unidecode


class StringSimilarity(object):
    def __init__(self):
        pass

    def sim(self, a, b):
        return ratio(unidecode(a), unidecode(b))


class JaroWinklerSimilarity(object):
    """
    Similar to StringSimilarity but give extra weight to common prefix
    """

    def __init__(self, prefix_weight=0.1):
        self._prefix_weight = prefix_weight

    def sim(self, a, b):
        return jaro_winkler(unidecode(a), unidecode(b), self._prefix_weight)


class DateSimilarity(object):
    def __init__(self, days_max_diff=30):
        self._days_max_diff = days_max_diff

    def sim(self, a, b):
        d = a - b
        if b > a:
            d = b - a
        if d.days < self._days_max_diff:
            return 1 - d.days / self._days_max_diff
        if a.year == b.year and a.month == b.day and a.day == b.month:
            return 0.5
        if a.year == b.year and a.day == b.day:
            return ratio(a.strftime("%Y%m%d"), b.strftime("%Y%m%d"))
        return 0
