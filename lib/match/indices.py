class NoopIndex(object):
    """
    This pair every record with every other record. It's like not using an index.
    """

    def __init__(self):
        pass

    def index(self, dfa, dfb):
        self._dfa = dfa
        self._dfb = dfb

    def __iter__(self):
        self._done = False
        return self

    def __next__(self):
        if self._done:
            raise StopIteration()
        self._done = True
        return self._dfa, self._dfb


class ColumnsIndex(object):
    """
    This pair records with the same value in specified columns.
    """

    def __init__(self, cols):
        self._cols = cols

    def index(self, dfa, dfb):
        self._dfa = dfa
        self._dfb = dfb

        set_a = set(
            dfa[self._cols].drop_duplicates().to_records(index=False).tolist())
        set_b = set(
            dfb[self._cols].drop_duplicates().to_records(index=False).tolist())
        self._keys = list(set_a.intersection(set_b))
        self._len = len(self._keys)

    def __iter__(self):
        self._cur = 0
        return self

    def _bool_index(self, df, key):
        result = None
        for ind, val in enumerate(key):
            if result is None:
                result = df[self._cols[ind]] == val
            else:
                result = result & (df[self._cols[ind]] == val)
        return result

    def __next__(self):
        if self._cur >= self._len:
            raise StopIteration()
        key = self._keys[self._cur]
        rows_a = self._dfa.loc[self._bool_index(self._dfa, key)]
        rows_b = self._dfb.loc[self._bool_index(self._dfb, key)]
        self._cur += 1
        if len(rows_a.shape) == 1:
            rows_a = rows_a.to_frame().transpose()
        if len(rows_b.shape) == 1:
            rows_b = rows_b.to_frame().transpose()
        return rows_a, rows_b
