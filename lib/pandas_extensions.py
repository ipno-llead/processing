import time

import pandas as pd


@pd.api.extensions.register_dataframe_accessor("timed")
class TimedAccessor:
    """Times Pandas pipe method.

    Call this accessor's pipe method (df.timed.pipe) instead of regular pipe method
    in order to time function execution and print duration.

    Examples:
        # just import this module for this accessor to become available
        import lib.pandas_extensions

        ...

        # time my_func and print duration
        df.timed.pipe(my_func, *args, **kwargs)
    """

    _obj: pd.DataFrame

    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def pipe(self, func, *args, **kwargs):
        def timed_func(df, *args, **kwargs):
            start = time.perf_counter()
            result = func(df, *args, **kwargs)
            print(
                "timed.pipe(%s) (%.2fs)" % (func.__name__, time.perf_counter() - start)
            )
            return result

        return self._obj.pipe(timed_func, *args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._obj, item)
