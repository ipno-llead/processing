import json
import operator
import re
import io
from typing import Callable, Dict, Union

import numpy as np
import pandas as pd


CondFunc = Callable[[pd.Series, str, int, int], bool]


class Condition(object):
    def __init__(self, fn: CondFunc):
        self._fn = fn

    def __and__(self, other: "Condition"):
        def combined(*args, **kwargs):
            if not self._fn(*args, **kwargs):
                return False
            return other._fn(*args, **kwargs)

        return Condition(combined)

    def __or__(self, other: "Condition"):
        def combined(*args, **kwargs):
            if self._fn(*args, **kwargs):
                return True
            return other._fn(*args, **kwargs)

        return Condition(combined)

    def __xor__(self, other: "Condition"):
        def combined(*args, **kwargs):
            if self._fn(*args, **kwargs):
                return not other._fn(*args, **kwargs)
            return other._fn(*args, **kwargs)

        return Condition(combined)

    def __invert__(self):
        def combined(*args, **kwargs):
            return not self._fn(*args, **kwargs)

        return Condition(combined)

    def __call__(self, *args, **kwargs) -> bool:
        return self._fn(*args, **kwargs)


def _value(row: pd.Series, text: str, lineno: int, maxline: int, accessor):
    if isinstance(accessor, Column):
        return accessor.of(row)
    elif isinstance(accessor, TextAccessor):
        return text
    elif isinstance(accessor, LinenoAccessor):
        return accessor.relative_to(lineno)
    elif isinstance(accessor, MaxlineAccessor):
        return accessor.relative_to(maxline)
    return accessor


def _compare(op, a, b):
    def cond(row: pd.Series, text: str, lineno: int, maxline: int) -> bool:
        return op(
            _value(row, text, lineno, maxline, a),
            _value(row, text, lineno, maxline, b),
        )

    return Condition(cond)


class Column(object):
    def __init__(self, name: str):
        self._name = name

    def of(self, row: pd.Series):
        return row[self._name]

    def __eq__(self, other):
        return _compare(operator.eq, self, other)

    def __ne__(self, other):
        return _compare(operator.ne, self, other)

    def __lt__(self, other):
        return _compare(operator.lt, self, other)

    def __gt__(self, other):
        return _compare(operator.gt, self, other)

    def __ge__(self, other):
        return _compare(operator.ge, self, other)

    def __le__(self, other):
        return _compare(operator.le, self, other)

    def match(self, pattern: str, ignore_case: bool = False, flags: int = 0):
        if ignore_case:
            flags = flags | re.IGNORECASE
        compiled_re = re.compile(pattern, flags=flags)
        return Condition(
            lambda sr, text, lineno, maxline: compiled_re.match(self.of(sr)) is not None
        )

    def isna(self):
        return Condition(lambda sr, text, lineno, maxline: pd.isna(self.of(sr)))

    def notna(self):
        return Condition(lambda sr, text, lineno, maxline: pd.notna(self.of(sr)))


class _Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(_Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ColumnsAccessor(metaclass=_Singleton):
    def __getattr__(self, name):
        return Column(name)


class _Scalar:
    def __eq__(self, other):
        return _compare(operator.eq, self, other)

    def __ne__(self, other):
        return _compare(operator.ne, self, other)

    def __lt__(self, other):
        return _compare(operator.lt, self, other)

    def __gt__(self, other):
        return _compare(operator.gt, self, other)

    def __ge__(self, other):
        return _compare(operator.ge, self, other)

    def __le__(self, other):
        return _compare(operator.le, self, other)


class TextAccessor(_Scalar):
    def match(self, pattern: str, ignore_case: bool = False, flags: int = 0):
        if ignore_case:
            flags = flags | re.IGNORECASE
        compiled_re = re.compile(pattern, flags=flags)
        return Condition(
            lambda sr, text, lineno, maxline: compiled_re.match(text) is not None
        )


class _Int(_Scalar):
    def __init__(self, ops=[]):
        super().__init__()
        self._ops = ops

    def relative_to(self, val: int):
        for op, other in self._ops:
            val = op(val, other)
        return val

    def __add__(self, other):
        return type(self)(self._ops + [(operator.add, other)])

    def __sub__(self, other):
        return type(self)(self._ops + [(operator.sub, other)])

    def __mul__(self, other):
        return type(self)(self._ops + [(operator.mul, other)])

    def __truediv__(self, other):
        return type(self)(self._ops + [(operator.truediv, other)])

    def __mod__(self, other):
        return type(self)(self._ops + [(operator.mod, other)])

    def __floordiv__(self, other):
        return type(self)(self._ops + [(operator.floordiv, other)])


class LinenoAccessor(_Int):
    pass


class MaxlineAccessor(_Int):
    pass


def _read_lines(txt: str, combine_contiguous_lines: bool = False):
    spaces_re = re.compile(r"\s+")
    with io.StringIO(txt.strip()) as f:
        lines = []
        lineno = 0
        for line in f:
            line = spaces_re.sub(line.strip(), " ")
            if combine_contiguous_lines:
                if line == "" and len(lines) > 0:
                    yield lineno, " ".join(lines)
                    lineno += 1
                    lines = []
                if line != "":
                    lines.append(line)
            elif line != "":
                yield lineno, line
                lineno += 1


def extract_features(
    df: pd.DataFrame,
    text_column: str = "text",
    combine_contiguous_lines: bool = False,
    na_val=np.NaN,
    get_instructions: Union[
        Callable[
            [ColumnsAccessor, TextAccessor, LinenoAccessor, MaxlineAccessor],
            Dict[str, Dict[str, Condition]],
        ],
        None,
    ] = None,
) -> pd.DataFrame:
    """Split text into lines and extracts new features using a set of instructions

    Example usage:

    >>> extract_features(
    ...     df,
    ...     text_column="text",
    ...     get_instructions=lambda columns, text, lineno, maxline: {
    ...         "pagetype": {
    ...             "meeting": (columns.region == "addis") & text.match(r"^MEETING"),
    ...             "hearing": (columns.region == "carencro") & text.match(r"^Hearing "),
    ...         },
    ...         "continuation_page": {
    ...             True: (lineno == 0) & text.match(r'page ([2-9]|1[0-9])')
    ...             False: (lineno == 0) & text.match(r'page 1$')
    ...         }
    ...     })

    get_instructions callback must return a set of instructions of this form:

    {
        "FEATURE_1": {
            [VALUE_1]: [CONDITION_1],
            [VALUE_2]: [CONDITION_2],
            ...
        },
        "FEATURE_2": {
            ...
        }
        ...
    }

    If a feature is an existing column in the frame, the column will be updated with new value,
    otherwise a new column will be added.

    A value could be any scalar: str, int, bool are all fine.

    Conditions are created out of accessors. Examples of condition:

    >>> columns.pageno >= 10
    >>> text.match(r"SOME REGEX")
    >>> lineno == 0
    >>> lineno == maxline - 1

    Accessors are merely stateful objects, not Pandas series. The following accessors are given
    as arguments to get_instructions callback:
        columns:
            gives access to columns of the current row e.g. columns.my_column. Supported
            conditional operators are:

            >>> columns.COLUMN == SCALAR_OR_ACCESSOR
            >>> columns.COLUMN != SCALAR_OR_ACCESSOR
            >>> columns.COLUMN > SCALAR_OR_ACCESSOR
            >>> columns.COLUMN < SCALAR_OR_ACCESSOR
            >>> columns.COLUMN >= SCALAR_OR_ACCESSOR
            >>> columns.COLUMN <= SCALAR_OR_ACCESSOR
            >>> columns.COLUMN.match(r"REGEX_PATTERN")
            >>> columns.COLUMN.isna()
            >>> columns.COLUMN.notna()

        text:
            gives access to text of the current line. Supported conditional operators are:

            >>> text == STRING_OR_ACCESSOR
            >>> text != STRING_OR_ACCESSOR
            >>> text > STRING_OR_ACCESSOR
            >>> text < STRING_OR_ACCESSOR
            >>> text >= STRING_OR_ACCESSOR
            >>> text <= STRING_OR_ACCESSOR
            >>> text.match(r"REGEX_PATTERN")

        lineno:
            gives access to the current line number. Supported conditional operators are:

            >>> lineno == NUMBER_SCALAR_OR_ACCESSOR
            >>> lineno != NUMBER_SCALAR_OR_ACCESSOR
            >>> lineno > NUMBER_SCALAR_OR_ACCESSOR
            >>> lineno < NUMBER_SCALAR_OR_ACCESSOR
            >>> lineno >= NUMBER_SCALAR_OR_ACCESSOR
            >>> lineno <= NUMBER_SCALAR_OR_ACCESSOR

            The following operators create accessors with modified value:

            >>> lineno + NUMBER_SCALAR
            >>> lineno - NUMBER_SCALAR
            >>> lineno * NUMBER_SCALAR
            >>> lineno / NUMBER_SCALAR
            >>> lineno % NUMBER_SCALAR
            >>> lineno // NUMBER_SCALAR

        maxline:
            gives access to the max line number of the current row. It supports the same
            operators as lineno.

    Conditions can be combined using bitwise operators: &, |, ^, and ~, which have the same
    effects as bitwise operators' on Pandas index. A condition is not a Pandas index though,
    and does other index operators. Example of combined conditions:

    >>> (lineno == 0) & text.match(r"REGEX_PATTERN")
    >>> (columns.region == "carencro") | (columns.region == "addis")

    Args:
        df (pd.DataFrame):
            the frame with texts to extract features from
        text_column (str):
            name of the text column. Defaults to "text"
        combine_contiguous_lines (bool):
            if set to True, lines that are not separated by empty lines are combined into a
            single line before processing. This is useful when the text to match is scattered
            on multiple lines.
        na_val (any):
            empty value used to populate a new column. Defaults to np.NaN
        get_instructions (Callable[
            [ColumnsAccessor, TextAccessor, LinenoAccessor, MaxlineAccessor],
            Dict[str, Dict[str, Condition]],
        ]):
            instructions getter callback. Described above.

    Returns:
        the modified frame
    """
    if get_instructions is None:
        raise ValueError("get_instructions kwarg must be given")
    instructions = get_instructions(
        ColumnsAccessor(), TextAccessor(), LinenoAccessor(), MaxlineAccessor()
    )
    for feat_label in instructions:
        if feat_label not in df.columns:
            df.loc[:, feat_label] = na_val
    for row_label, row in df.iterrows():
        feats_set = set()
        lines = list(_read_lines(row[text_column], combine_contiguous_lines))
        numlines = len(lines)
        for lineno, text in lines:
            for feat_label, conds in instructions.items():
                if feat_label in feats_set:
                    continue
                for value, cond in conds.items():
                    if cond(row, text, lineno, numlines - 1):
                        df.loc[row_label, feat_label] = value
                        feats_set.add(feat_label)
                        break
    return df
