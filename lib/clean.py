import sys
import re
import json
from typing import List, Tuple
import pandas as pd
import numpy as np
import datetime

from lib.date import combine_date_columns, combine_datetime_columns
from lib.standardize import standardize_from_lookup_table


def full_year_str(yr: str) -> str:
    if len(yr) == 4:
        return yr
    if yr[0] in ["1", "2", "0"]:
        return "20" + yr
    return "19" + yr


def swap_month_day(month: str, day: str) -> Tuple[str, str]:
    if int(month) > 12 and int(day) <= 12:
        return day, month
    return month, day


mdy_date_pattern_1 = re.compile(r"^\d{1,2}/\d{1,2}/\d{2}$")
mdy_date_pattern_2 = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
mdy_date_pattern_3 = re.compile(r"^\d{1,2}-\d{1,2}-\d{2}$")
dmy_date_pattern = re.compile(r"^\d{1,2}-\w{3}-\d{2}$")
year_pattern = re.compile(r"^(19|20)\d{2}$")
year_month_pattern = re.compile(r"^(19|20)\d{4}$")
month_day_pattern = re.compile(r"^[A-Z][a-z]{2}-\d{1,2}$")


def clean_date(val: str) -> tuple[str, str, str]:
    """Try parsing date with a few known patterns

    Args:
        val (str):
            the date string to parse

    Returns:
        a tuple of (year, month, day)

    Raises:
        ValueError:
            date string format is unknown
    """
    if val == "" or pd.isnull(val):
        return "", "", ""

    m = mdy_date_pattern_2.match(val)
    if m is not None:
        [month, day, year] = val.split("/")
        month, day = swap_month_day(month, day)
        return year, month.lstrip("0"), day.lstrip("0")

    m = mdy_date_pattern_1.match(val)
    if m is not None:
        [month, day, year] = val.split("/")
        year = full_year_str(year)
        month, day = swap_month_day(month, day)
        return year, month.lstrip("0"), day.lstrip("0")

    m = mdy_date_pattern_3.match(val)
    if m is not None:
        [month, day, year] = val.split("-")
        year = full_year_str(year)
        month, day = swap_month_day(month, day)
        return year, month.lstrip("0"), day.lstrip("0")

    m = dmy_date_pattern.match(val)
    if m is not None:
        [day, month, year] = val.split("-")
        month = str(datetime.datetime.strptime(month, "%b").month)
        year = full_year_str(year)
        return year, month, day

    m = year_month_pattern.match(val)
    if m is not None:
        return val[:4], val[4:].lstrip("0"), ""

    m = year_pattern.match(val)
    if m is not None:
        return val, "", ""

    m = month_day_pattern.match(val)
    if m is not None:
        dt = datetime.datetime.strptime(val, "%b-%d")
        return "", str(dt.month).zfill(2), str(dt.day).zfill(2)

    raise ValueError('unknown date format "%s"' % val)


def clean_dates(df: pd.DataFrame, cols: list[str], expand: bool = True) -> pd.DataFrame:
    """Parses date columns using a few known patterns.

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            the date columns. Each column must end with "_date"
        expand (bool):
            whether the result should be expanded to _year, _month and
            _day columns. Defaults to True.

    Returns:
        the updated frame
    """
    for col in cols:
        assert col.endswith("_date")
        dates = pd.DataFrame.from_records(
            df[col]
            .str.strip()
            .str.replace(r"//", r"/", regex=False)
            .str.replace(r"'", "", regex=False)
            .map(clean_date)
        )
        if expand:
            prefix = col[:-5]
            dates.columns = [prefix + "_year", prefix + "_month", prefix + "_day"]
            df = pd.concat([df, dates], axis=1)
        else:
            df.loc[:, col] = combine_date_columns(dates, 0, 1, 2)
    if expand:
        df = df.drop(columns=cols)
    return df


datetime_pattern_1 = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{1,2}$")


def clean_datetime(val) -> tuple[str, str, str, str]:
    """Parses datetime string using known patterns

    Args:
        val (str):
            the datetime string to parse

    Returns:
        a tuple of year, month, day and time string

    Raises:
        ValueError:
            datetime string format is unknown
    """
    if val == "" or pd.isnull(val):
        return "", "", "", ""
    m = datetime_pattern_1.match(val)
    if m is not None:
        [date, time] = re.split(r"\s+", val)
        [hour, minute] = time.split(":")
        year, month, day = clean_date(date)
        return year, month, day, "%s:%s" % (hour.zfill(2), minute.zfill(2))
    raise ValueError('unknown datetime format "%s"' % val)


def clean_datetimes(
    df: pd.DataFrame, cols: list[str], expand: bool = True
) -> pd.DataFrame:
    """Parses datetime columns using known patterns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            datetime column names. Each column name must end with "_datetime"
        expand (bool):
            whether result should be expanded into 4 columns: _year, _month,
            _day and _time. Defaults to True

    Returns:
        the updated frame
    """
    for col in cols:
        assert col.endswith("_datetime")
        dates = pd.DataFrame.from_records(df[col].str.strip().map(clean_datetime))
        if expand:
            prefix = col[:-9]
            dates.columns = [
                prefix + "_year",
                prefix + "_month",
                prefix + "_day",
                prefix + "_time",
            ]
            df = pd.concat([df, dates], axis=1)
        else:
            df.loc[:, col] = combine_datetime_columns(dates, 0, 1, 2, 3)
    if expand:
        df = df.drop(columns=cols)
    return df


def parse_dates_with_known_format(
    df: pd.DataFrame, cols: list[str], format: str
) -> pd.DataFrame:
    """Parses dates using strptime format and expands into _year, _month, _day columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            list of date columns. Each column name must end with "_date"
        format (str):
            strptime format string.

    Returns:
        the updated frame
    """
    for col in cols:
        assert col.endswith("_date")
        dates = pd.DataFrame.from_records(
            pd.to_datetime(df[col].astype(str), format=format).map(
                lambda x: ("", "", "")
                if pd.isnull(x)
                else (str(x.year), str(x.month), str(x.day))
            )
        )
        prefix = col[:-5]
        dates.columns = [prefix + "_year", prefix + "_month", prefix + "_day"]
        df = pd.concat([df, dates], axis=1)
    df = df.drop(columns=cols)
    return df


def clean_sexes(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans and standardizes sex columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            sex columns

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.strip()
            .str.lower()
            .str.replace(r"^m$", "male", regex=True)
            .str.replace(r"^f$", "female", regex=True)
            .str.replace(r"^unknown.*", "", regex=True)
            .str.replace(r"^null$", "", regex=True)
        )
        df = standardize_from_lookup_table(
            df, col, [["male"], ["female", "femaale", "famale", "femal"]]
        )
    return df


def clean_races(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans and standardize race columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            race columns

    Returns:
        the updated frame
    """
    for col in cols:
        # replacing one-letter race because they are too short
        # to use with standardize_from_lookup_table safely
        df.loc[:, col] = (
            df[col]
            .str.strip()
            .str.lower()
            .str.replace(r"^w$", "white", regex=True)
            .str.replace(r"^h$", "hispanic", regex=True)
            .str.replace(r"^b$", "black", regex=True)
            .str.replace(r"\bislande\b", "islander", regex=True)
        )
        df = standardize_from_lookup_table(
            df,
            col,
            [
                [
                    "black",
                    "african american",
                    "black / african american",
                    "black or african american",
                    "black/african american",
                ],
                ["white"],
                ["hispanic", "latino"],
                [
                    "native american",
                    "american indian",
                    "american indian or alaskan native",
                    "amer. ind.",
                    "american ind",
                    "american indian/alaska native",
                    "american indian/alaskan native",
                ],
                [
                    "asian / pacific islander",
                    "asian/pacific islander",
                    "asian",
                    "native hawaiian or other pacific islander",
                    "islander",
                    "asian/pacif",
                ],
                ["mixed", "two or more races", "multi-racial", "2 or more races"],
                ["indian"],
                ["middle eastern"],
            ],
        )
    return df


def clean_employment_status(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans and standardize employment status columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            employment status columns

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.strip()
            .str.lower()
            .str.replace(r"^i$", "inactive", regex=True)
            .str.replace(r"^a$", "active", regex=True)
        )
    return df


def clean_salary(series: pd.Series) -> pd.Series:
    """Cleans and standardize salary series

    Args:
        series (pd.Series):
            the series to process

    Returns:
        the updated series
    """
    return (
        series.str.strip()
        .str.lower()
        .str.replace("k", "000", regex=False)
        .str.replace(r"[^\d\.]", "", regex=True)
        .str.replace(r"^$", "0", regex=True)
        .astype("float64")
    )


def clean_salaries(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans and standardize salary columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            salary columns

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = clean_salary(df[col])
    return df


def clean_name(series: pd.Series) -> pd.Series:
    """Cleans and standardize name series

    Args:
        series (pd.Series):
            the series to process

    Returns:
        the updated series
    """
    return (
        series.str.strip()
        .str.replace(r"[^\w-]+", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"\s*-\s*", "-", regex=True)
        .str.lower()
        .str.strip()
        .fillna("")
        .str.strip("-")
    )


def clean_rank(series: pd.Series) -> pd.Series:
    """Cleans and standardize name series

    Args:
        series (pd.Series):
            the series to process

    Returns:
        the updated series
    """
    return (
        series.str.strip()
        .str.replace("s/ofc", "senior officer", regex=False)
        .str.replace("sgt", "sergeant", regex=False)
        .str.replace("ofc", "officer", regex=False)
        .str.replace("lt", "lieutenant", regex=False)
        .str.replace("cpt", "captain", regex=False)
        .str.replace("a/supt", "superintendent", regex=False)
        .str.replace(r"\(|\)", " ", regex=True)
        .str.lower()
        .str.strip()
        .fillna("")
        .str.strip("-")
    )


def clean_ranks(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans and standardize description columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            descriptive columns such as rank_desc and department_desc

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = clean_rank(df[col])
    return df


def names_to_title_case(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Converts name columns to title case

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            name columns

    Returns:
        the updated frame
    """
    cols_set = set(df.columns)
    for col in cols:
        if col not in cols_set:
            continue
        df.loc[:, col] = (
            df[col]
            .str.title()
            .str.replace(
                r" I(i|ii|v|x)$", lambda m: " I" + m.group(1).upper(), regex=True
            )
            .str.replace(
                r" V(i|ii|iii)$", lambda m: " V" + m.group(1).upper(), regex=True
            )
        )
    return df


def clean_names(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans and standardize name columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            name columns

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = clean_name(df[col])
    return df


name_pattern_1 = re.compile(r"^(\w{2,}) (\w\.) (\w{2,}.+)$")
name_pattern_2 = re.compile(r"^([-\w\']+), (\w{2,})$")
name_pattern_3 = re.compile(r'^(\w{2,}) ("\w+") ([-\w\']+)$')
name_pattern_4 = re.compile(r"^(\w{2,}) ([-\w\']+ (?:i|ii|iii|iv|v|jr|sr)\W?)$")
name_pattern_5 = re.compile(r"^([\w-]{2,}) (\w+) ([-\w\']+)$")
name_pattern_6 = re.compile(r"^([\w-]{2,}) ([-\w\']+)$")
name_pattern_7 = re.compile(r"^\w+$")


def split_names(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Split name column using known patterns

    Args:
        df (pd.DataFrame):
            the frame to process
        col (str):
            name column

    Returns:
        the updated frame with new columns: 'first_name',
        'middle_name' and 'last_name'
    """

    def split_name(val):
        if pd.isnull(val) or not val:
            return "", "", ""
        m = name_pattern_1.match(val)
        if m is not None:
            first_name = m.group(1)
            middle_name = m.group(2)
            last_name = m.group(3)
            return first_name, middle_name, last_name
        m = name_pattern_2.match(val)
        if m is not None:
            first_name = m.group(2)
            last_name = m.group(1)
            return first_name, "", last_name
        m = name_pattern_3.match(val)
        if m is not None:
            first_name = m.group(1)
            last_name = m.group(3)
            return first_name, "", last_name
        m = name_pattern_4.match(val)
        if m is not None:
            first_name = m.group(1)
            last_name = m.group(2)
            return first_name, "", last_name
        m = name_pattern_5.match(val)
        if m is not None:
            first_name = m.group(1)
            middle_name = m.group(2)
            last_name = m.group(3)
            return first_name, middle_name, last_name
        m = name_pattern_6.match(val)
        if m is not None:
            first_name = m.group(1)
            last_name = m.group(2)
            return first_name, "", last_name
        m = name_pattern_7.match(val)
        if m is not None:
            return "", "", val
        raise ValueError("unrecognized name format %s" % json.dumps(val))

    df = df.reset_index(drop=True)
    names = pd.DataFrame.from_records(
        df[col]
        .fillna("")
        .str.strip()
        .str.replace(r" +", " ", regex=True)
        .str.lower()
        .map(split_name)
        .to_list()
    )
    names.columns = ["first_name", "middle_name", "last_name"]
    return pd.concat([df, names], axis=1)


def standardize_desc(series: pd.Series) -> pd.Series:
    """Standardizes description series such as rank and department

    Args:
        series (pd.Series):
            the series to process

    Returns:
        the updated series
    """
    return series.str.strip().str.lower().fillna("")


def standardize_desc_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans and standardize description columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            descriptive columns such as rank_desc and department_desc

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = standardize_desc(df[col])
    return df


def float_to_int_str(
    df: pd.DataFrame, cols: list[str], cast_as_str: bool = False
) -> pd.DataFrame:
    """Turns float values in column into strings without trailing ".0"

    Data loaded with pd.read_csv tends to turn integer columns into
    float columns even if one value is missing. This
    reverses that effect by converting everything to a string and striping
    trailing ".0"s.

    Examples:
        [1973.0, np.nan, "abc"] => ["1973", "", "abc"]

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            columns to convert
        cast_as_str (bool):
            cast column to string even if no processing was needed.
            Defaults to False.

    Returns:
        the updated frame
    """
    cols_set = set(df.columns)
    for col in cols:
        if col not in cols_set:
            continue
        if df[col].dtype == np.float64:
            df.loc[:, col] = (
                df[col]
                .fillna(0)
                .astype("int64")
                .astype(str)
                .str.replace(r"^0$", "", regex=True)
            )
        elif df[col].dtype == np.object:
            idx = df[col].map(lambda v: type(v) == float)
            df.loc[idx, col] = (
                df.loc[idx, col]
                .fillna(0)
                .astype("int64")
                .astype(str)
                .str.replace(r"^0$", "", regex=True)
            )
            if cast_as_str:
                df.loc[~idx, col] = df.loc[~idx, col].astype(str)
        elif cast_as_str:
            df.loc[:, col] = df[col].astype(str)
    return df


def remove_future_dates(
    df: pd.DataFrame, max_date: str, prefixes: List[str]
) -> pd.DataFrame:
    """Sets to empty any date that is greater than max_date

    Args:
        df (pd.DataFrame):
            the dataframe to process
        max_date (str):
            maximum date in YYYY-MM-DD format
        prefixes (list of str):
            prefixes of date columns (such as those produced by clean_dates) to process.
            i.e. prefixes=['hire'] means 'hire_year', 'hire_month' and 'hire_day' will
            be consulted.

    Returns:
        the updated frame
    """
    md = datetime.datetime.strptime(max_date, "%Y-%m-%d")
    for prefix in prefixes:
        cols = [prefix + "_year", prefix + "_month", prefix + "_day"]
        dates = df[cols].replace({"": np.NaN}).astype(float).astype("Int64")
        for idx, _ in df.loc[
            (dates.iloc[:, 0] > md.year)
            | (
                (dates.iloc[:, 0] == md.year)
                & (
                    (dates.iloc[:, 1].notna() & (dates.iloc[:, 1] > md.month))
                    | (
                        dates.iloc[:, 2].notna()
                        & (dates.iloc[:, 1] == md.month)
                        & (dates.iloc[:, 2] > md.day)
                    )
                )
            )
        ].iterrows():
            for col in cols:
                df.loc[idx, col] = ""
    return df


def strip_birth_day_and_month(series: pd.Series) -> pd.Series:
    """Strips birth day from birth date column containing birth day, birth month, and birth year

    Args:
        series (pd.Series):
            the series to process

    Returns:
        the updated series
    """
    return (
        series.str.strip()
        .str.lower()
        .str.replace(r"(\w{2})\/(\w{2})\/(\w{4})", r"\3", regex=True)
    )


def strip_birth_date(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Strips birth day from birth date column containing birth day, birth month, and birth year

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            birth date columns

    Returns:
        the updated frame
    """
    for col in cols:
        df.loc[:, col] = strip_birth_day_and_month(df[col])
    return df


def canonicalize_officers(
    df: pd.DataFrame,
    clusters: list[tuple],
    uid_column: str = "uid",
    first_name_column: str = "first_name",
    last_name_column: str = "last_name",
) -> pd.DataFrame:
    for cluster in clusters:
        uid, first_name, last_name, = None, "", ""
        for idx in cluster:
            row = df.loc[df[uid_column] == idx].squeeze()
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            if uid is None or (
                len(row[first_name_column]) > len(first_name)
                or (
                    len(row[first_name_column]) == len(first_name)
                    and (
                        len(row[last_name_column]) > len(last_name)
                    )
                )
            ):
                uid = idx
                first_name = row[first_name_column]
                last_name = row[last_name_column]

        df.loc[df[uid_column].isin(cluster), uid_column] = uid
        df.loc[df[uid_column].isin(cluster), first_name_column] = first_name
        df.loc[df[uid_column].isin(cluster), last_name_column] = last_name

    return df


def convert_dates(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """converts dates to numerical format

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            date column

    Returns:
        the updated frame
    """
    for col in cols:
        # replacing one-letter race because they are too short
        # to use with standardize_from_lookup_table safely
        df.loc[:, col] = df[col].str.strip().str.lower()
        df = standardize_from_lookup_table(
            df,
            col,
            [
                [
                    "1",
                    "january",
                ],
                ["2", "february"],
                ["3", "march"],
                [
                    "4",
                    "april",
                ],
                [
                    "5",
                    "may",
                ],
                ["6", "june"],
                ["7", "july"],
                ["8", "august"],
                ["9", "september"],
                ["10", "october"],
                ["11", "november"],
                ["12", "december"],
            ],
        )
    return df


def clean_ranks(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Cleans and standardize rank_ columns

    Args:
        df (pd.DataFrame):
            the frame to process
        cols (list of str):
            rank columns

    Returns:
        the updated frame
    """
    for col in cols:
        # replacing one-letter race because they are too short
        # to use with standardize_from_lookup_table safely
        df.loc[:, col] = (
            df[col].str.strip().str.lower().str.replace(r"\.", "", regex=True)
        )
        df = standardize_from_lookup_table(
            df,
            col,
            [
                [
                    "deputy",
                    "dty",
                ],
                ["captain", "capt"],
                ["sergeant", "sgt"],
                [
                    "lieutenant",
                    "lt",
                ],
            ],
        )
    return df
