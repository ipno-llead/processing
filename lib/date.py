import pandas as pd


def combine_date_columns(
    df: pd.DataFrame, year_col: str, month_col: str, day_col: str
) -> pd.Series:
    """Combines date columns into a single column

    Args:
        df (pd.DataFrame):
            the frame to process
        year_col (str):
            year column
        month_col (str):
            month column
        day_col (str):
            day column

    Returns:
        the combined datetime series
    """
    dates = df[[year_col, month_col, day_col]]
    dates.columns = ["year", "month", "day"]
    return pd.to_datetime(dates, errors="coerce")


def combine_datetime_columns(
    df: pd.DataFrame, year_col: str, month_col: str, day_col: str, time_col: str
) -> pd.Series:
    """Combines datetime columns into a single column

    Args:
        df (pd.DataFrame):
            the frame to process
        year_col (str):
            year column
        month_col (str):
            month column
        day_col (str):
            day column
        time_col (str):
            time column

    Returns:
        the combined datetime series
    """
    time_frame = df[time_col].str.split(":", expand=True)
    dates = pd.concat([df[[year_col, month_col, day_col]], time_frame], axis=1)
    dates.columns = ["year", "month", "day", "hour", "minute"]
    return pd.to_datetime(dates)
