import pandas as pd


def duplicate_row(df: pd.DataFrame, idx: any, n: int = 2) -> pd.DataFrame:
    """Duplicates row at specified index

    Args:
        df (pd.DataFrame):
            the frame to process
        idx (any):
            the index at which to duplicate record
        n (int):
            how many duplicates to produce. Defaults to 2

    Returns:
        the processed frame
    """
    if n == 1:
        return df
    assert isinstance(df.index, pd.RangeIndex)
    row = df.iloc[idx]
    upper_rows = df.iloc[:idx]
    lower_rows = df.iloc[idx+1:]
    df = pd.concat(
        [upper_rows, pd.DataFrame.from_records([row] * n), lower_rows])
    df.index = pd.RangeIndex(start=0, stop=df.shape[0])
    return df
