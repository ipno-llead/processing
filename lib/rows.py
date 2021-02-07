import pandas as pd


def duplicate_row(df, idx, n=2):
    assert isinstance(df.index, pd.RangeIndex)
    row = df.iloc[idx]
    upper_rows = df.iloc[:idx]
    lower_rows = df.iloc[idx+1:]
    df = pd.concat(
        [upper_rows, pd.DataFrame.from_records([row] * n), lower_rows])
    df.index = pd.RangeIndex(start=0, stop=df.shape[0])
    return df
