import pandas as pd


def detect_unique_columns_subset(df):
    """
    Detect the subset of columns which is unique for each index label
    """
    unique_cols = df.columns
    rows = dict()
    for idx, row in df.iterrows():
        if idx not in rows:
            rows[idx] = row[unique_cols]
        else:
            unique_cols = [
                col for col in unique_cols
                if (pd.isnull(row[col]) and pd.isnull(rows[idx][col])) or row[col] == rows[idx][col]
            ]
    return unique_cols
