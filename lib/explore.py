import pandas as pd


def show_unique(df: pd.DataFrame) -> None:
    """Print summary of all columns"""
    print("%d rows" % len(df))
    for col in df.columns:
        print("%s:" % col)
        print("    dtype: %s" % df[col].dtype)
        s = df[col].unique()
        unique_len = len(s)
        print("    unique: %d" % unique_len)
        if unique_len < 100:
            print("        %s" % s)


def print_df(df: pd.DataFrame) -> None:
    """Removes display limits and print dataframe"""
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 1000)
    print(df)


def show_columns_with_differences(df: pd.DataFrame) -> pd.DataFrame:
    """Discard all columns with the same value across all rows"""
    df = df.dropna(axis=1, how="all")
    return df.loc[:, df.apply(lambda x: ~(x == x.iloc[0]).all(), axis=0)]
