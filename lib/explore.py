import pandas as pd
import numpy as np


def show_unique(df):
    print("%d rows" % len(df))
    for col in df.columns:
        print("%s:" % col)
        print("    dtype: %s" % df[col].dtype)
        s = df[col].unique()
        unique_len = len(s)
        print("    unique: %d" % unique_len)
        if unique_len < 100:
            print("        %s" % s)


def print_df(df):
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    print(df)


def show_columns_with_differences(df):
    df = df.dropna(axis=1, how='all')
    return df.loc[:, df.apply(lambda x: ~(x == x.iloc[0]).all(), axis=0)]
