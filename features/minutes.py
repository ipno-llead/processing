from functools import reduce
import operator

import deba
import pandas as pd
from pandas.core.groupby.generic import DataFrameGroupBy
import numpy as np


def only_minutes(df: pd.DataFrame):
    return df.loc[df.file_category == "minutes"].reset_index(drop=True)


def discard_empty_pages(df: pd.DataFrame):
    df.loc[:, "text"] = df.text.str.strip()
    return df.loc[df.text != ""].reset_index(drop=True)


def split_lines(df: pd.DataFrame):
    df.loc[:, "text"] = (
        df.text.str.lower().str.replace(r"\n\s+", "\n", regex=True).str.split("\n")
    )
    df = df.explode("text")
    df.loc[:, "text"] = (
        df.text.str.strip().str.replace(r"\s+", " ", regex=True).fillna("")
    )
    df = df[df.text != ""].reset_index(drop=True)
    df.loc[:, "lineno"] = df.groupby(["fileid", "pageno"]).cumcount()
    return df


def extract_pagetype(df: pd.DataFrame):
    pagetypes = {
        "meeting": [
            r".*\b(special )?(meeting|session) of the\b.*",
            r".+ met in special session.*",
            r"(meeting held on|minutes of) .+",
            r"(meeting |actions?/)?minutes",
            r".+ minutes",
            r"special .*\bmeeting",
            r".*\bapproved",
        ],
        "agenda": [r"(meeting )?agenda"],
        "notice": [r"notice"],
        "hearing": [r"hearing of ", r"special meeting .+ appeal"],
    }
    # for each key of pagetypes, create a column of the same name
    # with the value being line number where such pattern is detected
    for key, patterns in pagetypes.items():
        df.loc[:, key] = np.NaN
        df.loc[
            df.text.notna()
            & reduce(
                operator.or_, [df.text.str.match(pattern) for pattern in patterns]
            ),
            key,
        ] = df.lineno

    # aggregate pagetype columns to the smallest value in each page
    # in effect, the pagetype columns point to the earliest occurrence in a page
    grouped = df.groupby(["fileid", "pageno"])
    pagetype_cols = pagetypes.keys()
    for feat in pagetype_cols:
        df.loc[:, feat] = grouped[feat].transform(lambda x: x.min())

    # aggregate the new pagetype columns under a single column "pagetype"
    # selecting the pagetype with the smallest line number in each page
    df = df.set_index(["fileid", "pageno"], drop=True)
    pagetype_df = (
        df[pagetype_cols]
        .stack()
        .sort_values()
        .reset_index(drop=False)
        .drop_duplicates(["fileid", "pageno"], keep="first")
    )
    pagetype_df.columns = ["fileid", "pageno", "pagetype", "lineno"]
    pagetype_df = pagetype_df[["fileid", "pageno", "pagetype"]].set_index(
        ["fileid", "pageno"], drop=True
    )
    return df.join(pagetype_df).reset_index(drop=False).drop(columns=pagetype_cols)


def extract_docpageno(df: pd.DataFrame):
    df.loc[:, "docpageno"] = np.NaN
    for ind, pat in [
        (
            df.text.str.match(r"^(?:.*\bpage )([1-9]\d*)(?: of \d+)?$")
            & ~df.text.str.match(r".*\bon page \d+.*"),
            r"^(?:.*\bpage )([1-9]\d*)(?: of \d+)?$",
        ),
        (df.text.str.match(r"^-([1-9]\d*)-$"), r"^-([1-9]\d*)-$"),
        (df.text.str.match(r"^([1-9]\d*)$"), r"^([1-9]\d*)$"),
    ]:
        ind = ind & df.docpageno.isna()
        values = df.loc[ind, "text"].str.extract(pat, expand=False).astype(float)
        # eliminate values >= 1000 (probably captured year number)
        ind = ind & (values < 1000)
        df.loc[ind, "docpageno"] = values.loc[ind]
    df.loc[:, "docpageno"] = df.groupby(["fileid", "pageno"])["docpageno"].transform(
        lambda x: x.min()
    )
    return df


def minutes_features():
    df = pd.read_csv(deba.data("ocr/minutes_pdfs.csv")).pipe(only_minutes)
    return (
        df.pipe(discard_empty_pages)
        .pipe(split_lines)
        .pipe(extract_pagetype)
        .pipe(extract_docpageno)
        .drop(
            columns=[
                "lineno",
                "text",
                "filesha1",
                "filepath",
                "filetype",
                "file_category",
                "year",
                "month",
                "day",
            ]
        )
        .drop_duplicates()
        .merge(df[["fileid", "pageno", "text"]], on=["fileid", "pageno"])
    )


if __name__ == "__main__":
    df = minutes_features()
    df.to_csv(deba.data("features/minutes.csv"), index=False)
