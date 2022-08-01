from functools import reduce
import hashlib
import operator
import re

import deba
import pandas as pd
import numpy as np


def only_minutes(df: pd.DataFrame):
    return df.loc[df.file_category == "minutes"].reset_index(drop=True)


def split_lines(df: pd.DataFrame):
    # discard empty pages
    df.loc[:, "text"] = df.text.str.strip()
    df = df.loc[df.text != ""].reset_index(drop=True)

    # split text into lines
    df.loc[:, "text"] = df.text.str.replace(r"\n\s+", "\n", regex=True).str.split("\n")
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
            r"^.*\b(special )?(meeting|session) of the\b.*$",
            r"^.+ met in special session.*$",
            r"^(meeting held on|minutes of) .+$",
            r".*\bheld a special meeting\b.*",
            r".*\bactions?/minutes\b.*",
            r".*\bminutes/actions\b.*",
            r"^(meeting )?minutes$",
            r"^minutes to be approved$",
            r"^.+ minutes$",
            r"^civil service .*\bmeeting$",
            r"^special .*\bmeeting( in)?$",
            r"^(1\. )?roll call\b.*",
        ],
        "agenda": [r"^(meeting )?agenda$"],
        "notice": [r"^(public )?notice$", r"^notice of .+"],
        "hearing": [
            r"^hearing of .+$",
            r"^special meeting .+ appeal$",
            r".*\bwould like to file a formal appeal\b.*",
        ],
    }
    # for each key of pagetypes, create a column of the same name
    # with the value being line number where such pattern is detected
    for key, patterns in pagetypes.items():
        df.loc[:, key] = np.NaN
        df.loc[
            df.text.notna()
            & reduce(
                operator.or_,
                [
                    df.text.str.match(pattern, flags=re.IGNORECASE)
                    for pattern in patterns
                ],
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


def extract_docpageno(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "docpageno"] = np.NaN
    for ind, pat in [
        (
            df.text.str.match(
                r"^(?:.*\bpage )([1-9]\d*)(?: of \d+)?$", flags=re.IGNORECASE
            )
            & ~df.text.str.match(r".*\bon page \d+.*", flags=re.IGNORECASE),
            r"^(?:.*\bpage )([1-9]\d*)(?: of \d+)?$",
        ),
        (df.text.str.match(r"^-([1-9]\d*)-$", flags=re.IGNORECASE), r"^-([1-9]\d*)-$"),
        (df.text.str.match(r"^([1-9]\d*)$", flags=re.IGNORECASE), r"^([1-9]\d*)$"),
    ]:
        ind = ind & df.docpageno.isna()
        values = (
            df.loc[ind, "text"]
            .str.extract(pat, expand=False, flags=re.IGNORECASE)
            .astype(float)
        )
        # eliminate values >= 1000 (probably captured year number)
        ind = ind & (values < 1000)
        df.loc[ind, "docpageno"] = values.loc[ind]
    df.loc[:, "docpageno"] = df.groupby(["fileid", "pageno"])["docpageno"].transform(
        lambda x: x.min()
    )
    return df


def generate_docid(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["fileid", "pageno"])

    def gen_docid(row: pd.Series):
        hash = hashlib.sha1(usedforsecurity=False)
        hash.update(".".join([row.fileid, "%04d" % row.pageno]).encode("utf8"))
        return hash.hexdigest()[:8]

    df.loc[
        (df.docpageno == 1.0) | (df.docpageno.isna() & df.pagetype.notna()), "docid"
    ] = df.apply(gen_docid, axis=1)
    df.loc[:, "docid"] = df.groupby("fileid").docid.fillna(method="ffill")
    return df


if __name__ == "__main__":
    df = (
        pd.read_csv(deba.data("ocr/minutes_pdfs.csv"))
        .pipe(only_minutes)
        .pipe(split_lines)
        .pipe(extract_pagetype)
        .pipe(extract_docpageno)
        .pipe(generate_docid)
    )

    df.to_csv(deba.data("features/minutes_docid.csv"), index=False)
