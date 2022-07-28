from functools import reduce
import hashlib
import operator
import re

import deba
import pandas as pd
import numpy as np

from lib.clean import float_to_int_str


def only_minutes(df: pd.DataFrame):
    return df.loc[df.file_category == "minutes"].reset_index(drop=True)


def discard_empty_pages(df: pd.DataFrame):
    df.loc[:, "text"] = df.text.str.strip()
    return df.loc[df.text != ""].reset_index(drop=True)


def split_lines(df: pd.DataFrame):
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


def extract_east_baton_rouge_hrg_text(df: pd.DataFrame) -> pd.DataFrame:
    for pat in [
        r"^([A-Za-z,’\. ]+) VS?\. (?:BRPD|BATON ROUGE POLICE DEPARTMENT)",
        r"^(?:\d+\. )?(?:CONTINUATION OF )?APPEALS? (?:HEARINGS? )?(?:FOR|-|ON) (?:OFFICER )?([A-Za-z,’\. ]+)(?: \(.+)?$",
        r"^([A-Za-z,’\. ]+) APPEALS? HEARINGS? \(Resumed\)",
        r"^REQUEST BY ([A-Za-z,’\. ]+), BRPD, (?:TO APPEAL|APPEALING).*",
    ]:
        extracted = df.text.str.extract(pat, expand=False)
        df.loc[
            (df.region == "east_baton_rouge") & extracted.notna(), "accused"
        ] = extracted.str.strip()
    df.loc[
        (df.region == "east_baton_rouge") & df.text.str.contains("APPEAL"),
        "appeal_header",
    ] = df.text
    return df


def extract_hrg_text(df: pd.DataFrame) -> pd.DataFrame:
    return df.pipe(extract_east_baton_rouge_hrg_text)


def minutes_features():
    df = pd.read_csv(deba.data("ocr/minutes_pdfs.csv")).pipe(only_minutes)
    return (
        df.pipe(discard_empty_pages)
        .pipe(split_lines)
        # extract features from each line
        .pipe(extract_pagetype)
        .pipe(extract_docpageno)
        .pipe(generate_docid)
        .pipe(extract_hrg_text)
        # aggregate lines back into pages
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
        .pipe(float_to_int_str, ["docpageno"])
    )


if __name__ == "__main__":
    df = minutes_features()
    df.to_csv(deba.data("features/minutes.csv"), index=False)
