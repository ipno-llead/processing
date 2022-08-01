from datetime import datetime, date
import os
from functools import reduce
import hashlib
import operator
import re

import deba
import pandas as pd
import numpy as np

from lib.transform import first_valid_value


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
        (df.text.str.match(r".*\bpg\. ([1-9]\d*)\b$"), r".*\bpg\. ([1-9]\d*)\b$"),
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


def extract_date(df: pd.DataFrame) -> pd.DataFrame:
    """Extracts date from the first 4 lines and the last 4 lines of each page"""
    full_months = [date(1, x, 1).strftime("%B").lower() for x in range(1, 13)]
    df = df.set_index(["fileid", "pageno", "lineno"]).sort_index()

    df.loc[:, "parsed_date"] = np.NaN

    def extract_full_date(sr: pd.Series) -> pd.Series:
        return sr.str.extract(
            pat,
            expand=False,
            flags=re.IGNORECASE,
        )

    def parse_date(x):
        if pd.isna(x):
            return np.NaN
        try:
            v = parse_func(x)
            if v.year > 2100:
                return np.NaN
            return v
        except ValueError:
            return np.NaN

    for pat, parse_func in [
        (
            r".*\b((?:%s) \d{1,2},? \d{4})\b.*" % "|".join(full_months),
            lambda x: datetime.strptime(x.replace(",", ""), "%B %d %Y"),
        ),
        (
            r".*\b(\d{1,2}/\d{1,2}/\d{4})\b.*",
            lambda x: datetime.strptime(x, "%m/%d/%Y"),
        ),
        (r".*\b(\d{4}-\d{2}-\d{2})\b.*", lambda x: datetime.strptime(x, "%Y-%m-%d")),
    ]:
        gb = df.loc[df.parsed_date.isna()].groupby(["fileid", "pageno"])
        df.loc[df.parsed_date.isna(), "parsed_date"] = (
            gb.head(4)
            .text.transform(extract_full_date)
            .combine_first(gb.tail(4).text.transform(extract_full_date))
            .map(parse_date)
        )
    return df.reset_index()


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


def region_to_agency(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "agency"] = df.region.replace(
        {
            "youngsville": "Youngsville PD",
            "shreveport": "Shreveport PD",
            "mandeville": "Mandeville PD",
            "broussard": "Broussard PD",
            "greenwood": "Greenwood PD",
            "carencro": "Carencro PD",
            "louisiana_state": "Louisiana State PD",
            "slidell": "Slidell PD",
            "sulphur": "Sulphur PD",
            "bossier": "Bossier SO",
            "monroe": "Monroe PD",
            "iberia": "New Iberia PD",
            "kenner": "Kenner PD",
            "westmonroe": "West Monroe PD",
            "lake_charles": "Lake Charles PD",
            "addis": "Addis PD",
            "east_baton_rouge": "Baton Rouge PD",
            "westwego": "Westwego PD",
            "vivian": "Vivian PD",
            "harahan": "Harahan PD",
        }
    )
    return df.drop(columns=["region"])


def print_samples(df: pd.DataFrame):
    import vscodeSpotCheck
    import pathlib

    metadata = pd.read_csv(deba.data("meta/minutes_files.csv"))
    df = (
        df.set_index(["fileid", "pageno"])
        .sort_index()
        .groupby(["fileid", "pageno"])
        .apply(
            lambda x: pd.Series(
                {
                    "text": "\n".join(x.text),
                    "parsed_date": first_valid_value(x.parsed_date),
                    "docpageno": first_valid_value(x.docpageno),
                },
                index=["text", "parsed_date", "docpageno"],
            )
        )
        .reset_index()
    )
    vscodeSpotCheck.print_samples(
        df.loc[df.docpageno.isna()],
        resolve_source_path=lambda row: pathlib.Path(__file__).parent.parent
        / "data/raw_minutes"
        / metadata.loc[metadata.fileid == row.fileid, "filepath"].iloc[0],
        resolve_pageno=lambda row: row.pageno,
    )


if __name__ == "__main__":
    deba.set_root(os.path.dirname(os.path.dirname(__file__)))
    df = (
        pd.read_csv(deba.data("ocr/minutes_pdfs.csv"))
        .pipe(only_minutes)
        .pipe(split_lines)
        .pipe(extract_pagetype)
        .pipe(extract_docpageno)
        .pipe(extract_date)
        .pipe(generate_docid)
        .pipe(region_to_agency)
    )

    print_samples(df)

    df.to_csv(deba.data("features/minutes_docid.csv"), index=False)
