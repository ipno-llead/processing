import operator
import re
from functools import reduce

import deba
import pandas as pd

REGEXES_MAP = {
    "westwego": {
        "mtg": [r"^The City of Westwego Municipal"],
        "hrg": [r"^WESTWEGO MUNICIPAL FIRE AND POLICE", r"^Hearing of Appeal"],
    },
    "east_baton_rouge": {
        "mtg": [
            r"^MUNICIPAL FIRE (AND|&) POLICE",
            r"CIVIL SERVICE BOARD$",
            r"MINUTES OF THE MEETING",
            r"COUNCIL CHAMBERS",
        ]
    },
    "louisiana_state": {
        "mtg": [
            r"^MINUTES$",
            r"STATE POLICE COMMISSION",
            r"LOUISIANA STATE POLICE COMMISSION",
            r"GENERAL BUSINESS MEETING",
            r"MINUTES/ACTIONS",
        ]
    },
    "vivian": {"mtg": [r"TOWN HALL MEETING", r"COUNCIL MEETING$", r"^MINUTES OF"]},
    "mandeville": {
        "mtg": [
            r"^City of Mandeville$",
            r"^Minutes of Meeting$",
            r"^Municipal Police Employees",
            r"Civil Service Board$",
            r"^MUNICIPAL POLICE EMPLOYEES",
        ]
    },
    "kenner": {
        "mtg": [r"^MINUTES OF THE KENNER", r"^MEETING MINUTES$"],
        "hrg": [r"^HEARING OF APPEAL$"],
        "agd": [r"^AGENDA$"],
    },
    "addis": {
        "mtg": [
            r"TOWN OF ADDIS",
            r"MINUTES",
            r"The regular meeting of the Mayor and Town Council",
            r"Town of Addis Minutes",
        ]
    },
    "orleans": {
        "mtg": [
            r"^CIVIL SERVICE",
            r"SERVICE COMMISSION$",
            r"REGULAR MONTHLY MEETING",
            r"SPECIAL MEETING",
        ]
    },
    "sulphur": {"agd": [r"AGENDA"], "hrg": [r"Special Meeting .+ Appeal"]},
    "broussard": {
        "mtg": [r"^BROUSSARD MUNICIPAL FIRE", r"FIRE AND POLICE CIVIL SERVICE BOARD$"]
    },
    "carencro": {
        "mtg": [
            r"^Carencro Municipal Fire",
            r"Fire and Police Civil Service Board$",
            r"^Meeting held on",
        ]
    },
    "harahan": {
        "mtg": [r"^Harahan Municipal Fire", r"Fire & Police Civil Service Board$"]
    },
    "lake_charles": {
        "agd": [r"^Notice$", r"Board will meet"],
        "mtg": [
            r"Board met",
            r"^REGULAR MEETING MINUTES",
            r"^LAKE CHARLES MUNICIPAL",
            r"FIRE AND POLICE CIVIL SERVICE BOARD$",
        ],
    },
    "youngsville": {"mtg": [r"^MINUTES OF THE"]},
    "shreveport": {"mtg": [r"MINUTES"]},
    "iberia": {"mtg": [r"^MINUTES OF"]},
    "slidell": {
        "agd": [r"^MEETING AGENDA$"],
        "mtg": [r"^Board Members (Present|Absent)"],
    },
}


def only_minutes(df: pd.DataFrame):
    return df.loc[df.file_category == "minutes"].reset_index(drop=True)


def discard_empty_pages(df: pd.DataFrame):
    df.loc[:, "text"] = df.text.str.strip()
    return df.loc[df.text != ""].reset_index(drop=True)


def split_lines(df: pd.DataFrame):
    df.loc[:, "text"] = df.text.str.replace(r"\n\s+", "\n", regex=True).str.split("\n")
    df = df.explode("text")
    df.loc[:, "text"] = df.text.str.strip().str.replace(r"\s+", " ", regex=True)
    df = df[df.text != ""].reset_index(drop=True)
    df.loc[:, "lineno"] = df.groupby(["fileid", "pageno"]).cumcount()
    return df


def extract_page_features(lines: pd.DataFrame):
    page_fts = lines[["fileid", "pageno", "lineno", "text"]].set_index(
        ["fileid", "pageno", "lineno"], drop=False
    )
    page_fts.loc[:, "hd"] = page_fts.lineno <= 3
    linenos = (
        lines[["fileid", "pageno", "lineno"]]
        .groupby(["fileid", "pageno"])
        .max()
        .join(page_fts.lineno.to_frame(), how="right", lsuffix="_max")
    )
    page_fts.loc[:, "ft"] = linenos.lineno == linenos.lineno_max

    re_map = {
        "frontpage": [re.compile(s) for s in [r"page 1$", r"^1$"]],
        "contpage": [
            re.compile(s) for s in [r"page ([2-9]|1[0-9])", r"^([2-9]|1[0-9])$"]
        ],
    }

    def text_match_one_of(row: pd.DataFrame, patterns):
        if pd.isna(row.text):
            return False
        return reduce(
            operator.or_,
            [pattern.match(row.text) is not None for pattern in patterns],
            False,
        )

    for feat, patterns in re_map.items():
        page_fts.loc[:, feat] = page_fts.apply(
            text_match_one_of, axis=1, args=(patterns,)
        )

    page_fts = (
        page_fts.reset_index(drop=True)[["fileid", "pageno"] + list(re_map.keys())]
        .groupby(["fileid", "pageno"])
        .max()
    )

    return page_fts


def extract_doctype_feats(lines: pd.DataFrame):
    feats = lines.loc[
        lines.lineno <= 8, ["region", "fileid", "pageno", "lineno", "text"]
    ].set_index(["fileid", "pageno", "lineno"])

    def match(group: pd.DataFrame):
        region = group.region[0]
        if region not in REGEXES_MAP:
            return group
        for feat, patterns in REGEXES_MAP[region].items():
            group.loc[:, feat] = reduce(
                operator.or_,
                [group.text.str.match(pattern) for pattern in patterns],
                False,
            )
        return group

    feats = feats.groupby("region").apply(match)
    feat_cols = ["mtg", "hrg", "agd"]
    feats.loc[:, feat_cols] = feats[feat_cols].fillna(False)
    return feats.reset_index(drop=False).groupby(["fileid", "pageno"])[feat_cols].max()


def get_header_features(df: pd.DataFrame):
    lines = split_lines(df)
    doctype_feats = extract_doctype_feats(lines)
    page_feats = extract_page_features(lines)

    return (
        df.set_index(["fileid", "pageno"])
        .join(doctype_feats)
        .join(page_feats)
        .reset_index()
    )


def extract_features():
    return (
        pd.read_csv(deba.data("ocr/minutes_pdfs.csv"))
        .pipe(only_minutes)
        .pipe(discard_empty_pages)
        .pipe(get_header_features)
    )


if __name__ == "__main__":
    df = extract_features()
    df.to_csv(deba.data("features/minutes.csv"), index=False)
