import datetime
import os
import re
from typing import Any, Callable, Dict, List, Set, Tuple, Union

import deba
import pandas as pd
import numpy as np


def extract_text(df: pd.DataFrame, columns: Dict[str, List[str]]) -> pd.DataFrame:
    for column, regex_patterns in columns.items():
        for pat in regex_patterns:
            extracted = df.text.str.extract(pat, expand=False)
            df.loc[extracted.notna(), column] = extracted.str.strip()
    return df


def match_text(
    df: pd.DataFrame, columns: Dict[str, List[str]], text_column="text"
) -> pd.DataFrame:
    for column, regex_patterns in columns.items():
        for pat in regex_patterns:
            df.loc[df[text_column].str.match(pat), column] = True
    return df


def mapped_apply(
    df: pd.DataFrame,
    groupby: Union[str, List[str]],
    group_name_to_func: Dict[str, Callable[[pd.DataFrame], Any]],
) -> pd.DataFrame:
    """Applies each group with corresponding function based on group name"""
    return (
        df.groupby(groupby)
        .filter(lambda x: x.name in group_name_to_func)
        .groupby(groupby)
        .apply(lambda x: group_name_to_func[x.name](x))
    )


def extract_east_baton_rouge_hearing_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.pipe(
        extract_text,
        {
            "accused": [
                r"^([A-Za-z,’\. ]+) VS?\. (?:BRPD|BATON ROUGE POLICE DEPARTMENT)",
                r"^(?:\d+\. )?(?:CONTINUATION OF )?APPEALS? (?:HEARINGS? )?(?:FOR|-|ON) (?:OFFICER )?([A-Za-z,’\. ]+)(?: \(.+)?$",
                r"^([A-Za-z,’\. ]+) APPEALS? HEARINGS? \(Resumed\)",
                r"^REQUEST BY ([A-Za-z,’\. ]+), BRPD, (?:TO APPEAL|APPEALING).*",
            ]
        },
    )
    df.loc[:, "new_accused"] = df.accused.notna().astype(int)
    df.loc[:, "accused"] = df.accused.str.replace(
        r" is continued", "", regex=True, flags=re.IGNORECASE
    ).map(lambda x: np.NaN if pd.isna(x) else (x,))

    df.loc[:, "hrg_no"] = (
        df.groupby("docid")
        .new_accused.cumsum()
        .map(lambda x: np.NaN if x == 0.0 else x - 1)
    )
    gb = df.groupby(["docid", "hrg_no"])
    accused = gb.accused.first()
    hrg_text = gb.apply(
        lambda x: pd.Series(
            {
                "hrg_text": "\n".join(x.text),
                "start_pageno": x.pageno.min(),
                "end_pageno": x.pageno.max(),
            },
            index=["hrg_text", "start_pageno", "end_pageno"],
        )
    )
    return hrg_text.join(pd.DataFrame(accused, columns=["accused"]))


def extract_kenner_hearing_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.pipe(
        extract_text,
        {
            "kenner_agenda_item": [r"^AGENDA ITEM #(\d+)$"],
        },
    )
    df.loc[:, "kenner_agenda_item"] = df.groupby("docid").kenner_agenda_item.transform(
        "ffill"
    )
    df.loc[:, "allcap"] = df.text.str.match(r"^[A-Z]{2,}.+")
    agenda_item_title = df.groupby(["docid", "kenner_agenda_item"]).apply(
        lambda x: x.loc[
            x.allcap.expanding().apply(lambda x: x.all()).astype(bool), "text"
        ].to_list()
    )
    hoa_pat = re.compile(r".*HEARING OF APPEAL.*")
    agenda_hearing = agenda_item_title.map(lambda l: any(hoa_pat.match(s) for s in l))
    hrg_no = (
        agenda_item_title.loc[agenda_hearing]
        .groupby("docid")
        .apply(lambda x: x.groupby("kenner_agenda_item").ngroup())
    )
    accused_pat = re.compile(
        r"(?:(?:HEARING|HEARING OF APPEAL|DISCUSSION|JOINT HEARING)[ \W]+)?"
        r"(?:FIREFIGHTER|FIRE CAPTAIN|FIRE DRIVER|POLICE OFFICERS?|"
        r"Correctional Peace Officer|POLICE COMMUNICATIONS OFFICER|"
        r"ASSISTANT FIRE CHIEF|FIRE COMMUNICATIONS OFFICER|SERGEANT|CAPTAIN|LIEUTENANT) ([-A-Za-z,\.& ]+)"
    )

    def to_names_tuple(l: List[str]) -> Tuple[str]:
        return np.NaN if len(l) == 0 else tuple(l)

    accused = (
        agenda_item_title.loc[agenda_hearing]
        .map(
            lambda l: [
                m.group(1) for m in [accused_pat.match(s) for s in l] if m is not None
            ]
        )
        .map(to_names_tuple)
    )
    months = [datetime.date(1, x, 1).strftime("%B").upper() for x in range(1, 13)]
    discard_pat = re.compile(
        r"(?:AGENDA|DISCUSSION|HEARING|TERMINATION|REQUEST|DISMISS|EDUCATION|PERSONNEL|%s).*"
        % "|".join(months)
    )
    accused.loc[accused.isna()] = (
        agenda_item_title.loc[agenda_hearing & accused.isna()]
        .map(lambda l: [s for s in l if discard_pat.match(s) is None])
        .map(to_names_tuple)
    )

    hrg_no = hrg_no.loc[hrg_no.notna() & accused.notna()]
    df = df.set_index(["docid", "kenner_agenda_item"]).join(
        pd.DataFrame(hrg_no, columns=["hrg_no"]), how="left"
    )
    hrg_text = df.groupby(["docid", "hrg_no"]).apply(
        lambda x: pd.Series(
            {
                "hrg_text": "\n".join(x.text),
                "start_pageno": x.pageno.min(),
                "end_pageno": x.pageno.max(),
            },
            index=["hrg_text", "start_pageno", "end_pageno"],
        )
    )
    accused = (
        df.join(pd.DataFrame(accused, columns=["accused"]), how="left")
        .groupby(["docid", "hrg_no"])
        .accused.first()
    )
    return hrg_text.join(pd.DataFrame(accused, columns=["accused"]))


def extract_hearing_text(df: pd.DataFrame) -> pd.DataFrame:
    """Extracts `hrg_text` and `accused` columns

    The frame is split by `region` and passed to functions as mapped.
    Each function must return a dataframe with
        columns=['hrg_text', 'start_pageno', 'end_pageno', 'accused'] and
        index=['docid', 'hrg_no']
    (this shape will be enforced automatically)
    """
    df = df.pipe(
        mapped_apply,
        "region",
        {
            "east_baton_rouge": extract_east_baton_rouge_hearing_text,
            "kenner": extract_kenner_hearing_text,
        },
    )
    df.loc[:, "accused"] = df.accused.map(
        lambda l: np.NaN
        if pd.isna(l)
        else tuple(
            v for s in l for v in s.lower().replace(r" and ", " & ").split(" & ")
        )
    )
    return df.reset_index()


def print_samples(df: pd.DataFrame):
    import vscodeSpotCheck
    import pathlib

    metadata = pd.read_csv(deba.data("meta/minutes_files.csv"))
    vscodeSpotCheck.print_samples(
        df,
        resolve_source_path=lambda row: pathlib.Path(__file__).parent.parent
        / "data/raw_minutes"
        / metadata.loc[metadata.fileid == row.fileid, "filepath"].iloc[0],
        resolve_pageno=lambda row: row.start_pageno,
    )


if __name__ == "__main__":
    deba.set_root(os.path.dirname(os.path.dirname(__file__)))
    df = pd.read_csv(deba.data("features/minutes_docid.csv"))
    hrg_text_df = (
        extract_hearing_text(df)
        .set_index("docid", drop=True)
        .join(df.set_index("docid")[["fileid"]])
    )

    print_samples(hrg_text_df)

    hrg_text_df.to_csv(deba.data("features/minutes_hearing_text.csv"), index=False)
