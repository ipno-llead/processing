import deba
import pandas as pd


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
    df.loc[:, "text"] = df.text.str.strip().str.replace(r"\s+", " ", regex=True)
    df = df[df.text != ""].reset_index(drop=True)
    df.loc[:, "lineno"] = df.groupby(["fileid", "pageno"]).cumcount()
    df.loc[:, "maxline"] = df.lineno.mask(
        df.duplicated(["fileid", "pageno"], keep="last")
    ).bfill()
    return df


def extract_pagetype(df: pd.DataFrame):
    pagetypes = {
        "meeting": (
            (df.region == "westwego")
            & (df.text.str.match(r"^the city of westwego municipal"))
        )
        | (
            (df.region == "east_baton_rouge")
            & (
                df.text.str.match(r"^municipal fire (and|&) police")
                | df.text.str.match(r"civil service board$")
                | df.text.str.match(r"minutes of the meeting")
                | df.text.str.match(r"council chambers")
            )
        )
        | (
            (df.region == "louisiana_state")
            & (
                df.text.str.match(r"^minutes$")
                | df.text.str.match(r"state police commission")
                | df.text.str.match(r"louisiana state police commission")
                | df.text.str.match(r"general business meeting")
                | df.text.str.match(r"minutes/actions")
            )
        )
        | (
            (df.region == "vivian")
            & (
                df.text.str.match(r"town hall meeting")
                | df.text.str.match(r"council meeting$")
                | df.text.str.match(r"^minutes of")
            )
        )
        | (
            (df.region == "mandeville")
            & (
                df.text.str.match(r"^city of mandeville$")
                | df.text.str.match(r"^minutes of meeting$")
                | df.text.str.match(r"^municipal police employees")
                | df.text.str.match(r"civil service board$")
                | df.text.str.match(r"^municipal police employees")
            )
        )
        | (
            (df.region == "kenner")
            & (
                df.text.str.match(r"^minutes of the kenner")
                | df.text.str.match(r"^meeting minutes$")
            )
        )
        | (
            (df.region == "addis")
            & (
                df.text.str.match(r"town of addis")
                | df.text.str.match(r"minutes")
                | df.text.str.match(
                    r"the regular meeting of the mayor and town council"
                )
                | df.text.str.match(r"town of addis minutes")
            )
        )
        | (
            (df.region == "orleans")
            & (
                df.text.str.match(r"^civil service")
                | df.text.str.match(r"service commission$")
                | df.text.str.match(r"regular monthly meeting")
                | df.text.str.match(r"special meeting")
            )
        )
        | (
            (df.region == "broussard")
            & (
                df.text.str.match(r"^broussard municipal fire")
                | df.text.str.match(r"fire and police civil service board$")
            )
        )
        | (
            (df.region == "carencro")
            & (
                df.text.str.match(r"^carencro municipal fire")
                | df.text.str.match(r"fire and police civil service board$")
                | df.text.str.match(r"^meeting held on")
            )
        )
        | (
            (df.region == "harahan")
            & (
                df.text.str.match(r"^harahan municipal fire")
                | df.text.str.match(r"fire & police civil service board$")
            )
        )
        | (
            (df.region == "lake_charles")
            & (
                df.text.str.match(r"board met")
                | df.text.str.match(r"^regular meeting minutes")
                | df.text.str.match(r"^lake charles municipal")
                | df.text.str.match(r"fire and police civil service board$")
            )
        )
        | ((df.region == "youngsville") & df.text.str.match(r"^minutes of the"))
        | ((df.region == "shreveport") & df.text.str.match(r"minutes"))
        | ((df.region == "iberia") & df.text.str.match(r"^minutes of"))
        | (
            (df.region == "slidell")
            & df.text.str.match(r"^board members (present|absent)")
        ),
        "hearing": (
            (df.region == "westwego")
            & (
                df.text.str.match(r"^westwego municipal fire and police")
                | df.text.str.match(r"^hearing of appeal")
            )
        )
        | ((df.region == "kenner") & df.text.str.match(r"^hearing of appeal$"))
        | ((df.region == "sulphur") & df.text.str.match(r"special meeting .+ appeal")),
        "agenda": ((df.region == "kenner") & df.text.str.match(r"^agenda$"))
        | ((df.region == "sulphur") & df.text.str.match(r"agenda"))
        | (
            (df.region == "lake_charles")
            & (df.text.str.match(r"^notice$") | df.text.str.match(r"board will meet"))
        )
        | ((df.region == "slidell") & df.text.str.match(r"^meeting agenda$")),
    }
    df.loc[:, "pagetype"] = ""
    for pt, index in pagetypes.items():
        df.loc[index, "pagetype"] = pt
    return df


def extract_contpage(df: pd.DataFrame):
    df.loc[:, "contpage"] = False
    df.loc[:, "frontpage"] = False
    df.loc[
        ((df.lineno == 0) | (df.lineno == df.maxline))
        & (
            df.text.str.match(r".*\bpage ([2-9]|1[0-9])(?: of \d+)?")
            | df.text.str.match(r"^([2-9]|1[0-9])$")
        ),
        "contpage",
    ] = True
    df.loc[
        ((df.lineno == 0) | (df.lineno == df.maxline))
        & (df.text.str.match(r".*\bpage 1(?: of \d+)?$") | df.text.str.match(r"^1$")),
        "frontpage",
    ] = True
    return df


def aggregate_feats(df: pd.DataFrame):
    grouped = df.groupby(["fileid", "pageno"])
    for feat in ["pagetype", "contpage", "frontpage"]:
        df.loc[:, feat] = grouped[feat].transform(lambda x: x.max())
    return df[
        ["fileid", "pageno", "region", "pagetype", "frontpage", "contpage"]
    ].drop_duplicates()


def minutes_features():
    df = pd.read_csv(deba.data("ocr/minutes_pdfs.csv")).pipe(only_minutes)
    return (
        df.pipe(discard_empty_pages)
        .pipe(split_lines)
        .pipe(extract_pagetype)
        .pipe(extract_contpage)
        .pipe(aggregate_feats)
        .merge(df[["fileid", "pageno", "text"]], on=["fileid", "pageno"])
    )


if __name__ == "__main__":
    df = minutes_features()
    df.to_csv(deba.data("features/minutes.csv"), index=False)
