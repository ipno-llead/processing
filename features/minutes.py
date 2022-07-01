import deba
import pandas as pd


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
    df.loc[:, "maxline"] = df.lineno.mask(
        df.duplicated(["fileid", "pageno"], keep="last")
    ).bfill()
    return df


def extract_pagetype(df: pd.DataFrame):
    pagetypes = {
        "meeting": (
            (df.region == "westwego")
            & (df.text.str.match(r"^The City of Westwego Municipal"))
        )
        | (
            (df.region == "east_baton_rouge")
            & (
                df.text.str.match(r"^MUNICIPAL FIRE (AND|&) POLICE")
                | df.text.str.match(r"CIVIL SERVICE BOARD$")
                | df.text.str.match(r"MINUTES OF THE MEETING")
                | df.text.str.match(r"COUNCIL CHAMBERS")
            )
        )
        | (
            (df.region == "louisiana_state")
            & (
                df.text.str.match(r"^MINUTES$")
                | df.text.str.match(r"STATE POLICE COMMISSION")
                | df.text.str.match(r"LOUISIANA STATE POLICE COMMISSION")
                | df.text.str.match(r"GENERAL BUSINESS MEETING")
                | df.text.str.match(r"MINUTES/ACTIONS")
            )
        )
        | (
            (df.region == "vivian")
            & (
                df.text.str.match(r"TOWN HALL MEETING")
                | df.text.str.match(r"COUNCIL MEETING$")
                | df.text.str.match(r"^MINUTES OF")
            )
        )
        | (
            (df.region == "mandeville")
            & (
                df.text.str.match(r"^City of Mandeville$")
                | df.text.str.match(r"^Minutes of Meeting$")
                | df.text.str.match(r"^Municipal Police Employees")
                | df.text.str.match(r"Civil Service Board$")
                | df.text.str.match(r"^MUNICIPAL POLICE EMPLOYEES")
            )
        )
        | (
            (df.region == "kenner")
            & (
                df.text.str.match(r"^MINUTES OF THE KENNER")
                | df.text.str.match(r"^MEETING MINUTES$")
            )
        )
        | (
            (df.region == "addis")
            & (
                df.text.str.match(r"TOWN OF ADDIS")
                | df.text.str.match(r"MINUTES")
                | df.text.str.match(
                    r"The regular meeting of the Mayor and Town Council"
                )
                | df.text.str.match(r"Town of Addis Minutes")
            )
        )
        | (
            (df.region == "orleans")
            & (
                df.text.str.match(r"^CIVIL SERVICE")
                | df.text.str.match(r"SERVICE COMMISSION$")
                | df.text.str.match(r"REGULAR MONTHLY MEETING")
                | df.text.str.match(r"SPECIAL MEETING")
            )
        )
        | (
            (df.region == "broussard")
            & (
                df.text.str.match(r"^BROUSSARD MUNICIPAL FIRE")
                | df.text.str.match(r"FIRE AND POLICE CIVIL SERVICE BOARD$")
            )
        )
        | (
            (df.region == "carencro")
            & (
                df.text.str.match(r"^Carencro Municipal Fire")
                | df.text.str.match(r"Fire and Police Civil Service Board$")
                | df.text.str.match(r"^Meeting held on")
            )
        )
        | (
            (df.region == "harahan")
            & (
                df.text.str.match(r"^Harahan Municipal Fire")
                | df.text.str.match(r"Fire & Police Civil Service Board$")
            )
        )
        | (
            (df.region == "lake_charles")
            & (
                df.text.str.match(r"Board met")
                | df.text.str.match(r"^REGULAR MEETING MINUTES")
                | df.text.str.match(r"^LAKE CHARLES MUNICIPAL")
                | df.text.str.match(r"FIRE AND POLICE CIVIL SERVICE BOARD$")
            )
        )
        | ((df.region == "youngsville") & df.text.str.match(r"^MINUTES OF THE"))
        | ((df.region == "shreveport") & df.text.str.match(r"MINUTES"))
        | ((df.region == "iberia") & df.text.str.match(r"^MINUTES OF"))
        | (
            (df.region == "slidell")
            & df.text.str.match(r"^Board Members (Present|Absent)")
        ),
        "hearing": (
            (df.region == "westwego")
            & (
                df.text.str.match(r"^WESTWEGO MUNICIPAL FIRE AND POLICE")
                | df.text.str.match(r"^Hearing of Appeal")
            )
        )
        | ((df.region == "kenner") & df.text.str.match(r"^HEARING OF APPEAL$"))
        | ((df.region == "sulphur") & df.text.str.match(r"Special Meeting .+ Appeal")),
        "agenda": ((df.region == "kenner") & df.text.str.match(r"^AGENDA$"))
        | ((df.region == "sulphur") & df.text.str.match(r"AGENDA"))
        | (
            (df.region == "lake_charles")
            & (df.text.str.match(r"^Notice$") | df.text.str.match(r"Board will meet"))
        )
        | ((df.region == "slidell") & df.text.str.match(r"^MEETING AGENDA$")),
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
            df.text.str.match(r"page ([2-9]|1[0-9])")
            | df.text.str.match(r"^([2-9]|1[0-9])$")
        ),
        "contpage",
    ] = True
    df.loc[
        ((df.lineno == 0) | (df.lineno == df.maxline))
        & (df.text.str.match(r"page 1$") | df.text.str.match(r"^1$")),
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
    return (
        pd.read_csv(deba.data("ocr/minutes_pdfs.csv"))
        .pipe(only_minutes)
        .pipe(discard_empty_pages)
        .pipe(split_lines)
        .pipe(extract_pagetype)
        .pipe(extract_contpage)
        .pipe(aggregate_feats)
    )


if __name__ == "__main__":
    df = minutes_features()
    df.to_csv(deba.data("features/minutes.csv"), index=False)
