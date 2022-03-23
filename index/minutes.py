import re
import os
import pathlib
import json
import typing

import yaml
import deba
import pandas as pd


def open_dvc_files(dir_name: str) -> typing.List[typing.Dict]:
    with open(deba.data("%s.dvc" % dir_name), "r") as f:
        raw_dir_dvc = yaml.load(f)
    dir_hash = raw_dir_dvc["outs"][0]["md5"]
    dir_path = (
        pathlib.Path(os.path.realpath(__file__)).parent.parent
        / ".dvc/%s/%s"
        % (dir_hash[:2], dir_hash[2:])
    )
    with open(dir_path, "r") as f:
        dvc_files = json.load(f)
    ensure_no_file_duplications(dir_name, dvc_files)
    return dvc_files


def ensure_no_file_duplications(
    dir_name: str, dvc_files: typing.List[typing.Dict]
) -> None:
    files = dict()
    for obj in dvc_files:
        if obj["md5"] in files:
            raise ValueError(
                "file %s is redundant, found another file with the same md5: %s"
                % (
                    deba.data("%s/%s" % (dir_name, obj["relpath"])),
                    deba.data("%s/%s" % (dir_name, files[obj["md5"]])),
                )
            )
        files[obj["md5"]] = obj["relpath"]


months = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]
mdy_num_re = re.compile(r"(?:^|[^0-9])([0-9]{1,2}) ([0-9]{1,2}) ([0-9]{2,4})")
mdy_string_re = re.compile(
    r"(%s|%s) ([0-9]{1,2}) ([0-9]{2,4})"
    % ("|".join(months), "|".join(v[:3] for v in months))
)
ymd_num_re = re.compile(r"([0-9]{4})([0-9]{2})([0-9]{2})")
ymd_num2_re = re.compile(r"([0-9]{4}) ([0-9]{1,2}) ([0-9]{1,2})")


def parse_date(df: pd.DataFrame) -> pd.DataFrame:
    filenames = df.filename.str.lower().str.replace(r"[^a-z0-9 ]", " ").str.strip()

    def extract_date(v: str) -> typing.Dict:
        m = mdy_num_re.match(v)
        if m is not None:
            month, day, year = m[1], m[2], m[3]
        for pattern in [mdy_num_re, mdy_string_re, ymd_num_re, ymd_num2_re]:
            m = pattern.match(v)
            if m is not None:
                break


def set_filetype(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.filename.str.endswith(".pdf"), "filetype"] = "pdf"
    df.loc[df.filename.str.endswith(".doc"), "filetype"] = "doc"
    return df


def split_filename(df: pd.DataFrame) -> pd.DataFrame:
    filenames = df.filename.str.split("/")
    df.loc[:, "region"] = filenames.map(lambda v: v[0])
    df.loc[:, "fn"] = filenames.map(lambda v: v[-1])
    return df


def set_fileid(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "fileid"] = df.md5.map(lambda v: v[:7])
    return df


def set_file_category(df: pd.DataFrame) -> pd.DataFrame:
    column = "file_category"
    df.loc[:, column] = "other"
    df.loc[df.filename.str.match(r"receipt|invoice|transa"), column] = "receipt"
    df.loc[df.region == "broussard", column] = "minutes"
    df.loc[
        (df.region == "lake_charles") & df.fn.str.match(r"records", case=False), column
    ] = "minutes"
    df.loc[df.fn.str.match(r"carencro-municipal", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r"sulphur civil service", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r"minutes", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r"meeting min", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r"fpcsb_mprr", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r"mpecsbm", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r"transcript", case=False), column] = "transcript"
    df.loc[df.fn.str.match(r"v\.? city of", case=False), column] = "transcript"
    df.loc[df.fn.str.match(r"city of [^\s]+ v", case=False), column] = "transcript"
    df.loc[df.fn.str.match(r"cancelled", case=False), column] = "cancelled"
    df.loc[df.region == "addis", column] = "minutes"
    df.loc[df.fn.str.match(r"agenda", case=False), column] = "agenda"
    df.loc[df.fn.str.match(r"memo", case=False), column] = "memo"
    df.loc[
        (df.region == "orleans") & df.fn.str.match(r"^[A-Z][a-z]+,", case=False), column
    ] = "transcript"
    df.loc[
        (df.region == "orleans") & df.fn.str.match(r"dates", case=False), column
    ] = "dates"
    df.loc[df.region == "orleans", column] = "memo"
    return df


if __name__ == "__main__":
    df = (
        pd.DataFrame.from_records(open_dvc_files("raw_minutes"))
        .rename({"relpath": "filename"})
        .pipe(set_filetype)
        .pipe(split_filename)
        .pipe(set_fileid)
        .pipe(set_file_category)
    )
