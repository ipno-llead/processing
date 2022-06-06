import datetime
import re
import typing

import deba
import pandas as pd

from lib.clean import full_year_str, swap_month_day
from lib.dvc import files_meta_frame


def sanitize_date(month: str, day: str, year: str) -> typing.Tuple[str, str, str]:
    month, day = month.lstrip("0"), day.lstrip("0")
    if month != "":
        n = int(month)
        if n < 1 or n > 12:
            return "", "", ""
    if day != "":
        n = int(day)
        if n < 1 or n > 31:
            return "", "", ""
    if year != "":
        n = int(year)
        if n < 1900 or n > datetime.date.today().year:
            return "", "", ""
    return month, day, year


mdy_num_re = re.compile(r"(?:^|[^0-9])([0-9]{1,2}) ([0-9]{1,2}) ([0-9]{2,4})")
months = [datetime.date(2022, i, 1).strftime("%B").lower() for i in range(1, 13)]
months_pat = r"(?:^|[^a-zA-Z])(%s|%s)" % (
    "|".join(months),
    "|".join(v[:3] for v in months),
)
mdy_string_re = re.compile(
    r"%s[^\w]{1,2}([0-9]{1,2})[^\w]{1,2}([0-9]{2,4})" % months_pat
)
my_string_re = re.compile(r"%s[^\w]{1,2}([0-9]{4})" % months_pat)
md_string_re = re.compile(r"%s[^\w]{1,2}([0-9]{1,2})" % months_pat)
ymd_num_re = re.compile(r"([0-9]{4})([0-9]{2})([0-9]{2})")
ymd_num2_re = re.compile(r"([0-9]{4}) ([0-9]{1,2}) ([0-9]{1,2})")


def parse_date(df: pd.DataFrame) -> pd.DataFrame:
    def extract_date(orig: str) -> typing.Dict:
        m = mdy_num_re.search(orig)
        if m is not None:
            month, day, year = m.group(1), m.group(2), m.group(3)
            year = full_year_str(year)
            month, day = swap_month_day(month, day)
            return sanitize_date(month, day, year)

        m = mdy_string_re.search(orig)
        if m is not None:
            month, day, year = m.group(1), m.group(2), m.group(3)
            month = str(datetime.datetime.strptime(month[:3], "%b").month)
            year = full_year_str(year)
            return sanitize_date(month, day, year)

        m = my_string_re.search(orig)
        if m is not None:
            month, year = m.group(1), m.group(2)
            month = str(datetime.datetime.strptime(month[:3], "%b").month)
            return sanitize_date(month, "", year)

        m = md_string_re.search(orig)
        if m is not None:
            month, day = m.group(1), m.group(2)
            month = str(datetime.datetime.strptime(month[:3], "%b").month)
            return sanitize_date(month, day, "")

        m = ymd_num_re.search(orig)
        if m is not None:
            year, month, day = m.group(1), m.group(2), m.group(3)
            return sanitize_date(month, day, year)

        m = ymd_num2_re.search(orig)
        if m is not None:
            year, month, day = m.group(1), m.group(2), m.group(3)
            return sanitize_date(month, day, year)

        return "", "", ""

    dates = pd.DataFrame.from_records(
        df.filepath.str.lower()
        .str.replace(r"[^a-z0-9 ]", " ", regex=True)
        .str.strip()
        .map(extract_date)
    )
    dates.columns = ["month", "day", "year"]
    return pd.concat([df, dates], axis=1)


def set_filetype(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, "filetype"] = "unknown"
    df.loc[df.filepath.str.lower().str.endswith(".pdf"), "filetype"] = "pdf"
    df.loc[df.filepath.str.endswith(".doc"), "filetype"] = "word"
    df.loc[df.filepath.str.endswith(".docx"), "filetype"] = "word"
    return df


def split_filepath(df: pd.DataFrame) -> pd.DataFrame:
    filepaths = df.filepath.str.split("/")
    df.loc[:, "region"] = filepaths.map(lambda v: v[0])
    df.loc[:, "fn"] = filepaths.map(lambda v: v[-1])
    return df


def set_file_category(df: pd.DataFrame) -> pd.DataFrame:
    column = "file_category"
    df.loc[:, column] = "other"
    df.loc[df.region == "orleans", column] = "memo"
    df.loc[
        (df.region == "orleans") & df.fn.str.match(r".*dates.*", case=False), column
    ] = "dates"
    df.loc[
        (df.region == "orleans") & df.fn.str.match(r".*^[A-Z][a-z]+,.*", case=False),
        column,
    ] = "transcript"
    df.loc[df.fn.str.match(r".*memo.*", case=False), column] = "memo"
    df.loc[df.fn.str.match(r".*agenda.*", case=False), column] = "agenda"
    df.loc[df.region == "addis", column] = "minutes"
    df.loc[df.fn.str.match(r".*cancelled.*", case=False), column] = "cancelled"
    df.loc[df.fn.str.match(r".*city of [^\s]+ v.*", case=False), column] = "transcript"
    df.loc[df.fn.str.match(r".*v\.? city of.*", case=False), column] = "transcript"
    df.loc[df.fn.str.match(r".*transcript.*", case=False), column] = "transcript"
    df.loc[df.fn.str.match(r".*mpecsbm.*", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r".*fpcsb_mprr.*", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r".*meeting min.*", case=False), column] = "minutes"
    df.loc[df.fn.str.match(r".*minutes.*", case=False), column] = "minutes"
    df.loc[
        df.fn.str.match(r".*sulphur civil service.*", case=False), column
    ] = "minutes"
    df.loc[df.fn.str.match(r".*carencro-municipal.*", case=False), column] = "minutes"
    df.loc[
        (df.region == "lake_charles") & df.fn.str.match(r".*records.*", case=False),
        column,
    ] = "minutes"
    df.loc[df.region == "broussard", column] = "minutes"
    df.loc[
        df.filepath.str.match(r".*(receipt|invoice|transa).*", case=False), column
    ] = "receipt"
    return df


def clean_minutes() -> pd.DataFrame:
    return (
        files_meta_frame("raw_minutes.dvc")
        .pipe(set_filetype)
        .pipe(split_filepath)
        .pipe(set_file_category)
        .pipe(parse_date)[
            [
                "fileid",
                "filetype",
                "region",
                "year",
                "month",
                "day",
                "file_category",
                "filepath",
                "filesha1",
            ]
        ]
    )


if __name__ == "__main__":
    df = clean_minutes()
    df.to_csv(deba.data("meta/minutes_files.csv"), index=False)
