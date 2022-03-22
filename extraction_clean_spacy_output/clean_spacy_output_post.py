import pandas as pd
import os
import re
from lib.uid import gen_uid
import deba
from lib.clean import clean_names
from lib.columns import clean_column_names


def clean_post_agency():
    # split row if file contians multiuple accused names
    directory = os.listdir(deba.data("raw/post/spacy"))
    os.chdir(deba.data("raw/post/spacy"))
    for hearing in directory:
        if hearing.endswith(".csv"):
            df = pd.read_csv(hearing).pipe(clean_column_names)

            for columns in df.columns:
                if "agency" in df.columns:
                    data = (
                        df.agency.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency"] = data[0]
                    df.loc[:, "employment_status"] = data[1]
                    df.loc[:, "hire_date"] = data[2]
                    df.loc[:, "left_date"] = data[3]
                    df.loc[:, "left_reason"] = data[4]
                if "agency" not in df.columns:
                    break
                if "agency_1" in df.columns:
                    data = (
                        df.agency_1.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency_1"] = data[0]
                    df.loc[:, "employment_status_1"] = data[1]
                    df.loc[:, "hire_date_1"] = data[2]
                    df.loc[:, "left_date_1"] = data[3]
                    df.loc[:, "left_reason_1"] = data[4]
                if "agency_1" not in df.columns:
                    break
                if "agency_2" in df.columns:
                    data = (
                        df.agency_2.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency_2"] = data[0]
                    df.loc[:, "employment_status_2"] = data[1]
                    df.loc[:, "hire_date_2"] = data[2]
                    df.loc[:, "left_date_2"] = data[3]
                    df.loc[:, "left_reason_2"] = data[4]
                if "agency_2" not in df.columns:
                    break
                if "agency_3" in df.columns:
                    data = (
                        df.agency_3.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency_3"] = data[0]
                    df.loc[:, "employment_status_3"] = data[1]
                    df.loc[:, "hire_date_3"] = data[2]
                    df.loc[:, "left_date_3"] = data[3]
                    df.loc[:, "left_reason_3"] = data[4]
                if "agency_3" not in df.columns:
                    break
                if "agency_4" in df.columns:
                    data = (
                        df.agency_4.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency_4"] = data[0]
                    df.loc[:, "employment_status_4"] = data[1]
                    df.loc[:, "hire_date_4"] = data[2]
                    df.loc[:, "left_date_4"] = data[3]
                    df.loc[:, "left_reason_4"] = data[4]
                if "agency_4" not in df.columns:
                    break
                if "agency_5" in df.columns:
                    data = (
                        df.agency_5.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency_5"] = data[0]
                    df.loc[:, "employment_status_5"] = data[1]
                    df.loc[:, "hire_date_5"] = data[2]
                    df.loc[:, "left_date_5"] = data[3]
                    df.loc[:, "left_reason_5"] = data[4]
                if "agency_5" not in df.columns:
                    break
                if "agency_6" in df.columns:
                    data = (
                        df.agency_6.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency_6"] = data[0]
                    df.loc[:, "employment_status_6"] = data[1]
                    df.loc[:, "hire_date_6"] = data[2]
                    df.loc[:, "left_date_6"] = data[3]
                    df.loc[:, "left_reason_6"] = data[4]
                if "agency_6" not in df.columns:
                    break
                if "agency_7" in df.columns:
                    data = (
                        df.agency_7.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency_7"] = data[0]
                    df.loc[:, "employment_status_7"] = data[1]
                    df.loc[:, "hire_date_7"] = data[2]
                    df.loc[:, "left_date_7"] = data[3]
                    df.loc[:, "left_reason_7"] = data[4]
                if "agency_7" not in df.columns:
                    break
                if "agency_8" in df.columns:
                    data = (
                        df.agency_8.astype(str)
                        .str.lower()
                        .str.strip()
                        .str.replace(
                            r"( ?= ?| ?_ ?| ?\' ?| ?bÃ¢â\‚ ?(¬â€Ã¢â\‚)? ?(¬Ëœ)? ?)",
                            "",
                            regex=True,
                        )
                        .str.extract(
                            r"^(.+) (full-time|reserve|retired) ? ?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|voluntary (.+))?"
                        )
                    )
                    df.loc[:, "agency_8"] = data[0]
                    df.loc[:, "employment_status_8"] = data[1]
                    df.loc[:, "hire_date_8"] = data[2]
                    df.loc[:, "left_date_8"] = data[3]
                    df.loc[:, "left_reason_8"] = data[4]
                if "agency_8" not in df.columns:
                    break
            file_name = hearing
            df.to_csv(file_name, index=False)


def clean_post_names():
    #  add uid for files with one accused_name
    directory = os.listdir(deba.data("raw/post/spacy"))
    os.chdir(deba.data("raw/post/spacy"))
    for file in directory:
        if file.endswith(".csv"):
            df = pd.read_csv(file).pipe(clean_column_names)
            for columns in df.columns:
                if "officer_name" in df.columns:
                    names = (
                        df.officer_name.str.lower()
                        .str.strip()
                        .str.extract(r"(\w+\'?\w+?)\,? (\w+) ?(\w+)?")
                    )
                    df.loc[:, "last_name"] = names[0]
                    df.loc[:, "first_name"] = names[1]
                    df.loc[:, "middle_name"] = names[2]
                    df = df.drop(columns=["officer_name"]).pipe(
                        clean_names, ["first_name", "last_name", "middle_name"]
                    )

            file_name = file
            df.to_csv(file_name, index=False)


def add_uid():
    #  add uid for files with one accused_name
    directory = os.listdir(deba.data("raw/post/spacy"))
    os.chdir(deba.data("raw/post/spacy"))
    for hearing in directory:
        if hearing.endswith(".csv"):
            df = pd.read_csv(hearing).pipe(clean_column_names)
            for columns in df.columns:
                if "accused_name" in df.columns:
                    names = df.accused_name.str.extract(r"(\w+) (\w+)")
                    df.loc[:, "first_name"] = names[0]
                    df.loc[:, "last_name"] = names[1]
                    df = df.pipe(clean_names, ["first_name", "last_name"]).pipe(
                        gen_uid, ["first_name", "last_name", "agency"]
                    )
            file_name = hearing
            df.to_csv(file_name, index=False)


def remove_filename_suffix():
    directory = os.listdir(deba.data("raw/post/spacy"))
    os.chdir(deba.data("raw/post/spacy"))
    for file in directory:
        if file.endswith(".csv"):
            os.rename(file, re.sub(r".pdf.txt", "", file))


def add_title_and_file_name_column():
    # add titles and file names to csv
    directory = os.listdir(deba.data("raw/post/spacy"))
    os.chdir(deba.data("raw/post/spacy"))
    for file in directory:
        if file.endswith(".csv"):
            df = pd.read_csv(file)
            df = df.astype(str)
            os.rename(file, re.sub(r".pdf.txt", "", file))

            df["file_name"] = os.path.basename(file)
            df.loc[:, "file_name"] = df.file_name.str.replace(
                ".csv", ".pdf", regex=False
            )

            df["title"] = os.path.basename(file)
            df.loc[:, "title"] = df.title.str.replace(".csv", "", regex=False)

            file_name = file
            df.to_csv(file_name, index=False)


if __name__ == "__main__":
    clean_post_agency()
