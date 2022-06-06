import re
import hashlib
import os
import pathlib
import json
from subprocess import run
import typing

import yaml
import pandas as pd


def _root_dir() -> pathlib.Path:
    return pathlib.Path(os.path.realpath(__file__)).parent.parent


def dvc_cache_dir() -> str:
    """Returns DVC cache dir"""
    return run(
        ["dvc", "cache", "dir"],
        capture_output=True,
        check=True,
        encoding="utf-8",
        cwd=_root_dir(),
    ).stdout.strip()


def open_dvc_object_from_cache(cache_dir: str, md5: str):
    """Reads a DVC object residing in cache"""
    dir_path = os.path.join(cache_dir, md5[:2], md5[2:])
    with open(dir_path, "r") as f:
        return json.load(f)


def open_dvc_files(dir_name: str) -> typing.List[typing.Dict]:
    """Returns files from a DVC-tracked folder

    Each file is a dictionary with 2 keys:
    - md5: file md5
    - relpath: file path relative to folder path
    """
    with open("%s.dvc" % dir_name, "r") as f:
        raw_dir_dvc = yaml.load(f, Loader=yaml.Loader)
    dir_md5 = raw_dir_dvc["outs"][0]["md5"]
    cache_dir = dvc_cache_dir()
    dvc_files = open_dvc_object_from_cache(cache_dir, dir_md5)
    detect_file_duplications(dir_name, dvc_files)
    return dvc_files


def strip_extensions(filename: str) -> str:
    return re.sub(r"(\.\w{3})+$", "", filename)


def detect_file_duplications(
    dvc_file_path: str, dir_name: str, dvc_files: typing.List[typing.Dict]
) -> None:
    """Detects files with the same md5 from a DVC-tracked folder

    Args:
        dir_name (str):
            the real path to the parent directory
        dvc_files (list of dict):
            list of dictionaries as read from a [hash].dir file in
            DVC cache. Each dictionary should have the following keys:
            - "relpath": path to the file relative to dir_name
            - "md5": md5 hash of the file

    Returns:
        the updated frame
    """
    files = dict()
    duplications: typing.List[typing.Tuple[str, str]] = []
    for obj in dvc_files:
        if obj["md5"] in files:
            duplications.append(
                (
                    os.path.join(dir_name, obj["relpath"]),
                    os.path.join(dir_name, files[obj["md5"]]),
                )
            )
            continue
        files[obj["md5"]] = obj["relpath"]
    if len(duplications) > 0:
        rm_files = []
        for file1, file2 in duplications:
            file1 = os.path.relpath(file1, str(_root_dir()))
            file2 = os.path.relpath(file2, str(_root_dir()))
            name1 = strip_extensions(file1)
            name2 = strip_extensions(file2)
            if name1.startswith(name2):
                rm_files.append(json.dumps(file1))
            else:
                rm_files.append(json.dumps(file2))
        rm_text = ""
        if len(rm_files) > 0:
            rm_text = (
                "\nRemove the duplications to continue:\n    rm "
                + " \\\n       ".join(rm_files)
                + (" \\\n    && dvc add --file %s %s" % (dvc_file_path, dir_name))
            )

        raise ValueError(
            "Found files with same md5:\n%s%s"
            % (
                "\n".join(
                    [
                        "    %s == %s" % (json.dumps(file1), json.dumps(file2))
                        for file1, file2 in duplications
                    ]
                ),
                rm_text,
            )
        )


def gen_filesha1(df: pd.DataFrame, dir_name: str) -> pd.DataFrame:
    """Generates filesha1 for each file

    Args:
        df (pd.DataFrame):
            the dvc file records
        dir_name (str):
            the root directory that contains all the files

    Returns:
        the frame with filesha1 column
    """

    def digest(filepath: str) -> str:
        hash = hashlib.sha1(usedforsecurity=False)
        with open(os.path.join(dir_name, filepath), "rb") as f:
            hash.update(f.read())
        return hash.hexdigest()

    df.loc[:, "filesha1"] = df.filepath.map(digest)
    return df


def set_fileid(df: pd.DataFrame) -> pd.DataFrame:
    """Generates fileid for each file

    Args:
        df (pd.DataFrame):
            the dvc file records

    Returns:
        the frame with fileid column
    """
    df.loc[:, "fileid"] = df.filesha1.map(lambda v: v[:7])
    return df


def real_dir_path(dvc_file_path: str, dvc_obj=None):
    if dvc_obj is None:
        with open(dvc_file_path, "r") as f:
            dvc_obj = yaml.load(f, Loader=yaml.Loader)
    return os.path.join(
        os.path.dirname(dvc_file_path), dvc_obj["wdir"], dvc_obj["outs"][0]["path"]
    )


def files_meta_frame(
    dvc_file_path: str,
) -> pd.DataFrame:
    """Reads files from a DVC tracked folder and returns their metadata as a frame

    The returned metadata frame will have 1 row per file, with the following columns:

    - filepath: file path relative to the DVC-tracked folder
    - filesha1: SHA1 hash of the file
    - md5:      MD5 hash of the file
    - fileid:   unique identifier of the file

    Args:
        dvc_file_path (str):
            path to the DVC file that track a folder

    Returns:
        metadata of all files as a frame
    """
    with open(dvc_file_path, "r") as f:
        dir_obj = yaml.load(f, Loader=yaml.Loader)
    dir_md5 = dir_obj["outs"][0]["md5"]
    cache_dir = dvc_cache_dir()
    dvc_files = open_dvc_object_from_cache(cache_dir, dir_md5)
    dir_name = real_dir_path(dvc_file_path, dir_obj)

    detect_file_duplications(dvc_file_path, dir_name, dvc_files)

    return (
        pd.DataFrame.from_records(dvc_files)
        .rename(columns={"relpath": "filepath"})
        .pipe(gen_filesha1, dir_name)
        .pipe(set_fileid)
    )
