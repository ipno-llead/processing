import json
import os
import pathlib
import subprocess

import pandas as pd
import deba

deba.set_root(os.path.dirname(os.path.dirname(__file__)))

data_file = deba.data("features/minutes.csv")

metadata = pd.read_csv(deba.data("meta/minutes_files.csv"))


def resolve_source_path(row):
    filepath = metadata.loc[metadata.fileid == row["fileid"], "filepath"].iloc[0]
    return pathlib.Path(__file__).parent.parent / "data/raw_minutes" / filepath


def resolve_pageno(row):
    return row["pageno"]


if __name__ == "__main__":

    def resolve_filepath(file):
        while file.is_symlink():
            file = (file.parent / file.readlink()).resolve()
        return file

    data_file = resolve_filepath(data_file)

    result = subprocess.run(
        ["/Users/khoipham/go/bin/taste", os.path.realpath(data_file), "-n", "20"],
        capture_output=True,
        check=True,
        cwd=os.path.dirname(__file__),
        encoding="utf-8",
    )
    print(
        json.dumps(
            [
                {
                    "sourcePath": str(resolve_source_path(row)),
                    "pageNumber": resolve_pageno(row),
                    "record": row,
                }
                for row in json.loads(result.stdout.strip())
            ]
        )
    )
