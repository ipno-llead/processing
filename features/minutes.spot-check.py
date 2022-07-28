import os
import pathlib

import pandas as pd
import deba

deba.set_root(os.path.dirname(os.path.dirname(__file__)))

data = pd.read_csv(deba.data("features/minutes.csv"))
data = data.loc[data.accused.isna() & data.appeal_header.notna()]

metadata = pd.read_csv(deba.data("meta/minutes_files.csv"))


def resolve_source_path(row: pd.Series) -> str:
    filepath = metadata.loc[metadata.fileid == row.fileid, "filepath"].iloc[0]
    return pathlib.Path(__file__).parent.parent / "data/raw_minutes" / filepath


def resolve_pageno(row: pd.Series) -> int:
    return row.pageno


if __name__ == "__main__":
    import random
    import json

    total = len(data)
    if total == 0:
        raise ValueError("data has no row")
    else:
        random.seed()
        n = min(total, 100)
        indices = set()
        while True:
            for i in range(0, 5):
                v = random.randint(0, total - 1)
                if v not in indices:
                    indices.add(v)
                    break
            else:
                break
            if len(indices) >= n:
                break
        indices = sorted(indices)
        print(
            json.dumps(
                [
                    {
                        "sourcePath": str(resolve_source_path(row)),
                        "pageNumber": resolve_pageno(row),
                        "record": {
                            k: v if pd.notna(v) else None
                            for k, v in row.to_dict().items()
                        },
                    }
                    for _, row in data.iloc[indices].iterrows()
                ],
                allow_nan=False,
            )
        )
