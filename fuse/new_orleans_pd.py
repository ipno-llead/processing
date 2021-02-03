import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.columns import (
    rearrange_personnel_columns, rearrange_personnel_history_columns
)

import sys
sys.path.append("../")


def deduplicate_personnel_data(df):
    records = dict()
    for _, row in df.iterrows():
        if row.uid not in records:
            records[row.uid] = {"uid": row.uid}
        record = records[row.uid]
        for col in ["last_name", "middle_name", "middle_initial", "first_name", "birth_year"]:
            if not pd.isnull(row[col]):
                record[col] = row[col]
    return pd.DataFrame.from_records(list(records.values()))


def fuse():
    df = pd.read_csv(
        data_file_path("clean/pprr_new_orleans_pd.csv"))

    return (
        rearrange_personnel_columns(
            deduplicate_personnel_data(rearrange_personnel_columns(df))),
        rearrange_personnel_history_columns(df)
    )


if __name__ == "__main__":
    personnel_df, history_df = fuse()
    ensure_data_dir("fuse")
    personnel_df.to_csv(data_file_path(
        "fuse/per_new_orleans_pd.csv"), index=False)
    history_df.to_csv(data_file_path(
        "fuse/perhist_new_orleans_pd.csv"), index=False)
