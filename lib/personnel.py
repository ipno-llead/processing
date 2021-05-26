from lib.columns import rearrange_personnel_columns
import pandas as pd
import sys
sys.path.append("../")


def fuse_personnel(df: pd.DataFrame, *other_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Fuses personnel columns from multiple frames.

    If an uid appear across multiple frames then all columns belong to that
    uid will be collected and be present in the final output.

    Args:
        df (pd.DataFrame):
            the first frame
        other_dfs (list of pd.DataFrame):
            the rest of the frames

    Returns:
        the fused frame
    """
    df = rearrange_personnel_columns(df).set_index("uid", drop=False)
    try:
        records = df.set_index("uid", drop=False).to_dict('index')
    except ValueError:
        print(df[df.uid.duplicated(keep=False)])
        raise
    for other_df in other_dfs:
        for idx, row in rearrange_personnel_columns(other_df).set_index("uid", drop=False).iterrows():
            if idx in records:
                record = records[idx]
                for k, v in row.to_dict().items():
                    if v == '' or pd.isnull(v):
                        continue
                    if k in record and pd.notnull(record[k]) and record[k] != '':
                        continue
                    record[k] = v
            else:
                records[idx] = row.to_dict()
    return rearrange_personnel_columns(pd.DataFrame.from_records(list(records.values())))
