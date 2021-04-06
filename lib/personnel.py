from lib.columns import rearrange_personnel_columns
import pandas as pd
import sys
sys.path.append("../")


def fuse_personnel(df, *other_dfs):
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
