import pandas as pd
from .path import data_file_path
from .clean import old_clean_name, clean_name
from .uid import gen_uid


def uid_translate(df, name_cols, id_cols):
    df2 = df.copy()
    for col in name_cols:
        df2.loc[:, col] = clean_name(df[col])
    df2 = gen_uid(df2, id_cols, 'uid')
    for col in name_cols:
        df2.loc[:, col] = old_clean_name(df[col])
    df2 = gen_uid(df2, id_cols, 'old_uid')
    uids = df2.loc[df2.old_uid != df2.uid, ['old_uid', 'uid']]
    fn = data_file_path('uid_translator.csv')
    try:
        uids = pd.concat([pd.read_csv(fn), uids])
    except Exception:
        pass
    uids.drop_duplicates(ignore_index=True).to_csv(fn, index=False)
    return df
