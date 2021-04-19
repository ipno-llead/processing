import hashlib
from lib.path import data_file_path
from lib.exceptions import NonUniqueUIDException


def gen_uid_from_row(series, id_cols):
    return hashlib.md5(series[id_cols].astype("str").agg(', '.join, axis=1).encode('utf-8')).hexdigest()


def gen_uid(df, id_cols, uid_name="uid"):
    df.loc[:, uid_name] = df[id_cols].astype("str").agg(', '.join, axis=1).map(
        lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()
    )
    uniq_df = df[[uid_name] + id_cols].drop_duplicates()
    if uniq_df[uid_name].duplicated().any():
        raise Exception(
            "uid hash collide!\n%s" % (
                uniq_df[uniq_df[uid_name].duplicated(
                    keep=False)].to_string()
            ))
    return df


def ensure_uid_unique(df, uid_cols, output_csv=False):
    if type(uid_cols) == str:
        uid_cols = [uid_cols]
    if df[df[uid_cols].duplicated()].shape[0] == 0:
        return
    dup_df = df[df[uid_cols].duplicated(keep=False)]\
        .dropna(axis=1, how='all').sort_values(uid_cols)
    if output_csv:
        dup_df.to_csv(data_file_path('duplicates.csv'), index=False)
    raise NonUniqueUIDException(
        'DataFrame is not unique:\n%s'
        % dup_df)
