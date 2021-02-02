import hashlib


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
