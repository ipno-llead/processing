import hashlib

import pandas as pd
from Levenshtein import distance


def match_records(a, b, columns, sep=", ", threshold=2):
    # ensure a[columns] is unique
    df_a = a[columns].drop_duplicates()
    # ensure b[columns] is unique
    df_b = b[columns].drop_duplicates()
    # for each entry in b find 1 entry in a that fit threshold
    # or exactly the same in which case remove entry from a
    joined_a = df_a.astype("str").agg(sep.join, axis=1).tolist()
    joined_b = df_b.astype("str").agg(sep.join, axis=1).tolist()
    set_a = set(joined_a)
    exact = []
    suspects = []
    not_found = []
    for v in joined_b:
        if v in set_a:
            set_a.remove(v)
            exact.append(v)
            continue

        found_sus = False
        for u in set_a:
            dist = distance(u, v)
            if dist <= threshold:
                found_sus = True
                suspects.append((u, v, dist))

        if not found_sus:
            not_found.append(v)

    print("%d exact match" % len(exact))
    print("%d not found" % len(not_found))
    print("%d suspects (Levenshtein distance <= %d)" %
          (len(suspects), threshold))
    sus_df = pd.DataFrame.from_records(suspects, columns=["a", "b", "dist"])
    if len(sus_df) > 0:
        print("suspect cases are:\n%s" % sus_df.to_string())

    return (
        pd.Series(exact),
        sus_df,
        pd.Series(not_found)
    )


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


def concat_dfs(dfs):
    """
    Insert empty strings for every missing columns before pd.concat
    so that no float casting happen.
    """
    col_names = set([col for df in dfs for col in df.columns])
    for col in col_names:
        for df in dfs:
            if col not in df.columns:
                df.loc[:, col] = ""
    return pd.concat(dfs)


def show_similar_rows(a, b, cols):
    """
    Show rows with the same values between a and b for `cols`
    """
    a = a.set_index(cols)
    b = b.set_index(cols)

    i = 0
    for k, v in a.iterrows():
        try:
            s = b.loc[k]
        except KeyError:
            continue
        print(k)
        print("  %s" % v.to_frame().transpose().to_string())
        print("  %s" % s.to_string())
        i += 1
        if i == 20:
            break
