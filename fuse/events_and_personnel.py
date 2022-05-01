import deba
import pandas as pd


def join():
    dfa = pd.read_csv(deba.data("fuse/event.csv"))
    dfb = pd.read_csv(deba.data("fuse/personnel.csv"))

    df = pd.merge(
        dfa[["agency", "uid"]],
        dfb[["first_name", "last_name", "middle_name", "uid"]],
        how="outer",
        on="uid",
    )

    df = df.dropna().drop_duplicates(subset=["uid"])
    return df


if __name__ == "__main__":
    df = join()
    df.to_csv(deba.data("fuse/events_and_personnel.csv"), index=False)
