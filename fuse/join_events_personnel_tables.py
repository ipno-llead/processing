import deba
import pandas as pd


def join_event_and_personnel():
    dfa = pd.read_csv(deba.data("fuse/event.csv"))
    dfb = pd.read_csv(deba.data("fuse/personnel.csv"))

    df = pd.merge(
        dfa[["agency", "uid"]],
        dfb[["first_name", "last_name", "middle_name", "uid"]],
        on="uid",
        how="outer",
    )
    return df.dropna().drop_duplicates(subset=["uid"])


if __name__ == "__main__":
    df = join_event_and_personnel()
    df.to_csv(deba.data("fuse/events_and_personnel_merged.csv"), index=False)
