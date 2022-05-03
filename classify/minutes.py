import deba
import pandas as pd


def classify():
    df = pd.read_csv(deba.data("features/minutes.csv"))
    df.loc[:, "pagetype"] = "continuation"
    for col_name, pt in [
        ("mtg", "meeting"),
        ("hrg", "hearing"),
        ("agd", "agenda"),
    ]:
        df.loc[df[col_name], "pagetype"] = pt
    return df.drop(columns=["mtg", "hrg", "agd"])


if __name__ == "__main__":
    df = classify()
    print("summary by jurisdiction")
    print(
        df.groupby("region")["pagetype"].value_counts().unstack().fillna(0).astype(int)
    )
    df.to_csv(deba.data("classify/minutes.csv"), index=False)
