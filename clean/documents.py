import pandas as pd
import deba


def clean():
    df = pd.read_csv(deba.data("raw/documents/docs.csv"))
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/documents.csv"), index=False)
