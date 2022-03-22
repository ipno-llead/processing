import pandas as pd
import os
from lib.columns import rearrange_post_columns
import deba


def fuse_post():
    directory = os.listdir(deba.data("raw/post/spacy"))
    os.chdir(deba.data("raw/post/spacy"))

    df = rearrange_post_columns(
        pd.concat([pd.read_csv(file) for file in directory if file.endswith(".csv")])
    )
    df.to_csv(deba.data("raw/post/fused/padme.csv", index=False))


if __name__ == "__main__":
    fuse_post()
