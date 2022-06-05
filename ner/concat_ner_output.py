import pandas as pd
import os
import glob

files = os.path.join("data/ner/post/post_officer_history/output", "*.csv")

files = glob.glob(files)


def concat_ner_output():
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    df.to_csv("data/raw/post/post_officer_history_.csv", index=False)
    return df


if __name__ == "__main__":
    concat = concat_ner_output()
