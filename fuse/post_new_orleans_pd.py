import pandas as pd
import deba

def post():
    df = pd.read_csv(deba.data("clean/post_officer_history.csv"))
    df = df[df.agency.isin(["new-orleans-pd"])]
    return df 

if __name__ == "__main__":
    df = post()
    df.to_csv(deba.data("fuse/post_officer_history_new_orleans_pd.csv"), index=False)