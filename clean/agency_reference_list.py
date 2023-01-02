import pandas as pd
import deba


def clean():
    df = pd.read_csv(deba.data("raw/agency_reference_list/agency-reference-list.csv"))
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/agency-reference-list.csv"), index=False)
