import pandas as pd
import deba
from lib.uid import gen_uid


def assign_cols(df):
    df["disposition"] = "sustained"
    df["action"] = "terminated"
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/point_coupee/pointe-coupee-so-cprr-2017.csv"))
        .pipe(assign_cols)
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["allegation", "disposition", "action", "termination_date", "arrest_date"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_point_coupee_so_2017.csv"), index=False)
