import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def clean():
    df = (
        pd.read_csv(deba.data("raw/lockport_pd/lockport_pd_cprr_2021_byhand.csv"))
        .pipe(clean_column_names)
        .rename(columns={"notification_date": "termination_date"})\
        .pipe(set_values, {"agency": "lockport-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["disposition", "action", "receive_date", "uid"], "allegation_uid"
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_lockport_pd_2021.csv"), index=False)
