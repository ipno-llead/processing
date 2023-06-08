import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def clean():
    df = (
        pd.read_csv(deba.data("raw/lasalle_so/lasalle_so_cprr_2018_2022.csv"))
        .pipe(clean_column_names)
        .pipe(set_values, {"agency": "lasalle-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["allegation", "allegation_desc", "disposition", "receive_date", "uid"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_lasalle_so_2018_2022.csv"), index=False)
