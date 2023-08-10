import pandas as pd
import deba
from lib.uid import gen_uid
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, standardize_desc_cols, clean_dates


def clean():
    df = (
        pd.read_csv(deba.data("raw/richland_so/cprr_richland_so_2019_2023.csv"))
        .pipe(clean_column_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(set_values, {"agency": "richland-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "investigation_start_date", "disposition"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_richland_so_2019_2023.csv"), index=False)
