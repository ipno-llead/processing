import sys

sys.path.append("../")
import pandas as pd
from lib.path import data_file_path
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, standardize_desc_cols, clean_dates
from lib.uid import gen_uid


def clean():
    df = (
        pd.read_csv(data_file_path("raw/erath_pd/erath_pd_cprr_2018_2020_byhand.csv"))
        .pipe(clean_column_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["initial_action", "action"])
        .pipe(clean_dates, ["investigation_start_date"])
        .pipe(set_values, {"agency": "Erath PD"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["uid", "allegation", "initial_action", "action"], "allegation_uid"
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(data_file_path("clean/cprr_erath_pd_2018_2020.csv"), index=False)
