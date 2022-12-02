import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid


def clean():
    df = (
        pd.read_csv(
            deba.data("raw/jefferson_davis_so/jefferson_davis_so_2013_2022_byhand.csv"), encoding="ISO-8859-1"
        )
        .pipe(clean_column_names)
        .pipe(standardize_desc_cols, ["allegation", "allegation_desc"])
        .rename(columns={"action_date": "termination_date"})
        .pipe(set_values, {"agency": "jefferson-davis-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["allegation", "allegation_desc", "action", "uid"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_jefferson_davis_so_2013_2022.csv"), index=False)
