import pandas as pd
import dirk
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols, clean_names
from lib.uid import gen_uid


def clean_action(df):
    df.loc[:, "action"] = (
        df.disciplinary_action.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("none", "", regex=False)
        .str.replace(" &", ";", regex=False)
        .str.replace("written reprimand", "letter of reprimand", regex=False)
        .str.replace(r"\,", ";", regex=True)
    )
    return df.drop(columns="disciplinary_action")


def split_names(df):
    names = (
        df.employee_name.str.lower()
        .str.strip()
        .fillna("")
        .str.extract(r"(\w+)\, (\w+) ?(\w+)?")
    )

    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2].fillna("")
    return df.drop(columns="employee_name")


def clean():
    df = (
        pd.read_csv(dirk.data("raw/eunice_pd/eunice_pd_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .drop(columns=["pages"])
        .rename(
            columns={"investigation_disposition": "disposition", "date": "receive_date"}
        )
        .pipe(clean_dates, ["receive_date"])
        .pipe(clean_action)
        .pipe(split_names)
        .pipe(standardize_desc_cols, ["disposition", "allegation"])
        .pipe(clean_names, ["last_name", "middle_name", "first_name"])
        .pipe(set_values, {"agency": "Eunice PD"})
        .pipe(gen_uid, ["last_name", "middle_name", "first_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "action", "disposition", "allegation", "receive_day"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(dirk.data("clean/cprr_eunice_pd_2019_2021.csv"), index=False)
