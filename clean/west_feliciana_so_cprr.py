import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import clean_dates, float_to_int_str, clean_names, standardize_desc_cols, split_names
import pandas as pd

import pandas as pd

def split_officer_names(df):
    name_split = df['officer_name'].str.strip().str.split(n=1, expand=True)
    df['first_name'] = name_split[0]
    df['last_name'] = name_split[1]
    df = df.drop(columns=['officer_name'])
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/west_feliciana_so/west_feliciana_so_cprr_2010_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "date": "receive_date",
                "employee_name": "officer_name",
                "allegation_date": "incident_date",
                "allegation_made": "allegation",
                "investigation_disposition": "disposition",
                "disciplinary_action": "action",})
        .pipe(clean_dates, ["receive_date", "incident_date"])
        .pipe(split_officer_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["allegation", "disposition", "action", "employment_status_as_of_06_30_2023"])
        .pipe(set_values, {"agency": "west-feliciana-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "action", "uid"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_west_feliciana_so_2010_2021.csv"), index=False)
