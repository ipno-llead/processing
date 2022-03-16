import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, float_to_int_str
from lib.uid import gen_uid
import pandas as pd


def assign_allegations(df):
    df.loc[:, "allegation"] = df.rule_violation.str.cat(
        df.paragraph_violation, sep=" - paragraph "
    )
    df = df.drop(columns=["rule_violation", "paragraph_violation"])
    return df


def clean():
    df = pd.read_csv(
        deba.data("raw/greenwood_pd/greenwood_pd_cprr_2015-2020_byhand.csv")
    )
    df = clean_column_names(df)
    df = df.rename(
        columns={
            "title": "rank_desc",
            "incident_date": "occur_date",
            "complaintant": "complainant_type",
            "complaintant_race": "complainant_race",
            "complaintant_gender": "complainant_sex",
        }
    )
    return (
        df.pipe(clean_dates, ["occur_date", "receive_date"])
        .pipe(float_to_int_str, ["comission_number"])
        .pipe(set_values, {"agency": "Greenwood PD"})
        .pipe(gen_uid, ["first_name", "last_name"], "uid")
        .pipe(assign_allegations)
        .pipe(
            gen_uid,
            ["first_name", "last_name", "occur_year", "occur_month", "occur_day"],
            "allegation_uid",
        )
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_greenwood_pd_2015_2020.csv"), index=False)
