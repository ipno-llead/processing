import pandas as pd
from lib.uid import gen_uid
import deba
from lib.columns import set_values
from lib.clean import clean_dates


def assign_rank(df):
    df.loc[:, "rank_desc"] = "detective"
    return df 


def filter_agency(df):
    df = df[df.agency == "new-orleans-pd"]
    return df 


def assign_demo(df):
    df.loc[:, "race"] = "white"
    df.loc[:, "sex"] = "male"
    return df 


def venezia():
    df = (pd.read_csv(deba.data("raw/new_orleans_pd/venezia_martin.csv"))
          .pipe(assign_demo)
          .pipe(set_values, {"agency": "new-orleans-pd"})
          .pipe(gen_uid, ["first_name", "last_name", "agency"])
          .pipe(gen_uid, ["uid", "incident_date", "allegation", "disposition"], "allegation_uid")
    )
    return df 


def dillmann():
    df = (pd.read_csv(deba.data("raw/new_orleans_pd/dillmann_john.csv"))
          .pipe(assign_demo)
          .pipe(assign_rank)
          .pipe(filter_agency)
          .pipe(gen_uid, ["first_name", "last_name", "agency"])
          .pipe(clean_dates, ["hire_date", "left_date"])
          )
    return df 


if __name__ == "__main__":
    dfa = venezia()
    dfb = dillmann()
    dfa.to_csv(deba.data("clean/cprr_new_orleans_pd_venezia.csv"), index=False)
    dfb.to_csv(deba.data("clean/cprr_new_orleans_pd_dillmann.csv"), index=False)