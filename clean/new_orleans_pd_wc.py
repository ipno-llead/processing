import pandas as pd
from lib.uid import gen_uid
import deba
from lib.columns import set_values


def venezia():
    df = (pd.read_csv(deba.data("raw/new_orleans_pd/venezia_martin.csv"))
          .pipe(set_values, {"agency": "new-orleans-pd"})
          .pipe(gen_uid, ["first_name", "last_name", "agency"])
          .pipe(gen_uid, ["uid", "incident_date", "allegation", "disposition"], "allegation_uid")
    )
    return df 

if __name__ == "__main__":
    df = venezia()
    df.to_csv(deba.data("clean/cprr_new_orleans_pd_venezia.csv"), index=False)