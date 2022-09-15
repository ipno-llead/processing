import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def split_name(df):
    names = df.name.str.lower().str.strip().str.extract(r"(\w+) (\w+)")
    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns="name")


def assign_agency(df):
    df.loc[
        (df.last_name == "gemar") & (df.first_name == "rodney"), "agency"
    ] = "hammond-pd"

    df.loc[
        (df.last_name == "hampton") & (df.first_name == "mark"), "agency"
    ] = "hammond-pd"
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/tangipahoa_da/tangipahoa_da_brady_2021.csv"))
        .pipe(clean_column_names)
        .pipe(split_name)
        .pipe(set_values, {"agency": "tangipahoa-so", "source_agency": "tangipahoa-da"})
        .pipe(assign_agency)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "source_agency"], "brady_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/brady_tangipahoa_da_2021.csv"), index=False)
