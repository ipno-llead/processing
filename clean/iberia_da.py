import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names
from lib.uid import gen_uid


def split_names(df):
    names = df.name.str.lower().str.strip().str.extract(r"(\w+)\, (\w+) ?(\"\w+ ?\")?")

    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "alias"] = names[2].str.replace(r"\"", "", regex=True)
    return df.drop(columns=["name"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/iberia_da/iberia_da_brady_2022.csv"))
        .pipe(clean_column_names)
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"source_agency": "iberia-da"})
        .pipe(set_values, {"agency": ""})
        .pipe(gen_uid, ["first_name", "last_name"], "uid")
        .pipe(gen_uid, ["first_name", "last_name", "source_agency"], "brady_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/brady_iberia_da_2022.csv"), index=False)
