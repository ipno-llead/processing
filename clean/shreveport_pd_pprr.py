import deba 
import pandas as pd
from lib.columns import (clean_column_names, set_values)
from lib.clean import (
    clean_races,
    clean_sexes,
    standardize_desc_cols,
    clean_names,
)
from lib.uid import gen_uid

def assign_agency(df):
    df["agency"] = "shreveport-pd"
    return df

def clean_rank(df, cols):
    for col in cols:
        df[col] = (
            df[col].str.lower()
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )
    return df


def normalize_race_and_sex(df):
    race_map = {
        "b": "black", "w": "white", "h": "hispanic", "a": "asian",
        "i": "native american", "n": "native american",
        "multiraci": "mixed", "native": "native american",
        "black": "black", "white": "white", "asian": "asian", "hispanic": "hispanic",
        "": None, "nan": None,
    }

    sex_map = {
        "m": "male", "male": "male",
        "f": "female", "female": "female",
        "multiple": None, "": None, "nan": None,
    }

    def clean_and_dedup(value, mapping):
        tokens = str(value).lower().strip().replace("/", ",").replace(";", ",").split(",")
        tokens = [t.strip() for t in tokens if t.strip() in mapping]
        if len(tokens) == 1:
            return mapping[tokens[0]]
        else:
            return pd.NA

    df["race"] = df["race"].apply(lambda x: clean_and_dedup(x, race_map))
    df["sex"] = df["sex"].apply(lambda x: clean_and_dedup(x, sex_map))

    return df


def split_date_strings(df, date_cols):
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce", format="%m/%d/%Y")

        df[f"{col}_year"] = df[col].dt.year
        df[f"{col}_month"] = df[col].dt.month
        df[f"{col}_day"] = df[col].dt.day

    df = df.drop(columns=date_cols)
    return df


def split_middle_initial(df):
    df["middle_name"] = df["first_name"].str.extract(r"\b(\w)$")  # last single letter
    df["first_name"] = df["first_name"].str.extract(r"^(\w+)")
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/shreveport_pd/shreveport_pd_pprr_1990_2001.csv"))
        .pipe(clean_column_names)
        .rename(columns={"hire_da": "hire_date", "date_separ": "separation_date", "badge": "badge_no"})
        # .pipe(lambda df: df[df["race"].isin(["b", "w", "h", "a", "i", "n", "multiraci", "native"])])
        .pipe(normalize_race_and_sex)
        #.pipe(lambda df: (print(df[["race", "sex"]].drop_duplicates()), df)[1])
        .pipe(split_middle_initial)
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "shreveport_pd"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(split_date_strings, ["hire_date", "separation_date"])
        .pipe(clean_rank, ["rank"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_shreveport_pd_1990_2001.csv"), index=False)
