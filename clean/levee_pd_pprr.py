import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_rank, split_names, clean_names, strip_leading_comma, standardize_desc_cols
from lib.uid import gen_uid


def split_officer_name(df):
    df["officer"] = df["officer"].str.strip(" '\"")
    names = df["officer"].str.extract(r"(?P<last_name>[^,]+),\s*(?P<first_name>.+)")
    df["first_name"] = names["first_name"]
    df["last_name"] = names["last_name"]
    df = df.drop(columns=["officer"])
    return df

def split_date_column(df, column):
    # Clean and parse date
    df[column] = df[column].str.strip(" '\"")
    df[column] = pd.to_datetime(df[column], errors="coerce", format="%m/%d/%Y")
    # Get the base name (drop the "_date" suffix if present)
    base = column.replace("_date", "")
    # Extract components
    df[f"{base}_year"] = df[column].dt.year
    df[f"{base}_month"] = df[column].dt.month
    df[f"{base}_day"] = df[column].dt.day
    return df

def strip_sex_and_race(df):
    df["race"] = df["race"].str.strip(" '\"")
    df["sex"] = df["sex"].str.strip(" '\"")
    return df

def clean():
    df = (
        pd.read_csv(deba.data("raw/levee_pd/levee_pd_pprr_1980_2025.csv"))
        .pipe(clean_column_names)
        .drop(columns=['salary'])
        .rename(columns={"year_of_birth": "birth_date", "gender":"sex", "date_of_hire": "hire_date", "date_of_termination":"termination_date"})
        .pipe(split_officer_name)
        .pipe(split_date_column, "birth_date")
        .pipe(split_date_column, "hire_date")
        .pipe(split_date_column, "termination_date")
        .pipe(strip_sex_and_race)
        .pipe(lambda df: df.assign(
            race=df["race"].replace({
                "b": "black",
                "w": "white",
                "B": "black",
                "W": "white",
                "M": "black",
                "": pd.NA,
            }),
            sex=df["sex"].replace({
                "M": "male",
                "F": "female",
                "B": "male",
                "m": "male",
                "": pd.NA,
            })))
        #.pipe(clean_rank, ["rank"])
        .pipe(set_values, {"agency": "levee-pd"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )
    return df 


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_levee_pd_1980_2025.csv"), index=False)
