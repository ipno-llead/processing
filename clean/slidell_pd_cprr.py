import pandas as pd
import re 

from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_names, standardize_desc_cols
from lib.uid import gen_uid


def extract_and_clean_rank(df):
    rank_map = {
        r"capt\.": "captain",
        r"officer": "officer",
        r"r/o": "reporting officer",
        r"clerk": "clerk",
        r"com\. ofc\.": "communications officer",
    }

    def extract_rank_and_name(name):
        name_lower = name.lower()
        rank_found = ""
        for pattern, rank in rank_map.items():
            if re.search(pattern, name_lower):
                rank_found = rank
                name = re.sub(pattern, "", name, flags=re.IGNORECASE).strip()
                break
        return pd.Series([rank_found, name.strip(",. ")])

    df[["rank_desc", "officer_name"]] = df["officer_name"].apply(extract_rank_and_name)
    return df


def split_officer_name(df):
    def split_name(name):
        parts = name.strip().split()
        if len(parts) == 0:
            return pd.Series(["", ""])
        elif len(parts) == 1:
            return pd.Series([parts[0], ""])
        else:
            return pd.Series([parts[0], " ".join(parts[1:])])

    df[["first_name", "last_name"]] = df["officer_name"].apply(split_name)
    df = df.drop(columns=["officer_name"])
    return df


def split_date_columns(df, date_col="receive_date"):
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df["receive_year"] = df[date_col].dt.year
    df["receive_month"] = df[date_col].dt.month
    df["receive_day"] = df[date_col].dt.day
    df.drop(columns=[date_col], inplace=True)
    return df


def replace_commas_slashes_in_allegations(df):
    df["allegation"] = (
        df["allegation"]
        .astype(str)
        .str.replace(r"[,/]", ";", regex=True) 
        .str.replace(r"\s*;\s*", "; ", regex=True) 
        .str.replace(r"\s+", " ", regex=True)         
        .str.strip()                                   
        .str.rstrip(";.,")                             
    )
    return df

def clean():
    df = (
        pd.read_csv(deba.data("raw/slidell_pd/slidell_pd_cprr_2007_2010.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "date": "receive_date",
                "officer": "officer_name",
            }
        )
        .pipe(split_date_columns, date_col="receive_date")
        .pipe(extract_and_clean_rank)
        .pipe(split_officer_name)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(replace_commas_slashes_in_allegations)
        .pipe(standardize_desc_cols, ["allegation", "disposition"])
        .pipe(set_values, {"agency": "slidell-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation", "disposition", "uid"], "allegation_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_slidell_pd_2007_2010.csv"), index=False)


    
