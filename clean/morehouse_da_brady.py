import pandas as pd
import deba
from lib.columns import clean_column_names
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid

def clean():
    df = (pd.read_csv(deba.data("raw/morehouse_da/morehouse_da_brady_2017_2022.csv"))
        .pipe(clean_column_names)\
        .rename(columns={"tracking_id": "tracking_id_og"})
        .pipe(standardize_desc_cols, ["tracking_id_og"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
    )

    return df