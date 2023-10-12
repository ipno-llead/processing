import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_names
from lib.uid import gen_uid


def clean_agency(df):
    df.loc[:, "agency"] = df.agency.str.replace(
        r"orleans-so", "new-orleans-so", regex=False
    )
    return df[~((df.agency.fillna("") == ""))]


def concat_allegation_cols(df):
    df.loc[:, "allegation_desc"] = df.allegation.fillna("").str.cat(
        df.initial_allegation.fillna(""), sep="; "
    )

    df.loc[:, "allegation_desc"] = df.allegation.str.replace(r"; $", "", regex=True)
    return df.drop(columns=["initial_allegation", "allegation"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/morehouse_da/morehouse_da_brady_2017_2022.csv"))
        .pipe(clean_column_names)
        .rename(columns={"tracking_id": "tracking_id_og"})
        .pipe(standardize_desc_cols, ["tracking_id_og"])
        .pipe(clean_agency)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(
            set_values,
            {"source_agency": "morehouse-da", "brady_list_date": "12/1/2022"},
        )
        .pipe(concat_allegation_cols)
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "source_agency",
                "allegation_desc",
                "action",
                "tracking_id",
            ],
            "brady_uid",
        )
    )

    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/brady_morehouse_da_2022.csv"), index=False)
