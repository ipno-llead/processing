import pandas as pd

from lib.columns import clean_column_names, set_values
import deba
from lib.standardize import standardize_from_lookup_table
from lib.uid import gen_uid
from lib.clean import clean_sexes, clean_races


def clean():
    return (
        pd.read_csv(deba.data("raw/carencro_pd/carencro_pd_pprr_2021.csv"))
        .pipe(clean_column_names)
        .pipe(
            standardize_from_lookup_table,
            "rank_desc",
            [
                ["chief"],
                ["assistant chief"],
                ["officer", "p.o"],
                ["captain"],
                ["detective"],
                ["sergeant", "sargeant"],
                ["military"],
            ],
        )
        .pipe(set_values, {"agency": "carencro-pd"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_races, ["race"])
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_carencro_pd_2021.csv"), index=False)
