import pandas as pd

from lib.columns import clean_column_names, set_values
import dirk
from lib.standardize import standardize_from_lookup_table
from lib.uid import gen_uid
from lib.clean import clean_sexes, clean_races


def clean():
    return (
        pd.read_csv(dirk.data("raw/carencro_pd/carencro_pd_pprr_2021.csv"))
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
                ["military/activated"],
            ],
        )
        .pipe(set_values, {"agency": "Carencro PD"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_races, ["race"])
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(dirk.data("clean/pprr_carencro_pd_2021.csv"), index=False)
