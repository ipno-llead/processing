import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
import deba


def clean():
    return (
        pd.read_csv(
            deba.data(
                "raw/east_feliciana_so/cprr_east_feliciana_so_2016_2023_byhand.csv"
            ),
            encoding="ISO-8859-1",
        )
        .pipe(clean_column_names)
        .pipe(set_values, {"agency": "east-feliciana-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["allegation_desc", "disposition", "uid"], "allegation_uid")
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_east_feliciana_so_2016_2023.csv"), index=False)
