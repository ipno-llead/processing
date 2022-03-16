import pandas as pd

from lib import salary
from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_salaries
from lib.uid import gen_uid


def split_names(df):
    col_name = [col for col in df.columns if col.endswith("employee_name")][0]
    names = df[col_name].str.strip().str.lower().str.extract(r"^(\w+)[ ,]+(\w+)$")
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "last_name"] = names[0]
    return df.drop(columns=[col_name])


def clean_pprr_17_18(df, year):
    return (
        df.pipe(clean_column_names)
        .drop(columns=["department"])
        .pipe(
            set_values,
            {
                "agency": "Youngsville PD",
                "salary_freq": salary.YEARLY,
                "salary_year": year,
            },
        )
        .pipe(split_names)
        .pipe(clean_salaries, ["salary"])
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


def clean_pprr_19():
    return (
        pd.read_csv(deba.data("raw/youngsville_pd/youngsville_csd_pprr_2019.csv"))
        .pipe(clean_column_names)
        .drop(columns=["department", "department_name_category", "type"])
        .pipe(
            set_values,
            {
                "agency": "Youngsville PD",
                "salary_freq": salary.YEARLY,
                "salary_year": 2019,
                "employment_status": "full-time",
            },
        )
        .pipe(split_names)
        .pipe(clean_salaries, ["salary"])
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
    )


if __name__ == "__main__":
    df17 = clean_pprr_17_18(
        pd.read_csv(deba.data("raw/youngsville_pd/youngsville_csd_pprr_2017.csv")),
        2017,
    )
    df18 = clean_pprr_17_18(
        pd.read_csv(deba.data("raw/youngsville_pd/youngsville_csd_pprr_2018.csv")),
        2018,
    )
    df19 = clean_pprr_19()
    df17.to_csv(deba.data("clean/pprr_youngsville_csd_2017.csv"), index=False)
    df18.to_csv(deba.data("clean/pprr_youngsville_csd_2018.csv"), index=False)
    df19.to_csv(deba.data("clean/pprr_youngsville_csd_2019.csv"), index=False)
