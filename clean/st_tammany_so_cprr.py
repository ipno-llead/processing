from lib.columns import clean_column_names
import deba
from lib.clean import split_names, float_to_int_str, clean_names
from lib.uid import gen_uid
import pandas as pd


def remove_newlines(df):
    for col in ["full_name", "occur_raw_date", "allegation"]:
        df.loc[:, col] = (
            df[col]
            .str.replace(r"(\d)\r\n(\d)", r"\1\2", regex=True)
            .str.replace(r"\r\n", " ")
        )
    return df


def pad_dept_code(df):
    df.loc[df.department_code != "", "department_code"] = df.department_code.str.zfill(
        2
    )
    return df


def assign_department_desc(df):
    dept_df = pd.read_csv(
        deba.data("raw/st_tammany_so/st_tammany_department_codes_tabula.csv")
    )
    dept_df = clean_column_names(dept_df)
    dept_df.loc[:, "loc"] = dept_df.loc[:, "loc"].str.replace(r"\*$", "", regex=True)
    dept_df = dept_df.set_index("loc")
    dept_dict = dept_df.org_code.to_dict()
    dept_dict["20"] = "St. Tammany Parish Jail"

    df.loc[:, "department_desc"] = df.department_code.map(
        lambda x: dept_dict.get(x, "")
    )
    return df


def extract_occur_date(df):
    dates = df.occur_raw_date.str.extract(r"^(\d+)\/(\d+)\/(\d{4})$")
    df.loc[:, "occur_month"] = dates[0]
    df.loc[:, "occur_day"] = dates[1]
    df.loc[:, "occur_year"] = dates[2]
    return df.drop(columns=["occur_raw_date"])


def assign_agency(df):
    df.loc[:, "agency"] = "st-tammany-so"
    df.loc[:, "data_production_year"] = 2021
    return df


def remove_new_lines_from_allegations(df):
    df.loc[:, "allegation"] = df.allegation.str.replace(r"(\n|\r)\s*", " ", regex=True)
    return df


def extract_action_and_disposition(df):
    actions = (
        df.allegation.str.lower()
        .str.strip()
        .fillna("")
        .str.extract(r"- (\w+ ?\w+? ?\w+? ?\w+?)$")
    )
    df.loc[:, "action"] = (
        actions[0]
        .str.replace(r"\bhours\b", "hour", regex=True)
        .str.replace(
            r"suspend?e?d?s?i?o?n? (\w+) (\w+)", r"\1-\2 suspension", regex=True
        )
        .str.replace(r"^terminated$", "termination", regex=True)
        .str.replace(r"^demoted$", "demotion", regex=True)
        .str.replace(r"^paragraph 4$", "", regex=True)
        .str.replace(r"(^neglect of work$|^failure to work$)", "", regex=True)
        .str.replace(r"^(\w+) hours?$", r"\1-hour suspension", regex=True)
        .str.replace(
            r"^(\w+) hour ?o?f? ?suspension$", r"\1-hour suspension", regex=True
        )
        .str.replace(r"^80hours$", "80-hour suspension", regex=True)
        .str.replace(r"^verbal$", "verbal warning", regex=True)
        .str.replace(r"^other 1 year ", "1-year ", regex=True)
    )

    df.loc[:, "disposition"] = df.action.str.replace(r"(.+)", "sustained", regex=True)

    return df


def clean():
    df = pd.concat(
        [
            pd.read_csv(
                deba.data("raw/st_tammany_so/st_tammany_so_cprr_2011-2020_tabula.csv")
            ),
            pd.read_csv(
                deba.data("raw/st_tammany_so/st_tammany_so_cprr_2020-2021_tabula.csv")
            ),
        ]
    )
    df = clean_column_names(df)
    df = df.rename(
        columns={
            "dept": "department_code",
            "date_of_incident": "occur_raw_date",
            "discipline_action_outcome": "allegation",
        }
    )
    df = (
        df.pipe(split_names, "name")
        .pipe(float_to_int_str, ["department_code"])
        .pipe(pad_dept_code)
        .pipe(assign_department_desc)
        .pipe(extract_occur_date)
        .pipe(extract_action_and_disposition)
        .pipe(assign_agency)
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["agency", "occur_year", "occur_month", "occur_day", "uid", "allegation"],
            "allegation_uid",
        )
        .pipe(remove_new_lines_from_allegations)
    )
    df = df.drop(columns=["name"])
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_st_tammany_so_2011_2021.csv"), index=False)
