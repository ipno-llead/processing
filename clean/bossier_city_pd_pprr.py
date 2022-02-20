import pandas as pd
import bolo
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, clean_salaries, float_to_int_str
from lib import salary
from lib.uid import gen_uid


def extract_department_desc(df):
    df.loc[
        :, "actual_position_title"
    ] = df.actual_position_title.str.lower().str.strip()
    departments = df.actual_position_title.str.extract(
        r"(jaile?r?|(dept)?\.?records|public information|"
        r"communications?|patrolman|office|information tech)"
    )
    df.loc[:, "department_desc"] = (
        departments[0]
        .str.replace(r"dept\.records", "records", regex=True)
        .str.replace(r"jaile?r?", "corrections", regex=True)
        .str.replace("patrolman", "patrol", regex=False)
        .str.replace(r"communication$", "communications", regex=True)
        .str.replace("office", "administration", regex=False)
        .str.replace("information tech", "it")
    )
    return df


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.actual_position_title.str.lower()
        .str.strip()
        .str.replace(r"\.", "", regex=True)
        .str.replace(r" ?-? ?(of)? ?police ?", "", regex=True)
        .str.replace(
            r" ?d?e?p?t?records ?| \biv?i?i?\b|legal |"
            r"\boffice\b |public information |information tech |"
            r" ?(of)? ?communications? ?| \(part time\)|jail ",
            "",
            regex=True,
        )
        .str.replace(r"secretary.+", "secretary", regex=True)
        .str.replace(r" ?- ?", "", regex=True)
        .str.replace(r"adm asst.+", "administrative assistant", regex=True)
        .str.replace(r"\bsuperviso\b", "supervisor", regex=True)
    )
    return df.drop(columns="actual_position_title")


def split_name(df):
    df.loc[:, "employee_name"] = (
        df.employee_name.str.lower()
        .str.strip()
        .str.replace(r"(\w+),? (\w+)\.?,", r"\1 \2,", regex=True)
        .str.replace(r"^mc gee", "mcgee", regex=True)
        .str.replace(r"christopherc$", "christopher c", regex=True)
        .str.replace(r"christopherl$", "christopher l", regex=True)
        .str.replace(r"(\w+), (\w{1})\.? (\w+)", r"\1, \3 \2", regex=True)
        .str.replace(r"  +", " ", regex=True)
    )
    names = df.employee_name.str.extract(
        r"^(\w+-?\w+?) ?(iii?|jr|iv)?, (\w+\'?\w+) ?(.+)?"
    )
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "suffix"] = names[1].fillna("")
    df.loc[:, "first_name"] = names[2].fillna("")
    df.loc[:, "middle_name"] = names[3].fillna("")
    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ").fillna("")
    return df


def drop_rows_with_missing__firt_and_last_name(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def assign_agency(df):
    df.loc[:, "agency"] = "Bossier City PD"
    return df


def clean_employee_id(df):
    df.loc[:, "employee_id"] = df.employee_number.fillna("")
    return df.drop(columns="employee_number")


def clean():
    df = (
        pd.read_csv(bolo.data("raw/bossier_city_pd/bossiercity_pd_pprr_2019.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "2000": "salary_2000",
                "2001": "salary_2001",
                "2002": "salary_2002",
                "2003": "salary_2003",
                "2004": "salary_2004",
                "2005": "salary_2005",
                "2006": "salary_2006",
                "2007": "salary_2007",
                "2008": "salary_2008",
                "2009": "salary_2009",
                "2010": "salary_2010",
                "2011": "salary_2011",
                "2012": "salary_2012",
                "2013": "salary_2013",
                "2014": "salary_2014",
                "2015": "salary_2015",
                "2016": "salary_2016",
                "2017": "salary_2017",
                "2018": "salary_2018",
                "2019": "salary_2019",
            }
        )
        .pipe(clean_employee_id)
        .pipe(extract_department_desc)
        .pipe(clean_rank_desc)
        .pipe(split_name)
        .pipe(
            float_to_int_str,
            [
                "salary_2000",
                "salary_2001",
                "salary_2002",
                "salary_2003",
                "salary_2004",
                "salary_2005",
                "salary_2006",
                "salary_2007",
                "salary_2008",
                "salary_2009",
                "salary_2010",
                "salary_2011",
                "salary_2012",
                "salary_2013",
                "salary_2014",
                "salary_2015",
                "salary_2016",
                "salary_2017",
                "salary_2018",
                "salary_2019",
            ],
        )
        .pipe(
            clean_salaries,
            [
                "salary_2000",
                "salary_2001",
                "salary_2002",
                "salary_2003",
                "salary_2004",
                "salary_2005",
                "salary_2006",
                "salary_2007",
                "salary_2008",
                "salary_2009",
                "salary_2010",
                "salary_2011",
                "salary_2012",
                "salary_2013",
                "salary_2014",
                "salary_2015",
                "salary_2016",
                "salary_2017",
                "salary_2018",
                "salary_2019",
            ],
        )
        .pipe(set_values, {"salary_freq": salary.YEARLY})
        .pipe(clean_dates, ["birth_date", "hire_date"])
        .pipe(assign_agency)
        .pipe(drop_rows_with_missing__firt_and_last_name)
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency"])
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(bolo.data("clean/pprr_bossier_city_pd_2000_2019.csv"), index=False)
