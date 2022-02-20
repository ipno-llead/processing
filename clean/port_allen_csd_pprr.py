from lib.columns import clean_column_names
import bolo
from lib.uid import gen_uid
from lib.clean import clean_names, clean_dates, standardize_desc_cols
import pandas as pd


def extract_name(df):
    cols = (
        df.employee_number_full_name.str.strip()
        .str.replace(r" +- +", r" ")
        .str.extract(r"^([-\d]+) +(.*) +(\w{2,})(?: (\w))?$")
    )
    cols.columns = ["employee_id", "last_name", "first_name", "middle_name"]
    return pd.concat([df, cols], axis=1).drop(columns=["employee_number_full_name"])


def standardize_employment_status(df):
    status_dict = {"T": "terminated", "A": "active", "I": "inactive"}
    df.loc[:, "employment_status"] = df.employment_status.str.strip().map(
        lambda x: status_dict.get(x, "")
    )
    return df


def fix_typo(df):
    df.loc[:, "rank_desc"] = df.rank_desc.str.replace(r"^t ", "").str.replace(
        r"offtcer", "officer"
    )
    return df


def assign_agency(df):
    df.loc[:, "data_production_year"] = "2020"
    df.loc[:, "agency"] = "Port Allen CSD"
    return df


def clean():
    df = pd.read_csv(bolo.data("raw/port_allen_csd/port_allen_csd_pprr_2020.csv"))
    df = clean_column_names(df)
    df = df.rename(
        columns={
            "dept_number": "department_code",
            "status": "employment_status",
            "rank_title": "rank_desc",
        }
    )
    df = df.drop(columns=["job_class"])
    df = df.dropna(how="all")
    return (
        df.pipe(extract_name)
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(standardize_employment_status)
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["rank_desc"])
        .pipe(fix_typo)
        .pipe(assign_agency)
        .pipe(gen_uid, ["agency", "employee_id"])
    )


if __name__ == "__main__":
    df = clean()

    df.to_csv(bolo.data("clean/pprr_port_allen_csd_2020.csv"), index=False)
