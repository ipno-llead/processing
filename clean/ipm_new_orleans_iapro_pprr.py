import deba
from lib.columns import clean_column_names
from lib.clean import (
    clean_races,
    float_to_int_str,
    standardize_desc_cols,
    clean_sexes,
    clean_names,
    clean_dates,
)
from lib.uid import gen_uid
import pandas as pd


def remove_badge_number_zeroes_prefix(df):
    df.loc[:, "badge_no"] = df.badge_no.str.replace(r"^0+", "", regex=True)
    return df


def clean_employee_type(df):
    df.loc[:, "employee_type"] = df.employee_type.str.lower().str.replace(
        r"commisioned", "commissioned"
    )
    return df


def strip_time_from_dates(df, cols):
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.replace(r" \d+:.+", "", regex=True)
            .str.replace(r"(\d{4})-(\d{2})-(\d{2})", r"\2/\3/\1", regex=True)
        )
    return df


def assign_agency(df):
    df.loc[:, "data_production_year"] = "2018"
    df.loc[:, "agency"] = "New Orleans PD"
    return df


def clean_current_supervisor(df):
    df.loc[:, "employee_id"] = df.employee_id.astype(str)
    officer_number_dict = df.set_index("employee_id").uid.to_dict()
    df.loc[:, "current_supervisor"] = df.current_supervisor.map(
        lambda x: officer_number_dict.get(x, "")
    )
    return df


def remove_unnamed_officers(df):
    df.loc[:, "last_name"] = (
        df.last_name.str.replace(r"^unknown.*", "", regex=True)
        .str.replace(r"^none$", "", regex=True)
        .str.replace("not an nopd officer", "", regex=False)
    )
    return df[df.last_name != ""].reset_index(drop=True)


def clean_department_desc(df):
    df.department_desc = (
        df.department_desc.str.lower()
        .str.strip()
        .str.replace(r"(fob|isb|msb|pib|not) - ", "", regex=True)
        .str.replace(r"\bservice\b", "services", regex=True)
        .str.replace("nopd officer", "", regex=False)
    )
    return df


def clean_rank_desc(df):
    df.rank_desc = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(r' ?investigative special$', '', regex=True)
        .str.replace(r" ?police", "", regex=True)
        .str.replace(r"dec$", "decree", regex=True)
        .str.replace(r"supt", "superintendent", regex=False)
        .str.replace(r"\bdev(e)?\b", "developer", regex=True)
        .str.replace(",", " ", regex=False)
        .str.replace(r"iv$", "", regex=True)
        .str.replace(r" ?-", " ", regex=True)
        .str.replace(r"(ii?i?|1|2|3|4)?$", "", regex=True)
        .str.replace(r"spec$", "specialist", regex=True)
        .str.replace(r"sup(v)?$", "supervisor", regex=True)
        .str.replace(r"\basst\b", "assistant", regex=True)
        .str.replace(" ?sr", "senior", regex=True)
        .str.replace(r" ?mgr", " manager", regex=True)
        .str.replace(" academy", "", regex=False)
        .str.replace(r" \boff\b ?", " officer", regex=True)
        .str.replace(r" of$", "", regex=True)
        .str.replace(r"(3|4|&|5)", "", regex=True)
        .str.replace(r" \bcoor\b", " coordinator", regex=True)
        .str.replace(r"\bopr\b", "operations", regex=True)
        .str.replace("default", "", regex=False)
        .str.replace(r"\bspec\b", "specialist", regex=True)
        .str.replace("recov", "recovery", regex=False)
        .str.replace(r"\bprog\b", "program", regex=True)
        .str.replace(r"\btech\b", "technician", regex=True)
        .str.replace("applic", "application", regex=False)
        .str.replace(r" \(nopd\)$", "", regex=True)
        .str.replace("cnslr", "counseler", regex=False)
        .str.replace(r"\binfo\b", "information,", regex=True)
        .str.replace('  ', ' ', regex=False)
        .str.replace(r'awards coord \( dept\)', 'awards coordinator', regex=True)
        .str.replace(r'information?\,?', 'information', regex=True)
        .str.replace(r'\binstru\b', 'instructor', regex=True)
        .str.replace(r"\badmini?n?s?t?r?a?t?i?v?e?\b", "admin", regex=True)
        .str.replace(r'\bcouns\b', 'counselor', regex=True)
        .str.replace(r'\bfield\b', '', regex=True)
        .str.replace(r'\(eis\)', '', regex=True)
        .str.replace(r'\banalyt?\b', 'analyst', regex=True)
        .str.replace(r'\bse$', '', regex=True)
        .str.replace(r'\bapp inv a\b', '', regex=True)
        .str.replace(r' \bex\b ', '', regex=True)
        .str.replace(r'\bsup sup\b', '', regex=True)
        .str.replace(r'operations$', 'operator', regex=True)
        .str.replace(r'^unknown rank$', '', regex=True)
        .str.replace(r'^dna analyst senior$', 'senior dna analyst', regex=True)
    )
    return df


def clean():
    df = pd.read_csv(deba.data("raw/ipm/new_orleans_iapro_pprr_1946-2018.csv"))
    df = df.dropna(axis=1, how="all")
    df = clean_column_names(df)
    print(df.columns.tolist())
    df = df.drop(
        columns=["employment_number", "working_status", "shift_hours", "exclude_reason"]
    )
    df = df.rename(
        columns={
            "badge_number": "badge_no",
            "title": "rank_desc",
            "employment_ended_on": "left_date",
            "officer_department": "department_desc",
            "officer_division": "division_desc",
            "officer_sub_division_a": "sub_division_a_desc",
            "officer_sub_division_b": "sub_division_b_desc",
            "assigned_to_unit_on": "dept_date",
            "hired_on": "hire_date",
            "officer_sex": "sex",
            "officer_race": "race",
            "middle_initial": "middle_name",
            "officer_number": "employee_id",
        }
    )
    return (
        df.pipe(
            float_to_int_str, ["years_employed", "current_supervisor", "birth_year"]
        )
        .pipe(remove_badge_number_zeroes_prefix)
        .pipe(clean_rank_desc)
        .pipe(
            standardize_desc_cols,
            ["rank_desc", "employment_status", "officer_inactive", "department_desc"],
        )
        .pipe(clean_employee_type)
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_races, ["race"])
        .pipe(clean_department_desc)
        .pipe(assign_agency)
        .pipe(gen_uid, ["agency", "employee_id"])
        .pipe(strip_time_from_dates, ["hire_date", "left_date", "dept_date"])
        .pipe(clean_dates, ["hire_date", "left_date", "dept_date"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(remove_unnamed_officers)
        .pipe(clean_current_supervisor)
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_new_orleans_ipm_iapro_1946_2018.csv"), index=False)
