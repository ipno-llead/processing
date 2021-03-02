from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
import pandas as pd
import sys
sys.path.append("../")


def clean():
    df = pd.read_csv(data_file_path(
        "ipm/new_orleans_iapro_pprr_1946-2018.csv"))
    df = df.dropna(axis=1, how="all")
    df = clean_column_names(df)
    df = df.drop(columns=["employment_number"])
    df.columns = ['employee_id', 'badge_no', 'rank_desc', 'employee_type',
                  'employment_status', 'hired_date', 'term_date',
                  'officer_inactive', 'years_employed', 'working_status', 'shift_hours',
                  'exclude_reason', 'birth_year', 'sex', 'race',
                  'officer_department', 'officer_division', 'officer_sub_division_a',
                  'officer_sub_division_b', 'assigned_to_unit_on', 'first_name',
                  'middle_name', 'last_name', 'current_supervisor']
    return df
