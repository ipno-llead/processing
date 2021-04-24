from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_names
import pandas as pd
import sys
sys.path.append("../")


def remove_header_rows(df):
    return df[
        df.date_received.str.strip() != "Date\rReceived"
    ].reset_index(drop=True)


def rename_columns(df):
    df = df.drop(columns=[
        'month', 'quarter', 'numb_er_of_cases', 'related_item_number'])
    df = df.rename(columns={
        'date_received': 'receive_date',
        'case_number': 'tracking_number',
        'job_title': 'rank_desc',
        'charge_disposition': 'disposition',
        'location_or_facility': 'department_desc',
        'assigned_agent': 'investigating_supervisor'
    })
    return df


def remove_carriage_return(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r"-\r", r"-")\
            .str.replace(r"\r", r" ")
    return df


def split_name(df):
    series = df.name_of_accused.fillna('').str.strip()
    for col, pat in [('first_name', r"^([\w'-]+)(.*)$"), ('middle_initial', r'^(\w\.) (.*)$')]:
        names = series[series.str.match(pat)].str.extract(pat)
        df.loc[series.str.match(pat), col] = names[0]
        series = series.str.replace(pat, r'\2').str.strip()
    df.loc[:, 'last_name'] = series
    return df.drop(columns=['name_of_accused'])


def clean():
    df = pd.read_csv(data_file_path(
        "new_orleans_so/new_orleans_so_cprr_2019_tabula.csv"))
    df = clean_column_names(df)
    return df\
        .pipe(remove_header_rows)\
        .pipe(rename_columns)\
        .pipe(remove_carriage_return, [
            'name_of_accused', 'disposition', 'charges', 'summary', 'investigating_supervisor',
            'terminated_resigned'
        ])\
        .pipe(split_name)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])
