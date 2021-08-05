from lib.path import data_file_path, ensure_data_dir
from lib.columns import clean_column_names
from lib.clean import clean_dates, float_to_int_str
from lib.uid import gen_uid
import pandas as pd
import sys
sys.path.append("../")


def assign_charges(df):
    df.loc[:, 'charges'] = df.rule_violation.str.cat(
        df.paragraph_violation, sep=' - paragraph ')
    df = df.drop(columns=['rule_violation', 'paragraph_violation'])
    return df


def clean():
    df = pd.read_csv(
        data_file_path("dropbox/greenwood_pd/greenwood_pd_cprr_2015-2020_byhand.csv"))
    df = clean_column_names(df)
    df = df.rename(columns={
        'title': 'rank_desc',
        'incident_date': 'occur_date',
        'complaintant': 'complainant_type',
        'complaintant_race': 'complainant_race',
        'complaintant_gender': 'complainant_sex'
    })
    return df\
        .pipe(clean_dates, ['occur_date', 'receive_date'])\
        .pipe(float_to_int_str, ['comission_number'])\
        .pipe(gen_uid, ['first_name', 'last_name'], 'mid')\
        .pipe(assign_charges)\
        .pipe(gen_uid, ['first_name', 'last_name', 'occur_year', 'occur_month', 'occur_day'], 'complaint_uid')


if __name__ == '__main__':
    df = clean()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/cprr_greenwood_pd_2015_2020.csv"), index=False)
