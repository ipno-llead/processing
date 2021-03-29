from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.uid import gen_uid
from lib.clean import (
    clean_names, clean_dates, standardize_desc_cols
)
from lib.standardize import standardize_from_lookup_table
from lib.rows import duplicate_row
import pandas as pd
import re
import sys
sys.path.append("../")


def standardize_rank_desc(df):
    rank_map = {
        'ofc.': "officer", 'cpl.': "corporal", 'sgt.': "sergeant"
    }
    df.loc[:, "rank_desc"] = df.rank_desc.map(lambda x: rank_map.get(x, ""))
    return df


def clean_occur_time(df):
    def replace(s):
        if s == "":
            return ""
        p = s.split(":")
        if len(p) == 1:
            if int(p[0]) > 12:
                p = ["00"]+p
            else:
                p.append("00")
        return ":".join([v.zfill(2) for v in p])

    df.loc[:, "occur_time"] = df.occur_time.str.strip().fillna("")\
        .str.replace(r"\s+.+", "").map(replace)
    return df


def clean_badge_no(df):
    df.loc[:, "badge_no"] = df.badge_no.str.strip()
    return df


def clean_occur_date(df):
    df.loc[:, "occur_date"] = df.occur_date.str.strip().fillna("")\
        .str.replace(r".+\s+(.+)", r"\1")
    return df


def split_rows_by_charges(df):
    i = 0
    for idx, row in df.iterrows():
        if pd.isna(row.charge):
            continue
        parts = row.charge.split("#")[1:]
        df = duplicate_row(df, idx+i, len(parts))
        for j, p in enumerate(parts):
            df.loc[idx+i+j, "charge"] = re.sub(
                r" \(.+\)$", "",
                p.lower().strip().replace(": ", " ").replace("n/a", "")
            )
        i += len(parts)-1
    return df


def extract_rule_violation(df):
    lookup_table = [
        ['2:15 conduct unbecoming of an officer', '2:15 conduct unbecoming an officer',
            '2:15 conduer unbecoming of an officer'],
        ['112/2 command of temper'],
        ['3:11 carrying out orders'],
        ['122 departmental motor vehicle accident', '123 departmental motor vehicle accident',
            '122 departmental vehicle accident', 'departmental motor vehicle accident'],
        ['2:21 use of alcohol and controlled substance'],
    ]
    df.loc[:, "charge"] = df.charge.fillna("").str.replace(r"\s*(;|and)$", "")
    df = standardize_from_lookup_table(df, "charge", lookup_table)
    rules = df.charge.str.split(" ", n=1, expand=True)
    df.loc[:, "rule_code"] = rules.loc[:, 0].fillna("")
    df.loc[:, "rule_violation"] = rules.loc[:, 1].fillna("")
    df = df.drop(columns="charge")
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "Port Allen PD"
    return df


def assign_prod_year(df, year):
    df.loc[:, "data_production_year"] = year
    return df


def clean19():
    df = pd.read_csv(data_file_path("port_allen_pd/port_allen_cprr_2019.csv"))
    df = clean_column_names(df)
    df.columns = [
        'receive_date', 'rank_desc', 'first_name', 'last_name', 'badge_no', 'charge',
        'disposition', 'occur_date', 'occur_time', 'tracking_number']
    df = df.drop(index=df[df.tracking_number.isna()
                          ].index).reset_index(drop=True)
    df = df\
        .pipe(clean_names, ["first_name", "last_name"])\
        .pipe(standardize_desc_cols, ["rank_desc", "disposition"])\
        .pipe(standardize_rank_desc)\
        .pipe(clean_occur_time)\
        .pipe(clean_occur_date)\
        .pipe(clean_dates, ["receive_date", "occur_date"])\
        .pipe(split_rows_by_charges)\
        .pipe(clean_badge_no)\
        .pipe(extract_rule_violation)\
        .pipe(combine_rule_columns)\
        .pipe(assign_agency)\
        .pipe(assign_prod_year, '2019')\
        .pipe(gen_uid, ["agency", "first_name", "last_name", "badge_no"])
    return df


def combine_rule_columns(df):
    df.loc[:, 'rule_code'] = df.rule_code.fillna('')
    df.loc[:, 'rule_violation'] = df.rule_violation.fillna('')

    def combine(row):
        return ' '.join(filter(None, [row.rule_code, row.rule_violation]))
    df.loc[:, 'charges'] = df.apply(combine, axis=1, result_type='reduce')
    df = df.drop(columns=['rule_code', 'rule_violation'])
    return df


def combine_appeal_and_action_columns(df):
    def combine(row):
        buf = list()
        if pd.notnull(row.action):
            buf.append(row.action)
        if pd.notnull(row.appeal):
            buf.append('appeal: %s' % row.appeal)
        if pd.notnull(row.hearing_date):
            buf.append('hearing date: %s' % row.hearing_date)
        if pd.notnull(row.disposition_appeal):
            buf.append('appeal disposition: %s' % row.disposition_appeal)
        return '; '.join(buf)
    df.loc[:, 'action'] = df.apply(combine, axis=1, result_type='reduce')
    df = df.drop(columns=['appeal', 'hearing_date', 'disposition_appeal'])
    return df


def clean18():
    df = pd.read_csv(data_file_path(
        "port_allen_pd/port_allen_cprr_2017-2018_byhand.csv"))
    df = clean_column_names(df)
    df = df.rename(columns={
        "case_number": "tracking_number",
        "date_notification": "notification_date",
        "title": "rank_desc",
        "f_name": "first_name",
        "l_name": "last_name",
        "disposition_investigation": "disposition",
        "complaintant_type": "complainant_type"
    })
    df = df.dropna(how="all")
    return df\
        .pipe(clean_dates, ["receive_date", "occur_date"])\
        .pipe(combine_rule_columns)\
        .pipe(combine_appeal_and_action_columns)\
        .pipe(assign_agency)\
        .pipe(assign_prod_year, '2018')\
        .pipe(gen_uid, ["agency", "first_name", "last_name"])


if __name__ == "__main__":
    df = clean19()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/cprr_port_allen_pd_2019.csv"),
        index=False)
