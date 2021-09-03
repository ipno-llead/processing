from lib.columns import clean_column_names
from lib.uid import gen_uid
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_name, parse_dates_with_known_format, standardize_desc_cols, clean_ranks, clean_names
)
import pandas as pd
import numpy as np
import re
import os
import sys
sys.path.append("../")


def read_csv_files():
    prefix = data_file_path("raw/new_orleans_pd")
    pat = re.compile(r".+_\d{4}\.csv$")
    csv_files = [name for name in os.listdir(
        prefix) if pat.match(name) is not None]
    return {name: pd.read_csv(os.path.join(prefix, name))
            for name in csv_files}


def extract_rank_map(matched_dfs):
    rank_codes = pd.read_csv(data_file_path(
        "raw/new_orleans_pd/job_code_description_list.csv"))
    rank_codes.columns = ["rank_code", "rank_desc"]

    rank_dfs = []
    for k, df in matched_dfs.items():
        rank_dfs.append(df[["rank_code", "rank_desc"]])

    df = pd.concat(rank_dfs + [rank_codes]).drop_duplicates().sort_values(
        "rank_code").set_index("rank_code", drop=True)
    return df.loc[(df.rank_desc != "POLICE OFFICER 1") & (df.rank_desc != "POLICE OFFICER 4")]\
        .rank_desc.str.lower().to_dict()


def match_schema(input_dfs):
    result = dict()
    for k, df in input_dfs.items():
        if k.endswith("_2018.csv"):
            result[k] = match_schema_2018(df)
        else:
            result[k] = match_schema_other_years(df)
    return result


def match_schema_other_years(orig_df):
    df = orig_df[["empl_id", "birthdate_personal_dta", "department_id_job_dta",
                  "job_code_job_dta", "description_job_code_info", "badge_nbr"]]
    df.columns = ["employee_id", "birth_year",
                  "department_code", "rank_code", "rank_desc", "badge_no"]
    name_df = orig_df["name_personal_dta"].str.split(",", expand=True)
    df.loc[:, "last_name"] = name_df.iloc[:, 0]
    name_df = name_df.iloc[:, 1].str.split(" ", expand=True)
    df.loc[:, "first_name"] = name_df.iloc[:, 0]
    df.loc[:, "middle_initial"] = name_df.iloc[:, 1]
    return df


def match_schema_2018(orig_df):
    df = orig_df[["empl_id", "last_name", "first_name", "middle_name", "home_orgn",
                  "orgn_desc", "title_code", "title_desc", "badge_number", "hire_date_employment_dta"]]
    df.columns = ["employee_id", "last_name", "first_name", "middle_name", "department_code",
                  "department_desc", "rank_code", "rank_desc", "badge_no", "hire_date"]
    return df


def normalize_2018(df):
    df.loc[:, "department_code"] = df.department_code.astype(
        "str").map(lambda x: "%s027%s" % (x[:2], x[2:])).astype("int64")
    df = parse_dates_with_known_format(df, ["hire_date"], "%m/%d/%Y")
    df.loc[:, "middle_initial"] = df.middle_name.map(
        lambda x: x if x == "" else x[0])
    df.loc[:, "middle_name"] = df.middle_name.where(
        df.middle_name.str.len() > 1, "")
    return df


def normalize_badge_no(df):
    if df.badge_no.dtype == np.float64:
        df.loc[:, "badge_no"] = df.badge_no.fillna(0).astype("int32")
    df.loc[:, "badge_no"] = df.badge_no.fillna("").astype("str")\
        .str.strip().str.rjust(5, "0")
    df.loc[:, "badge_no"] = df.badge_no.where(df.badge_no != "00000", "")
    return df


def clean_names(df):
    for col in ["last_name", "first_name", "middle_name", "middle_initial"]:
        if col not in df.columns:
            continue
        df.loc[:, col] = clean_name(df[col])
    return df


def assign_year_col(df, year):
    df.loc[:, "data_production_year"] = year
    return df


def assign_agency_col(df):
    df.loc[:, "agency"] = "New Orleans PD"
    return df


def normalize_rank_desc(df, rank_map):
    df.loc[:, "rank_desc"] = df["rank_code"].map(lambda x: rank_map[x])
    return df


def normalize_other_years(df):
    df.loc[:, "birth_year"] = df.birth_year.astype("str").fillna("")
    return df


def remove_pol_prefix_from_department(df):
    df.loc[:, "department_desc"] = df.department_desc.fillna(
        "").str.replace(r"^pol ", "")
    return df


def extract_rank(df):
    ranks = df.nominee.str.lower().str.strip()\
        .str.replace('\.', '', regex=False)\
        .str.replace('sgt', 'sergeant', regex=False)\
        .str.extract(r'(\(\w+/?\w+\)|special agent|atf agent|agent|detective|deputy|investigative analyst|'
                     r'mag unit crime analyst|task force officer|retired detective|supervisory special agent|'
                     r'group supervisor|supervisory special agent|s/t|tpr)')
    df.loc[:, 'rank_desc'] = ranks[0]
    return df


def extract_badge_no(df):
    badges = df.nominee.str.lower().str.strip()\
        .str.extract(r'(^\d+) ')
    df.loc[:, 'badge_no'] = badges[0]
    return df 


def split_award_nominee_name(df):
    df.loc[:, 'nominee'] = df.nominee.str.lower().str.strip()\
        .str.replace('-', '', regex=False)\
        .str.replace("'", '', regex=False)\
        .str.replace((r'(\(\w+/?\w+\)|special agent |atf agent |agent |detective |deputy |investigative analyst |'
                     r'mag unit crime analyst |task force officer |retired detective |supervisory special agent |'
                     r'group supervisor |supervisory special agent |s/t |tpr|sgt |mt |lt)'), '', regex=True)\
        .str.replace(r'(\d+)', '', regex=True)\
        .str.replace(r'(\w+) ?(\w+)? ?(\w+)?, (\w+) ?(\w+)?', r'\4 \5 \1 \2 \3', regex=True)\
        .str.replace(r'(\w+)  ? ? ?(\w+)', r'\1 \2', regex=True)\
        .str.replace('t sean mccaffery', 'sean t mccaffery', regex=False)\
        .str.replace(r'st (\w+)', r'st\1', regex=True)\
        .str.replace(r'mc (\w+)', r'mc\1', regex=True)\
        .str.replace('brownrobertson', 'brown robertson', regex=False)\
        .str.replace('lewiswilliams', 'lewis williams', regex=False)\
        .str.replace('jonesbrewer', 'jones brewer', regex=False)\
        .str.replace('martinbrown', 'martin brown', regex=False)\
        .str.replace('sanclementehaynes', ' sanclemente haynes', regex=False)\
        .str.replace('oquendojohnson', ' oquendo johnson', regex=False)\
        .str.replace('trooper', '', regex=False)
    names = df.nominee.str.lower().str.strip().str.extract(
        r'((\w+) (\w+ )?(.+))')
    df.loc[:, 'first_name'] = names[1]\
        .str.strip()
    df.loc[:, 'last_name'] = names[3]\
        .str.strip()
    df.loc[:, 'middle_name'] = names.loc[:, 2].str.strip().fillna('')\
        .map(lambda s: '' if len(s) < 2 else s)
    df.loc[:, 'middle_initial'] = names.loc[:, 2].str.strip().fillna('')\
        .map(lambda s: '' if len(s) > 1 else s)
    return df.drop(columns={'nominee'})


def split_award_nominator_name(df):
    df.loc[:, 'nominated_by'] = df.nominated_by.str.lower().str.strip()\
        .str.replace(' -', '', regex=False)\
        .str.replace(r'\.', '', regex=True)
    attributes = df.nominated_by.str.extract(r'(\d+) (\w+), (\w+) ?(\w{1})? (.+)')
    df.loc[:, 'nominator_badge_no'] = attributes[0]
    df.loc[:, 'nominator_last_name'] = attributes[1]
    df.loc[:, 'nominator_first_name'] = attributes[2]
    df.loc[:, 'nominator_middle_initial'] = attributes[3]
    df.loc[:, 'nominator_rank_desc'] = attributes[4].fillna('')\
        .str.replace(r'\(|\)', '', regex=True)\
        .str.replace(r'^sgt$', 'sergeant', regex=True)\
        .str.replace(r'^cpt$', 'captain', regex=True)\
        .str.replace(r'^lt$', 'lieutenant', regex=True)\
        .str.replace(r'^d$', 'detective', regex=True)\
        .str.replace(r'^s/ofc$', 'senior officer', regex=True)\
        .str.replace(r'^ofc$', 'officer', regex=True)\
        .str.replace(r'^r$', 'reserve', regex=True)\
        .str.replace(r'^c$', 'corporal', regex=True)
    return df.drop(columns={'nominated_by'})


def clean_decision(df):
    df.loc[:, 'award_decision'] = df.command_decision.str.lower().str.strip().fillna('')\
        .str.replace('approval', 'approved', regex=False)
    return df.drop(columns={'command_decision'})


def clean_recommended_award(df):
    df.loc[:, 'recommended_award'] = df.recommended_award.str.lower().str.strip().fillna('')\
        .str.replace(r'�s |� |� ', '', regex=True)\
        .str.replace('superintendentaward', 'superintendent award', regex=False)
    return df


def clean_award_disposition(df):
    df.loc[:, 'award_given'] = df.award_disposition.str.lower().str.strip().fillna('')\
        .str.replace(r'�s |� |� ', '', regex=True)\
        .str.replace('superintendentaward', 'superintendent award', regex=False)
    return df.drop(columns=('award_disposition'))


def drop_rows_where_award_recommended_and_given_is_empty(df):
    return df[~((df.award_decision == '') & (df.award_given == ''))]


def clean_award():
    df = pd.read_csv(data_file_path('raw/ipm/new_orleans_pd_commendations_2016_2021.csv'))
    df = df\
        .pipe(clean_column_names)\
        .drop(columns={
            'item_number'
        })\
        .rename(columns={
            'date': 'recommendation_date',
            'disposition_date': 'decision_date',
            'award_date': 'receive_date'
        })\
        .pipe(extract_rank)\
        .pipe(extract_badge_no)\
        .pipe(split_award_nominee_name)\
        .pipe(split_award_nominator_name)\
        .pipe(clean_decision)\
        .pipe(clean_ranks, ['rank_desc', 'nominator_rank_desc'])\
        .pipe(standardize_desc_cols, ['recommended_award', 'award_disposition'])\
        .pipe(clean_recommended_award)\
        .pipe(clean_award_disposition)\
        .pipe(drop_rows_where_award_recommended_and_given_is_empty)
    return df


def clean():
    result = dict()
    name_pat = re.compile(r".+_(\d{4})\.csv$")

    for filename, df in read_csv_files().items():
        year = name_pat.match(filename).group(1)
        if year == "2018":
            df = df\
                .pipe(clean_column_names)\
                .pipe(match_schema_2018)\
                .pipe(clean_names)\
                .pipe(assign_year_col, year)\
                .pipe(assign_agency_col)\
                .pipe(normalize_badge_no)\
                .pipe(gen_uid, ["employee_id"])\
                .pipe(standardize_desc_cols, ["department_desc"])\
                .pipe(remove_pol_prefix_from_department)\
                .pipe(normalize_2018)
        else:
            df = df\
                .pipe(clean_column_names)\
                .pipe(match_schema_other_years)\
                .pipe(clean_names)\
                .pipe(assign_year_col, year)\
                .pipe(assign_agency_col)\
                .pipe(normalize_badge_no)\
                .pipe(gen_uid, ["employee_id"])\
                .pipe(normalize_other_years)

        result[year] = df

    rank_map = extract_rank_map(result)

    for year, df in result.items():
        df = normalize_rank_desc(df, rank_map)
        result[year] = df

    combined_df = pd.concat(list(result.values()))
    return combined_df


if __name__ == "__main__":
    df = clean()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/pprr_new_orleans_pd.csv"),
        index=False)
