from lib.columns import clean_column_names
from lib.uid import gen_uid
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, standardize_desc_cols, clean_dates, clean_sexes, clean_races, clean_datetimes
)
import pandas as pd
import sys
sys.path.append("../")


def split_name(df):
    names1 = df.name.str.strip().str.replace(
        r"^(\w+(?: \w\.)?) (\w+(?:, \w{2}\.)?)$", r"\1@@\2", regex=False).str.split("@@", expand=True)
    names2 = names1.iloc[:, 0].str.split(" ", expand=True)
    df.loc[:, "first_name"] = names2.iloc[:, 0]
    df.loc[:, "middle_initial"] = names2.iloc[:, 1]
    df.loc[:, "last_name"] = names1.iloc[:, 0]
    df = df.drop(columns=["name"])
    return df


def clean_action(df):
    df.loc[:, 'action'] = df.action.str.lower().str.strip()\
        .str.replace('.', '', regex=False)\
        .str.replace(r"(\. |, | and )", " | ", regex=False).str.replace(r"\.$", "", regex=False)\
        .str.replace(r"privlie?d?ges", "privileges", regex=True)\
        .str.replace("demotion to cpl to deputy and suspended for 7 days",
                     "7-day suspension/demotion from corporal to deputy", regex=False)\
        .str.replace("suspended five days and the loss of take home vehicle privileges for 60 days.",
                     "5-day suspension/loss of take home vehicle privileges for 60 days", regex=False)\
        .str.replace(r"\badmin\b", "administration", regex=True)\
        .str.replace("none.", "none", regex=False)\
        .str.replace("resignation", "resigned", regex=False)\
        .str.replace("correctrions", "corrections", regex=False)\
        .str.replace("prev.", "privileges", regex=False)\
        .str.replace("Verbally reprimanded", "verbal reprimand", regex=False)\
        .str.replace("one day suspension", "1-day suspension", regex=False)\
        .str.replace("one-day suspension", "1-day suspension", regex=False)\
        .str.replace("suspended two days", "2-day suspension", regex=False)\
        .str.replace("suspended without pay for two days", 
                     "2-day suspension without pay", regex=False)\
        .str.replace("seven day suspension.", "7-day suspension", regex=False)\
        .str.replace("3 day suspension", "3-day suspension", regex=False)\
        .str.replace("10 day suspension", "10-day suspension", regex=False)\
        .str.replace("30 day suspension", "30-day suspension", regex=False)\
        .str.replace("7 day suspension", "7-day suspension", regex=False)\
        .str.replace("5 day suspension", "5-day suspension", regex=False)\
        .str.replace("2 week suspension", "2-week suspension", regex=False)\
        .str.replace("suspended one day and loss of take home vehicle privileges for 30 days",
                     "1-day suspension/loss of take home vehicle privileges for 30 days", regex=False)\
        .str.replace("10 day suspension 30 day loss of extra duty detail privelages",
                     "10-day suspension/30-day loss of extra duty detail privileges", regex=False)\
        .str.replace("loss of take home vehicle privileges for 60 days", 
                     "60-day loss of take home vehicle privileges", regex=False)\
        .str.replace("loss of take of home vehicle privileges for 20 days",
                     "20-day loss of take of home vehicle privileges", regex=False)\
        .str.replace('none', '', regex=False)\
        .str.replace('lt harris received a two week suspension and was demoted to sgt',
                     '2-week suspensionde/demotion', regex=False)\
        .str.replace('seven day suspension', '7-day suspension', regex=False)\
        .str.replace('suspension loss of unit prev', 'suspension/loss of unit', regex=False)\
        .str.replace('incident investigated and forwarded to '
                     'ebrso administration for discipline outcome', 
                     'forwarded to administration for review', regex=False)\
        .str.replace('transferred to corrections', 'forwarded to corrections', regex=False)\
        .str.replace('no action taken', '', regex=False)\
        .str.replace(r'^terminated$', 'termination', regex=True)\
        .str.replace('2-week suspension and diversity class', 
                     '2-week suspension/diversity class', regex=False)\
        .str.replace('turned over to administration', 
                     'forwarded to administration for review', regex=False)\
        .str.replace('counseled and warned by capatin flynn', 
                     'counseled and warned', regex=False)\
        .str.replace('3-day suspension and not eligible to work blue bayou in the future', 
                     '3-day suspension/no longer eligible to work blue bayou', regex=False)\
        .str.replace('counseled/cautioned verbally', 'counseled/verbal caution', regex=False)\
        .str.replace(r' (\w+) (\d+) ', r'\1/\2', regex=True)\
        .str.replace(r' (\d{2})(\w{3})', r' \1-\2', regex=True)\
        .str.replace(r'capa?ti?n?', 'captain', regex=True)\
        .str.replace(r'\bia\b', 'internal affairs', regex=True)\
        .str.replace('days', 'day', regex=False)\
        .str.replace('privelages', 'privileges', regex=False)\
        .str.replace('suspension permanent removal from appledetail', 
                     'suspension/permanent removal from apple detail', regex=False)\
        .str.replace(r' \bdiversity class\b', ' diversity/cultural sensitivity class', regex=True)\
        .str.replace('10-dayuspension', '10-day suspension', regex=False)\
        .str.replace('suspensionde', 'suspension', regex=False)\
        .str.replace(r'(\d{2})(\w+)', r'\1-\2', regex=True)\
        .str.replace(' and ', '/', regex=False)\
        .str.replace('by', 'from', regex=True)\
        .str.replace('suspended five day/the 60-day loss of take home vehicle privileges',
                     '5-day suspension/60-day loss of take home vehicle privileges', regex=False)\
        .str.replace('verbal warning from captain andrew stevens/'
                     'also captain flynn in internal affairs afterwards',
                     'verbal warning from captain andrew '
                     'stevens/verbal warning from captain flynn', regex=False)\
        .str.replace('loss of take of home vehicle priviledgesfor/20-day', 
                     '30-day loss of take home vehicle privileges', regex=False)\
        .str.replace('captainain', 'captain', regex=False)\
        .str.replace('7-dayuspension/6month', '7-day suspension/6-month', regex=False)\
        .str.replace(r',  ?(\w+)', r'/\1', regex=True)\
        .str.replace(' (other than captain flynn speaking to blackwood/his supervisors)', '', regex=False)\
        .str.replace('terminetd', 'terminated', regex=False)\
        .str.replace(' note no report see phone log', '', regex=False)\
        .str.replace('due to other/multiple complaints - '
                     '30-day loss of take home vehicle privileges',
                     '30-day loss of take home vehicle privileges '
                     'due to multiple other complaints', regex=False)\
        .str.replace('loss of take home vehicle privilegesfor/30-day', 
                     '30-day loss of take home vehicle privileges', regex=False)
    return df


def clean_complainant(df):
    df.loc[:, "complainant_type"] = df.complainant_type.str.lower().str.strip()\
        .str.replace("brpd detective", 
                     "baton rouge police department detective", regex=False)\
        .str.replace("deer park texas pd", 
                     'deer park texas police department', regex=False)\
        .str.replace("ebrso administration and brpd",
                     "administration and baton rouge police department", regex=False)\
        .str.replace("administration (see also 17-19)", "administration", regex=False)\
        .str.replace('coworker', 'co-worker', regex=False)\
        .str.replace('lsp ia', 'louisiana state police internal affairs', regex=False)\
        .str.replace('lsu pd', 'louisiana state university police department', regex=False)
    return df


def split_infraction(df):
    infractions = df.infraction.str.extract(r"^([A-Za-z ,]+)(\d.+)?$")
    df.loc[:, 'allegation'] = infractions.iloc[:, 1].fillna('').str.strip()\
        .str.replace(r"-(\d+)$", r".\1", regex=False)\
        .str.cat(
            infractions.iloc[:, 0].str.strip(),
            sep=': ')\
        .str.replace(r'^ - ', '', regex=False)
    df = df.drop(columns=["infraction"])
    return df


def clean_allegations(df):
    df.loc[:, 'allegation'] = df.allegation.str.lower().str.strip() \
        .str.replace(r'^([^\s]*)\s+', "", regex=False)\
        .str.replace(r'^(\d+)- (\d+)', r'\1-\2', regex=True)\
        .str.replace(r'^1', '01', regex=True)\
        .str.replace('-1', '01', regex=False)\
        .str.replace(r'01-01\.?1?4? - unsatisfactory  ?performanceb?d?',
                     '01-01.14 - unsatisfactory performance', regex=True)\
        .str.replace(r'^(\d{1})(\d{1})(\d{1})(\d{1})', r'\1\2-\3\4', regex=True)\
        .str.replace(r'01-0?d?1\.05? - courtesye?', '01-01.05 - courtesty', regex=True)\
        .str.replace('- other', '', regex=False)\
        .str.replace('offorce', 'of force', regex=False)\
        .str.replace('informationid', 'information', regex=False)\
        .str.replace(r'^- ', '', regex=True)\
        .str.replace(r'(\d+)-(\d+)-(\d+)', r'\1-\2.\3', regex=True)\
        .str.replace(r'^: ', '', regex=True)
    return df


def clean_rank_desc_20(df):
    df.loc[:, 'rank_desc'] = df.rank_desc\
        .str.replace('reserves', 'reserve', regex=False)\
        .str.replace(' 1', '', regex=False)
    return df


def clean_department_desc(df):
    df.loc[:, 'department_desc'] = df.department_desc\
        .str.replace('.', '', regex=False)\
        .str.replace('admin', 'administration', regex=False)\
        .str.replace('sub', 'substation', regex=False)\
        .str.replace('kleintepeter', 'kleinpeter', regex=False)\
        .str.replace('detectives', 'criminal investigations', regex=False)\
        .str.replace('uniform ', '', regex=False)\
        .str.replace(r'\(|\)', '', regex=True)
    return df


def clean_disposition_20(df):
    df.loc[:, 'disposition'] = df.disposition\
        .str.replace('o unfounded', 'unfounded', regex=False)
    return df 


def clean_birth_year_20(df):
    df.loc[:, 'birth_year'] = df.birth_year.astype(str)\
        .str.replace(r'(\d{2})', r'19\1', regex=True)
    return df 


def assign_agency(df):
    df.loc[:, "agency"] = "Baton Rouge SO"
    return df


def assign_prod_year(df, year):
    df.loc[:, "data_production_year"] = year
    return df


def clean18():
    df = pd.read_csv(data_file_path(
        "raw/baton_rouge_so/baton_rouge_so_cprr_2018.csv"))
    df = clean_column_names(df)
    df.columns = ['name', 'badge_no', 'rank_desc', 'rank_date', 'race', 'sex',
                  'birth_year', 'infraction',
                  'occur_datetime',
                  'complainant_type',
                  'disposition', 'action', 'department_desc']
    df = df\
        .pipe(split_name)\
        .pipe(split_infraction)\
        .pipe(
            standardize_desc_cols,
            ["rank_desc", "disposition", "complainant_type", "department_desc", "allegation"])\
        .pipe(clean_dates, ["rank_date"])\
        .pipe(clean_races, ["race"])\
        .pipe(clean_sexes, ["sex"])\
        .pipe(clean_datetimes, ["occur_datetime"])\
        .pipe(clean_action)\
        .pipe(clean_complainant)\
        .pipe(clean_department_desc)\
        .pipe(clean_allegations)\
        .pipe(assign_agency)\
        .pipe(assign_prod_year, '2018')\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(gen_uid, ["agency", "first_name", "last_name", "birth_year", "badge_no"])\
        .pipe(gen_uid, ['agency', 'uid', 'occur_year', 'occur_month', 'occur_day'], 'complaint_uid')
    return df


def clean20():
    df = pd.read_csv(data_file_path(
        "raw/baton_rouge_so/baton_rouge_so_cprr_2016-2020.csv"))
    df = clean_column_names(df)
    df.columns = ['tracking_number', 'name', 'badge_no', 'rank_desc', 'rank_date', 'race', 'sex',
                  'birth_year', "department_desc", 'infraction',
                  'occur_datetime',
                  'complainant_type',
                  'disposition', 'action']
    df = df\
        .pipe(split_name)\
        .pipe(split_infraction)\
        .pipe(
            standardize_desc_cols,
            ['rank_desc', 'disposition', 'complainant_type', 'department_desc', 'allegation'])\
        .pipe(clean_dates, ['rank_date'])\
        .pipe(clean_races, ['race'])\
        .pipe(clean_sexes, ['sex'])\
        .pipe(clean_datetimes, ['occur_datetime'])\
        .pipe(clean_action)\
        .pipe(clean_allegations)\
        .pipe(clean_rank_desc_20)\
        .pipe(clean_birth_year_20)\
        .pipe(clean_department_desc)\
        .pipe(assign_agency)\
        .pipe(clean_complainant)\
        .pipe(clean_disposition_20)\
        .pipe(assign_prod_year, '2020')\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(gen_uid, ["agency", "first_name", "last_name", "birth_year", "badge_no"])\
        .pipe(gen_uid, ['agency', 'uid', 'occur_year', 'occur_month', 'occur_day'], 'complaint_uid')
    return df


if __name__ == "__main__":
    df18 = clean18()
    df20 = clean20()
    ensure_data_dir("clean")
    df18.to_csv(
        data_file_path("clean/cprr_baton_rouge_so_2018.csv"),
        index=False)
    df20.to_csv(
        data_file_path("clean/cprr_baton_rouge_so_2016_2020.csv"),
        index=False)
