import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path


def extract_da_rosters(df):
    df = df[(df.agency.isin(['22nd district attorney', '21st da office',
                             '14th jdc da office', '17th da office', '19th da office', '25th da office',
                             '25th jdc', "29th jdc da's office", '2nd da office', '3rd jdc', '40th jdc',
                             '42nd jdc', '4th da office', '4th jdc', "5th jdc district attorney's office", 
                             'allen da office', 'attorney generals office', 'caldwell da office',
                             'east baton rouge da office', 'iberia da office', 'orleans da office',
                             'ouachita da office', 'st landry da office', 'st tammany da office', 
                             'tangipahoa da office', '11th da office']))]
    return df


def clean_agency(df):
    df.loc[:, 'agency'] = df.agency\
        .str.replace('22nd district attorney', "22nd District Attorney's Office", regex=False)\
        .str.replace('21st da office', "21st District Attorney's Office", regex=False)\
        .str.replace('14th jdc da office', "14th District Attorney's Office", regex=False)\
        .str.replace('17th da office', "17th District Attorney's Office", regex=False)\
        .str.replace('19th da office', "19th District Attorney's Office", regex=False)\
        .str.replace(r'25th (da office|jdc)', "25th District Attorney's Office", regex=True)\
        .str.replace("29th jdc da's office", "29th District Attorney's Office", regex=False)\
        .str.replace('2nd da office', "2nd District Attorney's Office", regex=False)\
        .str.replace('3rd jdc', "3rd District Attorney's Office", regex=False)\
        .str.replace('40th jdc', "40th District Attorney's Office", regex=False)\
        .str.replace('42nd jdc', "42nd District Attorney's Office", regex=False)\
        .str.replace(r'4th (da office|jdc)', "4th District Attorney's Office", regex=True)\
        .str.replace("5th jdc district attorney's office", "5th District Attorney's Office", regex=False)\
        .str.replace('allen da office', "33rd District Attorney's Office", regex=False)\
        .str.replace('attorney generals office', "Attorney General's Office", regex=False)\
        .str.replace('caldwell da office', "37th District Attorney's Office", regex=False)\
        .str.replace('east baton rouge da office', "19th District Attorney's office", regex=False)\
        .str.replace('iberia da office', "16th District ATtorney's Office", regex=False)\
        .str.replace('orleans da office', "Orleans District Attorney's Office", regex=False)\
        .str.replace('ouachita da office', "4th District Attorney's Office", regex=False)\
        .str.replace('st landry da office', "27th District Attorney's Office", regex=False)\
        .str.replace('st tammany da office', "22nd District Attorney's Office", regex=False)\
        .str.replace('tangipahoa da office', "21st District Attorney's Office", regex=False)\
        .str.replace('11th da office', "11th District Attorney's Office", regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/district_attorney/pprr_post_2020_11_06.csv'))
    df = df\
        .pipe(extract_da_rosters)\
        .pipe(clean_agency)
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/pprr_da_offices.csv'), index=False)
