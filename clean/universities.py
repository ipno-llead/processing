import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir


def extract_lsu_roster(df):
    df = df[(df.agency.isin(['univ. pd - baton rouge cc', 'univ. pd - bossier parish cc',
                             'univ. pd - centenary', 'univ. pd - delgado cc',
                             'univ. pd - delta cc pd', 'univ. pd - dillard', 'univ. pd - grambling',
                             'univ. pd - holy cross', 'univ. pd - lsu - alexandria',
                             'univ. pd - lsu - eunice', 'univ. pd - lsuhsc - monroe',
                             'univ. pd - lsuhsc - no', 'univ. pd - lsuhsc',
                             'univ. pd - lsuhsc - shreveport',
                             'univ. pd - lsu', 'univ. pd - lsu - shreveport',
                             'univ. pd - la tech',
                             'univ. pd - loyola', 'univ. pd - mcneese',
                             'univ. pd - nicholls',
                             'univ. pd - nsu', 'univ. pd - northwestern',
                             'univ. pd - nunez cc', 'univ. pd - southeastern',
                             'univ. pd - southern - br', 'univ. pd - southern - no',
                             'univ. pd - southern', 'univ. pd - southern - shreveport',
                             'tulane univ pd', 'univ. pd - ull', 'univ. pd - ulm',
                             'univ. pd - uno', 'univ. pd - usl', 'univ. pd - xavier',
                             'univ. pd - louisiana college']))]
    return df


def clean_agency(df):
    df.loc[:, 'agency'] = df.agency\
        .str.replace('univ. pd - baton rouge cc', 'Baton Rouge Community College PD', regex=False)\
        .str.replace('univ. pd - bossier parish cc',
                     'Bossier Parish Community College PD', regex=False)\
        .str.replace('univ. pd - centenary',
                     'Centenary University PD', regex=False)\
        .str.replace('univ. pd - delgado cc',
                     'Delgado Commmunity College PD', regex=False)\
        .str.replace('univ. pd - delta cc pd',
                     'Delta Community College PD', regex=False)\
        .str.replace('univ. pd - dillard',
                     'Dillard University PD', regex=False)\
        .str.replace('univ. pd - grambling',
                     'Grambling University PD', regex=False)\
        .str.replace('univ. pd - holy cross',
                     'Holy Cross University PD', regex=False)\
        .str.replace('univ. pd - lsu - alexandria',
                     'Louisiana State University - Alexandria PD', regex=False)\
        .str.replace('univ. pd - lsu', 'Louisiana State University - Eunice PD', regex=False)\
        .str.replace('univ. pd - lsu',
                     'Louisiana State University Health Sciences Center - Monroe PD', regex=False)\
        .str.replace('univ. pd - lsu',
                     'Louisiana State University Health Sciences Center - New Orleans PD',
                     regex=False)\
        .str.replace('univ. pd - lsu',
                     'Louisiana State University Health Sciences Center PD', regex=False)\
        .str.replace('univ. pd - lsuhsc - shreveport',
                     'Louisiana State University Health Services Center - Shreveport PD', regex=False)\
        .str.replace('univ. pd - lsu', 'Louisiana State University PD', regex=False)\
        .str.replace('univ. pd - la tech',
                     'Louisiana Technical University PD', regex=False)\
        .str.replace('univ. pd - loyola',
                     'Loyola University PD', regex=False)\
        .str.replace('univ. pd - mcneese', 'McNeese University PD', regex=False)\
        .str.replace('univ. pd - nicholls', 'Nicholls University PD', regex=False)\
        .str.replace('univ. pd - nsu', 'Northwestern State University PD', regex=False)\
        .str.replace('univ. pd - northwestern', 'Northwestern University PD', regex=False)\
        .str.replace('univ. pd - nunez cc', 'Nunez Community College PD', regex=False)\
        .str.replace('univ. pd - southeastern', 'Southeastern University PD', regex=False)\
        .str.replace('univ. pd - southern - br', 'Southern University - Baton Rouge PD', regex=False)\
        .str.replace('univ. pd - southern - no', 'Southern University - New Orleans PD', regex=False)\
        .str.replace('univ. pd - southern', 'southern university police department', regex=False)\
        .str.replace('univ. pd - southern - shreveport',
                     'Southern University - Shreveport PD', regex=False)\
        .str.replace('tulane univ pd', 'Tulane University PD', regex=False)\
        .str.replace('univ. pd - ull',
                     'University of Louisiana at Lafayette PD', regex=False)\
        .str.replace('univ. pd - ulm',
                     'University of Louisiana Monroe PD', regex=False)\
        .str.replace('univ. pd - uno',
                     'University of New Orleans PD', regex=False)\
        .str.replace('univ. pd - usl',
                     'University of Southwestern Louisiana PD', regex=False)\
        .str.replace('univ. pd - xavier',
                     'Xavier University PD', regex=False)\
        .str.replace('univ. pd - louisiana college', 
                     'Louisiana College PD', regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/universities/pprr_post_2020_11_06.csv'))
    df = df\
        .pipe(extract_lsu_roster)\
        .pipe(clean_agency)
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(data_file_path('clean/pprr_universities.csv'), index=False)
