from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.path import data_file_path, ensure_data_dir
from lib.clean import (
    clean_names, standardize_desc_cols
)
import pandas as pd
import sys
sys.path.append('../')


def extract_date_from_pib(df):
    tracking = df['pib control#'].str.split("-", 1, expand=True)
    df.loc[:, 'receive_year'] = tracking.loc[:, 0].fillna('')
    df.loc[:, 'partial_tracking_number'] = tracking.loc[:, 1].fillna('')
    df.loc[:, 'tracking_number'] = df.receive_year.str.cat(df.partial_tracking_number, sep='-')
    df = df.drop(columns=['pib control#', 'partial_tracking_number'])
    return df


def combine_rule_and_paragraph(df):
    df.loc[:, 'charges'] = df['allegation classification'].str.cat(df.allegation, sep='; ')\
        .str.replace(r'^; ', '', regex=True).str.replace(r'; $', '', regex=True)
    df = df.drop(columns=['allegation classification', 'allegation'])
    return df

def clean_charges(df):
    df.loc[:, 'charges'] = df.charges.str.lower().str.strip()\
        .str.replace('rule : moral conduc', 'rule 2: moral conduct', regex=False)\
        .str.replace('rule 2:moral conduct', 'rule 2: moral conduct', regex=False)\
        .str.replace('rule moral conduct', 'rule 2: moral conduct', regex=False)\
        .str.replace('rule: 2: moral conduc', 'rule 2: moral conduct', regex=False)\
        .str.replace('rule: moral conduct', 'rule 2: moral conduct', regex=False)\
        .str.replace('rule2: moralconduct', 'rule 2: moral conduct', regex=False)
    return df

def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip() \
        .str.replace('-', ' | ', regex=False)\
        .str.replace('/', ' | ', regex=False)\
        .str.replace('rui', '', regex=False)\
        .str.replace('dismissal', 'dismissed', regex=False)\
        .str.replace('dui', 'driving under the influence', regex=False)\
        .str.replace('sustained dismissal overturned by 4th circuit',
                     'sustained dismissal overturned by the 4th circuit court', regex=False)\
        .str.replace('sustÃ¤ined', 'sustained', regex=False)
    return df


def clean_directive(df):
    df.loc[:, 'directive'] = df.directive.str.lower().str.strip()\
        .str.replace('\\', '', regex=False)\
        .str.replace('reltive ', 'relative', regex=False)\
        .str.replace('licene', 'license', regex=False)\
        .str.replace('r.s./', 'r.s. ', regex=False)\
        .str.replace('r.s.1', 'r.s. ', regex=False)\
        .str.replace("r.s.'", 'r.s. ', regex=False)\
        .str.replace('r.s:', 'r.s. ', regex=False)\
        .str.replace('la r.s.', 'la. r.s.')\
        .str.replace('la s.', 'la. r.s. ')\
        .str.replace('8attery', 'battery', regex=False)\
        .str.replace('vehivle', 'vehicle', regex=False)\
        .str.replace('drivers', "driver's", regex=False)\
        .str.replace('32:79-driving', '32:79 driving', regex=False)\
        .str.replace('relative public payroll fraud', 'relative to public payroll fraud', regex=False)\
        .str.replace('to la r.s.', 'to wit: la r.s.', regex=False)\
        .str.replace('relativeto', 'relative to', regex=False)\
        .str.replace('4.108.1 resisting an officer/obstruction',
                     '4:108.1 relative to resisting an officer/obstruction', regex=False)\
        .str.replace('to wit 14:98', 'to wit: r.s. 14:98 relative to operating a vehicle while intoxicated ', regex=False)\
        .str.replace('reckles', 'reckless', regex=False)\
        .str.replace(r'r.s. 14:35$', 'r.s. 14:35 relative to simple battery')\
        .str.replace('r.s 32:81 following vehicles', 'r.s. 32:81 relative to following vehicles', regex=False)\
        .str.replace('r.s. 32:58 careless operation',
                     'r.s. 32:58 relative to careless operation of a moveable', regex=False)\
        .str.replace('r.s 47-507', 'r.s. 47-507 relative to display of plate', regex=False)\
        .str.replace('r.s. 14.35.3', 'r.s. 14:35.3 relative to domestic abuse battery', regex=False)\
        .str.replace('to wit: hit ad run', 'to wit: r.s. 14:100 hit and run driving', regex=False)\
        .str.replace('32:863.1 no proof of liability insurance',
                     'r.s. 32:863.1 relative to no proof of liability insurance', regex=False)\
        .str.replace('to wit: simple battery', 'to wit: la. r.s. 14:35 relative to simple battery', regex=False)\
        .str.replace('to wit:14:98', 'to wit: r.s. 14:98 relative to operating a vehicle while intoxicated', regex=False)\
        .str.replace('to wit r.s. la 32:865', 'to wit: r.s. la 32:865')\
        .str.replace('to wit 32.863.1', 'to wit: r.s. 32:863.1')\
        .str.replace('to wit r.s. 4:80 relative to felony carnel knowledge of a juvenile',
                     'to wit: r.s. 4:80 relative to felony carnel knowledge of a juvenile', regex=False)\
        .str.replace('to wit r.s. 32:81 relative to following vehicles',
                    'to wit: r.s. 32:81 relative to following vehicles', regex=False)\
        .str.replace('to wit r.s. 32:58 relative to careless operation of a moveable',
                     'to wit: r.s. 32:58 relative to careless operation of a moveable', regex=False)\
        .str.replace('to wit 32.863.1', 'to wit 32.863.1', regex=False)\
        .str.replace('14:98 driving while intoxicated',
                     '14:98 relative to operating a vehicle while intoxicated', regex=False)\
        .str.replace('14:98.3 d.w.i 3rd offense',
                     'r.s. 14:98 relative to operating a vehicle while intoxicated (3rd offense)', regex=False)\
        .str.replace('14:100 hit and run driving', '14:100 relative to hit and run driving', regex=False)\
        .str.replace('14:133 filing or maintaining false public records',
                     '14:133 relative to filing or maintaining false public records', regex=False)\
        .str.replace('32:51 no vehicle license', '32:51 relative to no vehicle license', regex=False)\
        .str.replace('32:61 maximum speed limit', '32:61 relative to maximum speed limit', regex=False)\
        .str.replace('32:50 careless operation of a motor vehicle',
                     '32:50 relative to careless operation of a motor vehicle', regex=False)\
        .str.replace('14:138 public payroll fraud', '14:138 relative to public payroll fraud', regex=False)\
        .str.replace('14:38 simple assault and r.s. 14:63 criminal trespassing',
                     '14:38 relative to simple assault | r.s. 14:63 criminal trespassing', regex=False)\
        .str.replace('relative sexual battery', 'r.s. 14:43.1 relative to sexual battery')\
        .str.replace("to la. r.s. 32:52 relative to driver's license expired",
                     "to wit: la. r.s. 32:52 relative to driver's license expired", regex=False)\
        .str.replace(r'^relative to theft$', 'r.s. 14:67 relative to theft')\
        .str.replace('relative to aggravated incest', 'r.s. 14:78.1 relative to aggravated incest', regex=False)\
        .str.replace('relative to attempt and conspiracy to commit offense',
                     '14:26 relative to attempt and conspiracy to commit offense', regex=False)\
        .str.replace('relative to conspiracy to commit offense',
                     '14:26 relative to attempt and conspiracy to commit offense', regex=False)\
        .str.replace('relative to forcible rape', '14:42.1 relative to forcible rape', regex=False)\
        .str.replace('relative to no proof of insurance',
                     'r.s. 32:863.1 relative to no proof of liability insurance', regex=False)\
        .str.replace(r'^relative to simple battery$', 'r.s. 14:35 relative to simple battery')\
        .str.replace(r'^relative to simple assault$', 'r.s. 14:38 relative to simple assault')\
        .str.replace(r'^simple assault$', 'r.s. 14:38 relative to simple assault')\
        .str.replace('r.s. 14:99 relative to reckless operation of a vehicle',
                     'r.s. 14:99 relative to reckless operation of a motor vehicle', regex=False)\
        .str.replace('recklesss', 'reckless', regex=False)\
        .str.replace("r.s. 32:412 driver's must be licensed", "r.s. 32:412 drivers must be licensed", regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path(
        'new_orleans_da/new_orleans_da_cprr_2021.csv'))
    df = clean_column_names(df)
    df.columns = ['pib control#', 'first name', 'last name', 'allegation classification', 'allegation',
                  'directive', 'finding', 'disposition']
    df = df\
        .rename(columns={
            'first name': 'first_name',
            'last name': 'last_name'
        })\
        .pipe(extract_date_from_pib)\
        .pipe(combine_rule_and_paragraph)\
        .pipe(clean_disposition)\
        .pipe(clean_directive)\
        .pipe(clean_charges)\
        .pipe(standardize_desc_cols, ['directive', 'disposition', 'charges'])\
        .pipe(clean_names, ['first_name', 'last_name'])\
        .pipe(set_values, {
            'data_production_year': 2021,
            'agency': 'New Orleans DA'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'last_name'])
    return df


if __name__ == '__main__':
    df = clean()
    ensure_data_dir('clean')
    df.to_csv(
        data_file_path('clean/cprr_new_orleans_da_2021.csv'),
        index=False)
