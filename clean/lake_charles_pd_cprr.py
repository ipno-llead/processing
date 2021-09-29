import sys
sys.path.append('../')
import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.clean import float_to_int_str, clean_dates
from lib.columns import clean_column_names
from lib.rows import duplicate_row
from lib.uid import gen_uid
from lib.standardize import standardize_from_lookup_table
import re


disposition_lookup = [
    ['exonerated', 'closeexhor', 'close/exhan'],
    ['sustained/resigned', 'sust/r', 'sust//r', 'sustr/r',
     'sust /r', 'sustr', 'bust//r', 'sustr the',
     'sustresign', 'sust/', 'sust//', 'bust/'],
    ['resigned', 'name /resigne', 'resigne', 'susp/resign',
     'clogul resigne', 'kesigne', '/r', '. term resign', 'close resisne'],
    ['no investigation merited', 'no invest,', 'no. invest.'],
    ['sustained', 'syst', 'ast/', 'sust /', 'syst/',
     'sast /', 'sust tyr', 'elfet sust/this an', 'sust',
     'susth', 'sust/aya', 'sustflo', 'sust//eap', 'sustl',
     'sast./2days', 'sast. /w'],
    ['sustained/exonerated', 'sust exoa', 'erhon/yst'],
    ['sustained/resigned', 'singe/resign', 'sustained; resigned'],
    ['unfounded', 'unfoune'],
    ['exonerated', 'examprate', 'exomerate', 'exopplate',
     'exhoreate', 'exhonerate', 'exhonesete',
     'exhone rate', 'excenerate', 'experate', 'estay', 'eranente',
     'examerate', 'exomenate', 'exonepte',
     'exokerate', 'exon / unformal'],
    ['not justified', 'notjustified', 'not just.'],
    ['justified', 'justified!', 'justified', 'just. 12day', 'just, (2day',
     'just.,lld', 'just wr', 'just./vr', 'just! vr', '3ust//3day', 'just /wr',
     'just/ vr', 'just/wr', 'just/7day', 'just ', 'just. bday'],
    ['justified/resigned', 'justified/r', 'justifiedb', 'justifiedll', 'justifiedresigne'],
    ['no fault', 'no fnest'],
    ['not sustained', 'not sust', 'not sast']]


def clean_charges(df):
    df.loc[:, 'charges'] = df.nature_of_complaint.str.lower().str.strip()\
        .str.replace(r'\. ?', ' ', regex=True)\
        .str.replace(',', '', regex=False)\
        .str.replace(r'(\d+) ', '', regex=True)\
        .str.replace('unsat perform', 'unsatisfactory performance', regex=False)\
        .str.replace('neg of duty', 'neglect of duty', regex=False)\
        .str.replace(r'cond unbecom(ing)?', 'conduct unbecoming', regex=True)\
        .str.replace('unauth force', 'unauthorized force', regex=False)\
        .str.replace('use of dept equip', 'use of department equipment', regex=False)\
        .str.replace('unknown', '', regex=False)
    return df.drop(columns='nature_of_complaint')


def split_rows_with_multiple_charges(df):
    i = 0
    for idx in df[df.charges.str.contains(r'/')].index:
        s = df.loc[idx + i, 'charges']
        parts = re.split(r'\s*(?:/)\s*', s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, 'charges'] = name
        i += len(parts) - 1
    return df


def clean_action(df):
    df.loc[:, 'action'] = df.action_taken.str.lower().str.strip()\
        .str.replace(r'no action ?(tak[eo]n)?', '', regex=True)
    return df.drop(columns=('action_taken'))


def consolidate_action_and_disposition(df):
    df.loc[:, 'action'] = df.action.str.cat(df.disposition, sep='|')\
        .str.replace(r'((not)? ?sustained|exonerated|unfounded|invalid complaint) ?', '', regex=True)\
        .str.replace(r'^\|', '', regex=True)\
        .str.replace(r'\|$', '', regex=True)\
        .str.replace(r'(\d+) (\w+)', r'\1-\2', regex=True)
    return df


def clean_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip()\
        .str.replace('invalid complaint', '', regex=False)\
        .str.replace('suspended', '', regex=False)
    return df


def split_rows_with_multiple_officers(df):
    i = 0
    for idx in df[df.name.str.contains(r"/|,")].index:
        s = df.loc[idx + i, "name"]
        parts = re.split(r"\s*(?:/|,)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "name"] = name
        i += len(parts) - 1
    return df


def drop_rows_missing_disp_charges_and_action(df):
    return df[~((df.disposition == '') & (df.charges == '') & (df.action == ''))]


def assign_empty_first_name_column(df):
    df.loc[:, 'first_name'] = ''
    return df


def review_first_names_from_post():
    df = pd.read_csv(data_file_path('match/lake_charles_pd_cprr_2020_extracted_first_names.csv'))
    df = df\
        .pipe(clean_column_names)
    return df


def assign_first_names_from_post(df):
    df.loc[:, 'name'] = df.name.str.lower().str.strip()\
        .str.replace('torres', 'torres paul', regex=False)\
        .str.replace('redd', 'redd jeffrey', regex=False)\
        .str.replace('romero', 'romero mayo', regex=False)\
        .str.replace('nevels', 'nevels harold', regex=False)\
        .str.replace('manuel', 'manuel carlos', regex=False)\
        .str.replace('morrow', 'morrow errel', regex=False)\
        .str.replace('mccloskey', 'mccloskey john', regex=False)\
        .str.replace('myers', 'myers hannah', regex=False)\
        .str.replace('mccue', 'mccue eddie', regex=False)\
        .str.replace('mills', 'mills logan', regex=False)\
        .str.replace('dougay', 'dougay bennon', regex=False)\
        .str.replace('saunier', 'saunier john', regex=False)\
        .str.replace('ewing', 'ewing joshua', regex=False)\
        .str.replace('johnson', 'johnson martin', regex=False)\
        .str.replace('jackson', 'jackson princeton', regex=False)\
        .str.replace('baccigalopi', 'baccigalopi dakota', regex=False)\
        .str.replace('breaux', 'breaux keithen', regex=False)\
        .str.replace('falcon', 'falcon bendy', regex=False)\
        .str.replace('ford', 'ford raymond', regex=False)\
        .str.replace('perkins', 'perkins carlton', regex=False)\
        .str.replace('ponthieaux', 'ponthieaux wilbert', regex=False)\
        .str.replace('markham', 'markham alan', regex=False)
    names = df.name.str.extract(r'(\w+) ?(.+)?')
    df.loc[:, 'last_name'] = names[0]
    df.loc[:, 'first_name'] = names[1].fillna('')
    return df.drop(columns='name')


def assign_agency(df):
    df.loc[:, "agency"] = "Lake Charles PD"
    return df


def clean_tracking_number_19(df):
    df.loc[:, 'tracking_number'] = df.iad_file\
        .str.replace(' ', '', regex=False)\
        .str.replace(',', '', regex=False)\
        .str.replace(r':|\.', '-', regex=True)\
        .str.replace(r'(\d{1})(\d{1})(\d{1})', r'\1\2-\3', regex=True)
    return df.drop(columns='iad_file')


def clean_complainant_19(df):
    df.loc[:, 'complainant'] = df.complainant_s.str.lower().str.strip().fillna('')\
        .str.replace(r'\blcps\b', "lake charles parish sheriff's office", regex=True)\
        .str.replace(r'\blcpd\b', 'lake charles police department', regex=True)
    return df.drop(columns='complainant_s')


def extract_rank_from_name_19(df):
    ranks = df.officer_s_accused.str.lower().str.strip()\
        .str.extract(r'(cpl|sgt|pt|p.o|e.o|e.p|cal|ca)')
    df.loc[:, 'rank_desc'] = ranks[0].fillna('')\
        .str.replace('p.o', 'parole officer', regex=False)\
        .str.replace('cpl', 'corporal', regex=False)\
        .str.replace('sgt', 'sergeant', regex=False)
    return df


def split_name_19(df):
    df.loc[:, 'officer_s_accused'] = df.officer_s_accused.str.lower().str.strip().fillna('')\
        .str.replace(r'cp[gl] ?|sgt ?|pt ?|p.o ?|e.o ?|e.p ?|cal ?|ca ?|e.o.l ?|(\d+) ?'
                     r'|, ?||& ?|unf?k?nown?l?y? ?|/ ?| ?- ?', '', regex=True)\
        .str.replace(r'^(\w{1})$', '', regex=True)\
        .str.replace('stickell stickell', 'stickell', regex=False)\
        .str.replace('pd', '', regex=False)\
        .str.replace('for ', '', regex=False)\
        .str.replace('kwashington', 'k. washington', regex=False)\
        .str.replace(r'riveraalicea|rivesa alecie|riveraalecia', 'rivera-alecia', regex=True)\
        .str.replace(r'^\. ?', '', regex=True)\
        .str.replace(r'(\w+)\.(\w+)', r'\1 \2', regex=True)\
        .str.replace(r'^(\w{1})\.? (\w+)\'?(\w+)?$', r'\2 \1', regex=True)\
        .str.replace('lmanual', 'manual l', regex=False)\
        .str.replace(r'^clouse$', 'clouse s', regex=True)\
        .str.replace(r'^an hunter$', 'hunter a', regex=True)\
        .str.replace('kyoung', 'young k', regex=False)

    names = df.officer_s_accused.str.extract(r'(\w{1}) ?(\w+)? ?(\w+)?')

    return df


def clean_investigation_start_date_19(df):
    df.loc[:, 'investigation_start_date'] = df.date\
        .str.replace('-', '/', regex=False)\
        .str.replace('.', '/', regex=False)\
        .str.replace('9817', '', regex=False)\
        .str.replace('412', '4/12', regex=False)\
        .str.replace(r'(\w+)/(\d+)', '', regex=True)\
        .str.replace('48/19', '4/8/2019', regex=False)\
        .str.replace('3716', '3/17/2016', regex=False)\
        .str.replace('5/416', '5/4/2016', regex=False)\
        .str.replace(r'^/(\d+)', '', regex=True)
    return df.drop(columns='date')


def clean_charges_19(df):
    df.loc[:, 'charges'] = df.complaint.str.lower().str.strip()\
        .str.replace('  ', ' ', regex=False)\
        .str.replace('if', 'of', regex=False)\
        .str.replace(r'\.', '', regex=True)\
        .str.replace(r', ?', '', regex=True)\
        .str.replace(r'f?i?e?leet/? f?c?i?r?a?g?a?s?hy?t?', 'fleet crash', regex=True)\
        .str.replace(r'fa[ls]e', 'false', regex=True)\
        .str.replace(r'(unhacancy|hopcoring|unbecom)', 'unbecoming', regex=True)\
        .str.replace(r'un?g?h?ss?ae?l?f?t?i?y?\b', 'unsatisfactory', regex=True)\
        .str.replace('viplence', 'violance', regex=False)\
        .str.replace('chaindcom', 'chain of command', regex=False)\
        .str.replace(r'perf?o?r?m', 'performance', regex=False)\
        .str.replace('ha passment', 'harrassment', regex=False)\
        .str.replace(r'destequip|equit', 'department equipment', regex=True)\
        .str.replace(r'profes?s?i?o?n?a?l?i?s?t?y?', 'professionalism', regex=True)\
        .str.replace(r'insubordination \(2cts\)', 'two counts of insubordination', regex=True)\
        .str.replace('uncuthorized', 'unauthorized', regex=False)\
        .str.replace(r'd?w?uty', 'duty', regex=False)\
        .str.replace('olinace', 'online', regex=False)\
        .str.replace('adher', 'adherence', regex=False)
    return df.drop(columns='complaint')


def extract_actions_from_disposition19(df):
    actions = df.disposition.str.lower().str.strip()\
        .str.replace('/', '', regex=False)\
        .str.replace(r're[dp]', 'reprimand', regex=True)\
        .str.replace(r'verbal$|g?s?f?iverbal reprimand', 'verbal reprimand', regex=True)\
        .str.replace('1 d day', '1day')\
        .str.extract(r'( ?\d+ ?|susp|wr|vr|verbal reprimand|term|whitten bust rep)')
    df.loc[:, 'action'] = actions[0].fillna('')\
        .str.replace('/', '', regex=False)\
        .str.replace(r'^ | $', '', regex=True)\
        .str.replace(r'(\d+) ?[wd]a?y?s?', r'\1-day suspension', regex=True)\
        .str.replace(r'(\d+)$', r'\1-day suspension', regex=True)\
        .str.replace(r'(wr|whitten bust rep)', 'written reprimand', regex=True)\
        .str.replace('vr', 'verbal reprimand', regex=False)\
        .str.replace('term', 'termination', regex=False)\
        .str.replace(r'\bsusp\b', 'suspenion', regex=True)
    return df


def clean_disposition19(df):
    df.loc[:, 'disposition'] = df.disposition.str.lower().str.strip().fillna('')\
        .str.replace('  ', ' ', regex=False)
    return standardize_from_lookup_table(df, 'disposition', disposition_lookup)


def clean_investigator19(df):
    df.loc[:, 'investigator_name'] = df.investigator.str.lower().str.strip()\
        .str.replace('refferred', '', regex=False)\
        .str.replace(r',|< |-|\.', '', regex=True)\
        .str.replace(r'^(\w{1})\.? ?', '', regex=True)\
        .str.replace(r'^h(\w+)', 'richard harrell', regex=True)\
        .str.replace(r'^c(\w+)', 'kirk carroll')
    return df


def clean20():
    df = pd.read_csv(data_file_path('raw/lake_charles_pd/lake_charles_pd_cprr_2020.csv'))
    df = df\
        .pipe(clean_column_names)\
        .rename(columns={
            'date_of_investigation': 'investigation_start_date'
        })\
        .pipe(clean_charges)\
        .pipe(split_rows_with_multiple_charges)\
        .pipe(clean_action)\
        .pipe(consolidate_action_and_disposition)\
        .pipe(clean_disposition)\
        .pipe(split_rows_with_multiple_officers)\
        .pipe(drop_rows_missing_disp_charges_and_action)\
        .pipe(assign_empty_first_name_column)\
        .pipe(assign_first_names_from_post)\
        .pipe(assign_agency)\
        .pipe(float_to_int_str, ['investigation_start_date'])\
        .pipe(gen_uid, ['first_name', 'last_name', 'agency'])\
        .pipe(gen_uid, ['first_name', 'last_name', 'investigation_start_date', 'charges', 'action'], 'complaint_uid')
    return df


def clean19():
    df = pd.read_csv(data_file_path('raw/lake_charles_pd/lake_charles_pd_cprr_2014_2019.csv'))\
        .pipe(clean_column_names)\
        .pipe(clean_investigation_start_date_19)\
        .pipe(clean_dates, ['investigation_start_date'])\
        .pipe(clean_tracking_number_19)\
        .pipe(clean_complainant_19)\
        .pipe(extract_rank_from_name_19)\
        .pipe(split_name_19)\
        .pipe(clean_charges_19)\
        .pipe(extract_actions_from_disposition19)\
        .pipe(clean_disposition19)\
        .pipe(clean_investigator19)
    return df


if __name__ == '__main__':
    df20 = clean20()
    ensure_data_dir('clean')
    df20.to_csv(data_file_path('clean/cprr_lake_charles_pd_2020.csv'), index=False)
