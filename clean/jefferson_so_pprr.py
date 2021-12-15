import sys
sys.path.append('../')
from lib.path import data_file_path
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import standardize_from_lookup_table


dept_desc_lookup_table = [
    ['it', 'i.t.'],
    ['accounting'],
    ['payroll'],
    ['purchasing'],
    ['narcotics'],
    ['baliff'],
    ['intake'],
    ['jail'],
    ['communications'],
    ['property', 'propert & evidence'],
    ['cib'],
    ['fines'],
    ['internal affairs'],
    ['burglary'],
    ['central records'],
    ['crime lab'],
    ['emergency medical services', 'ems'],
    ['homicide'],
    ['robbery'],
    ['internal management'],
    ['jtff'],
    ['ncic'],
    ['task force'],
    ['management'],
    ['pawn shop'],
    ['auto theft'],
    ['arson'],
    ['div c'],
    ['div b'],
    ['div a'],
    ['it; network', 'network'],
    ['it; database', 'database'],
    ['mail'],
    ['academy', 'training'],
    ['accounting'],
    ['human resources'],
    ['court'],
    ['strategic engagement'],
    ['narcotics', 'narcoces', 'narco', 'narocites', 'narcoitcs'],
    ['dea'],
    ['transportation'],
    ['jefferson parish corrections center', 'jpcc', 'jail management'],
    ['booking'],
    ['s.i.u'],
    ['shoot squaf'],
    ['economic crime'],
    ['traffic'],
    ['firearms'],
    ['crime scene'],
    ['dna lab', 'dna'],
    ['tax supervisor'],
    ['tax collecor'],
    ['finance', 'financial'],
    ['budget'],
    ['grants'],
    ['health insurance'],
    ['sales tax'],
    ['operations'],
    ['missing persons'],
    ['crimes'],
    ['personal violence'],
    ['intenstive probation'],
    ['intelligence'],
    ['cic'],
    ['management services', 'manag services'],
    ['print shop'],
    ['help desk tech'],
    ['light shop'],
    ['range'],
    ['accounts payable'],
    ['school offense'],
    ['p.o.c'],
    ['sib'],
    ['technical services', 'tech services'],
    ['dlr'],
    ['asset forfeiture', 'asset forfeure'],
    ['prisoner'],
    ['technical services'],
    ['lab services'],
    ['community relations'],
    ['youth programs'],
    ['public assignments'],
    ['patrol'],
    ['digital forensic'],
    ['evidence'],
    ['polygraph'],
    ['fingerprint'],
    ['photo lab'],
    ['vice'],
    ['insurance'],
    ['handgun permits', 'handgun perms'],
    ['terminal agency'],
    ['personnel background'],
    ['forensic chemistry'],
    ['process server'],
    ['intensive probation'],
    ['shoot squad'],
    ['qa/qc'],
    ['commisary'],
    ['canine'],
    ['paint and body'],
    ['fbi'],
    ['24th juidicial - i'],
    ['24th judicial - e'],
    ['24th judicial - f'],
    ['5th circuit app'],
    ['fbi task force'],
    ['24th judicial - j'],
    ['24th judicial - a'],
    ['24th judicial - n'],
    ['24th judicial - o'],
    ['24th judicial - c'],
    ['24th judicial - l'],
    ['communications ct. - domestic', 'comm ct. - domestic'],
    ['24th judicial - g'],
    ['intake booking'],
    ['juvenile court division a', 'juvenile court baliff, div. a'],
    ['fleet management'],
    ['juvenile court division c', 'juvenile court baliff, div. c'],
    ['campus police', 'police on campus'],
    ['jttf'],
    ['division a', 'div. a'],
    ['division b', 'div. b'],
    ['2nd parish ct'],
    ['codis'],
    ['ions'],
    ['it; motor pool', 'motor pool', 'eb motor pool'],
    ['yenni building'],
    ['division c', 'div. c'],
    ['24th juidicial h'],
    ['24th juidicial - m'],
    ['inmate security', 'inmate sec.'],
    ['claims'],
    ['contracts office', 'contracts manager office'],
    ['24th judicial - h'],
    ['prisoner transportation'],
    ['parish security']]


def extract_department_descriptions(df):
    df.loc[:, 'department_desc'] = df.rank_desc.str.lower().str.strip().fillna('')\
        .str.replace(r' $', '', regex=True)\
        .str.replace(r'   +', ' ', regex=True)
    return standardize_from_lookup_table(df, 'department_desc', dept_desc_lookup_table)


def clean_department_descriptions(df):
    df.loc[:, 'department_desc'] = df.department_desc\
        .str.replace(r'^it; it$', 'it', regex=True)\
        .str.replace(r'^patrol; patrol$', 'patrol', regex=True)\
        .str.replace(r'^it; it; network', 'it; network', regex=True)
    return df


def clean_district_desc(df):
    df.loc[:, 'district'] = df.dept_desc.fillna('')\
        .str.replace(r' ?sergeant ?', '', regex=True)\
        .str.replace('lieutenant', '', regex=False)
    return df.drop(columns='dept_desc')


def split_names(df):
    names = df.name.fillna('')\
        .str.replace(r'\.\,', ',', regex=True)\
        .str.replace('sue ellen', 'sue-ellen', regex=False)\
        .str.replace("jon' janice", "jon'janice", regex=False)\
        .str.replace('photo lab day', 'photolabday', regex=False)\
        .str.replace(' employees', '', regex=False)\
        .str.extract(r'^(\w+\-?\.?\'? ?\w+?\'?) ?(?:(\w+) )?\, (?:(\w+\-?\'?\w+?\'?)) ?(\w+)?\.?$')

    df.loc[:, 'last_name'] = names[0].fillna('')
    df.loc[:, 'suffix'] = names[1].fillna('')
    df.loc[:, 'first_name'] = names[2].fillna('')
    df.loc[:, 'middle_name'] = names[3].fillna('')\
        .str.replace(r'\.', '', regex=True)
    df.loc[:, 'last_name'] = df.last_name.str.cat(df.suffix, sep=' ')
    return df[~((df.first_name == '') & (df.last_name == ''))].drop(columns=['suffix', 'name'])


rank_desc_lookup_table = [
    ['commander', 'commaander', 'enforcement comn', 'comma', 'prisoner transportation com', 
     'commai', 'commando'],
    ['custodian', 'custodia'],
    ['detective', 'detecti', 'detectiv'],
    ['officer'],
    ['deputy'],
    ['deputy field trainee'],
    ['supervisor', 'supervis', 'super', 'superv'],
    ['manager'],
    ['chief officer'],
    ['chemist'],
    ['investigator'],
    ['assistant', 'asst.'],
    ['systems specialist', 'systspeciali'],
    ['emrgency computer', 'emergencycomputer'],
    ['sergeant', 'serge', 'sergeant; sergeant'],
    ['coordinator'],
    ['systems manager', 'systmanager'],
    ['bailiff', 'baliff'],
    ['field trainee'],
    ['instructor'],
    ['clerk'],
    ['chief pilot'],
    ['legal liaison'],
    ['chief'],
    ['mechanic'],
    ['enforcement commander', 'enforcement comn'],
    ['dispatcher'],
    ["chief's secretary", "chief's secret"],
    ['administrator', 'admin'],
    ['sheriff'], 
    ['chaplain'],
    ['polygraphist'],
    ['analyst'],
    ['technician'],
    ['director'],
    ['lieutenant'],
    ['community liaison'],
    ['repairman'],
    ['campus police', 'police campus'],
    ['grants writer'],
    ['secretary']]


def standardize_rank_descriptions(df):
    df.loc[:, 'rank_desc'] = df.rank_desc.str.lower().str.strip().fillna('')\
        .str.replace(r' $', '', regex=True)\
        .str.replace(r'  +', ' ', regex=True)
    return standardize_from_lookup_table(df, 'rank_desc', rank_desc_lookup_table)


def clean_rank_descriptions(df):
    df.loc[:, 'rank_desc'] = df.rank_desc\
        .str.replace(r'^sergeant; sergeant$', 'sergeant', regex=True)\
        .str.replace(r'^custodian; custodian', 'custodian', regex=True)\
        .str.replace(r'^detective; detective', 'detective', regex=True)\
        .str.replace(r'^deputy; deputy; field trainee$', 'deputy; field trainee', regex=False)\
        .str.replace(r'^deputy; deputy', 'deputy', regex=True)\
        .str.replace(r'^technician; technician', 'technician', regex=True)\
        .str.replace(r'^chief; officer$', 'chief', regex=True)\
        .str.replace(r'repairman; repairman', 'repairman', regex=True)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/jefferson_so/jefferson_parish_so_pprr_2020.csv'))\
        .pipe(clean_column_names)\
        .pipe(clean_district_desc)\
        .pipe(split_names)\
        .pipe(extract_department_descriptions)\
        .pipe(clean_department_descriptions)\
        .pipe(standardize_rank_descriptions)\
        .pipe(clean_rank_descriptions)\
        .pipe(set_values, {
            'agency': 'Jefferson SO'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'middle_name', 'last_name', 'employee_id'])
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/pprr_jefferson_so_2020.csv'), index=False)
