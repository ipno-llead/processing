import sys
sys.path.append('../')
from lib.path import data_file_path
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid


def extract_department_desc(df):
    departments = df.rank_desc\
        .str.extract(r'( ?i\.?t\.? ?| ?accounting ?| ?payroll ?| ?purchasing ?| ?property ?| ?narcotics ?| ?baliff ?|'
                     r' ?intake ?| ?jail ?| ?communications ?| ?propert ?| ?cib ?| ?juvenile ?|'
                     r' ?internal affairs ?| ?burglary ?| ?central records ?| ?crime lab ?| ?ems ?| ?homicide ?|'
                     r' ?robbery ?| ?court ?| ?human resources ?| ?accounting ?|'
                     r' ?academy ?| ?mail ?| ?database ?| ?network ?| ?\, div\. [abc] ?|'
                     r'\, hear 1|arson ?| ?auto theft ?| ?pawn shop ?| ?management ?|'
                     r' ?task force ?| ?ncic ?| ?pawn shop ?| ?jttf ?| ?internal management ?|'
                     r' ?dna lab ?| ?crime scene ?| ?dispatche?r? ?| ?training ?| ?firearms ?|'
                     r' ?traffic ?| ?economic crime ?| ?shoot squad ?| ?s\.i\.u ?|'
                     r' ?booking ?| ?jpcc ?| ?transportation ?|'
                     r'\, ?(.+)?| ?dea ?(.+)?| ?narcoc?e?c?s ?| ?strategic engagement ?|'
                     r' ?cic ?| ?intelligence ?| ?intensive probation ?|'
                     r' ?personal violence ?| ?crimes ?| ?missing persons ?|'
                     r' ?operations ?| ?sales tax ?| ?health insurance ?|'
                     r' ?grants ?| ?budget ?| ?finance?i?a?l? ?| ?tax (collector|supervisor) ?|'
                     r' ?public assisgments ?| ?youth programs ?| ?community relations ?|'
                     r' ?lab services ?|\;? ?technical services| ?prisoner ?|'
                     r' ?asset forfeure ?| ?strategic engagement ?| ?dlr ?|'
                     r' ?tech services ?| ?sib ?| ?p\.o\.c\. ?| ?school offense ?|'
                     r' ?accounts payable ?| ?range ?| ?light shop ?| ?public assignments ?|'
                     r' ?help desk tech ?| ?print shop ?| ?mang services ?|'
                     r' ?terminal agency ?| ?handgun perms ?| ?insurance ?| ?vice ?|'
                     r' ?photo lab ?| ?fingerprint ?| ?polygraph ?| ?evidence ?|'
                     r' ?digital forensic ?| ?dna ?| ?patrol ?)')
    df.loc[:, 'department_desc'] = departments[0]\
        .str.replace(r'^\, ', '', regex=True)\
        .str.replace(r'^  ?', '', regex=True).str.replace(r'  ?$', '', regex=True)\
        .str.replace(r'\bpropert\b', 'property', regex=True)\
        .str.replace(r'(database|network)', 'it', regex=True)\
        .str.replace('ems', 'emergency medial services', regex=False)\
        .str.replace(r'\-$', '', regex=True)\
        .str.replace(r'^dna$', 'dna lab', regex=True)\
        .str.replace(r'^jpcc$', 'jefferson parish corrections center', regex=True)\
        .str.replace('financial', 'finance', regex=False)\
        .str.replace(r' detectiv?e?', '', regex=True)\
        .str.replace(r'\btech\b', 'technical', regex=True)
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


def clean_rank_desc(df):
    df.loc[:, 'rank_desc'] = df.rank_desc\
        .str.replace(r'^prisonercom:$', 'prisoner commander', regex=True)\
        .str.replace(r'^prisonerserge$', 'prisoner sergeant', regex=True)\
        .str.replace(r'( ?i\.?t\.? ?| ?accounting ?| ?payroll ?| ?purchasing ?| ?property ?| ?narcotics ?| ?baliff ?|'
                     r' ?intake ?| ?jail ?| ?communications ?| ?propert ?| ?cib ?| ?juvenile ?|'
                     r' ?internal affairs ?| ?burglary ?| ?central records ?| ?crime lab ?| ?ems ?| ?homicide ?|'
                     r' ?robbery ?| ?court ?| ?human resources ?| ?accounting ?|'
                     r' ?academy ?| ?mail ?| ?database ?| ?network ?| ?\, div\. [abc] ?|'
                     r'\, hear 1|arson ?| ?auto theft ?| ?pawn shop ?| ?management ?|'
                     r' ?task force ?| ?ncic ?| ?pawn shop ?| ?jttf ?| ?internal management ?|'
                     r' ?dna lab ?| ?crime scene ?| ?dispatche?r? ?| ?training ?| ?firearms ?|'
                     r' ?traffic ?| ?economic crime ?| ?shoot squad ?| ?s\.i\.u ?|'
                     r' ?booking ?| ?jpcc ?| ?transportation ?|'
                     r'\, (.+)?| ?dea ?(.+)?| ?narcoc?e?c?s ?| ?strategic engagement ?|'
                     r' ?cic ?| ?intelligence ?| ?intensive probation ?|'
                     r' ?personal violence ?| ?crimes ?| ?missing persons ?|'
                     r' ?operations ?| ?sales tax ?| ?health insurance ?|'
                     r' ?grants ?| ?budget ?| ?finance?i?a?l? ?| ?tax (collector|supervisor) ?|'
                     r' ?public assisgments ?| ?youth programs ?| ?community relations ?|'
                     r' ?lab services ?|\;? ?technical services| ?prisoner ?|'
                     r' ?asset forfeure ?| ?strategic engagement ?| ?dlr ?|'
                     r' ?tech services ?| ?sib ?| ?p\.o\.c\. ?| ?school offense ?|'
                     r' ?accounts payable ?| ?range ?| ?light shop ?| ?public assignments ?|'
                     r' ?help desk tech ?| ?print shop ?| ?mang services ?|'
                     r' ?terminal agency ?| ?handgun perms ?| ?insurance ?| ?vice ?|'
                     r' ?photo lab ?| ?fingerprint ?| ?polygraph | ?evidence ?|'
                     r' ?digital forensic ?| ?dna ?)', '', regex=True)\
        .str.replace(r'\bcommaander\b', 'commander', regex=True)\
        .str.replace(r'\bpropert\b', 'property', regex=True)\
        .str.replace(r'\bcustodia\b', 'custodian', regex=True)\
        .str.replace('&', 'and', regex=True)\
        .str.replace(r'\bcomm\b', 'communications', regex=True)\
        .str.replace(r'\bdetecti\b', 'detective', regex=True)\
        .str.replace('booking of', 'booking officer', regex=False)\
        .str.replace(r'^(patrol)? ?deputy patrol ?(deputy)?$', 'patrol deputy', regex=True)\
        .str.replace(r' \- ', '; ', regex=True)\
        .str.replace(r'\bsupe?rv?i?s?o?\:?\b', 'supervisor', regex=True)\
        .str.replace(r'^r\.f\.manag', 'r.f manager', regex=True)\
        .str.replace(r'(^of$|^cc$)', '', regex=True)\
        .str.replace(r'^against$', 'against persons', regex=True)\
        .str.replace('gretnadetective', 'gretna detective', regex=False)\
        .str.replace('fbidetective', 'fbi detective', regex=False)\
        .str.replace(r'\bcomm?a??n?d?e?r?i?o?\b', 'commander', regex=True)\
        .str.replace('chief officer', 'chief officer', regex=False)\
        .str.replace(r'^invest$', 'investigator', regex=True)\
        .str.replace(r'^\.? ?', '', regex=True)\
        .str.replace(r'asst\.? ?', 'assistant ', regex=True)\
        .str.replace('systspeciali', 'systems specialist', regex=False)\
        .str.replace(r'^and ?', '', regex=True)\
        .str.replace('digal', 'digital', regex=False)\
        .str.replace('emergencycomputer', 'emergency computer', regex=False)\
        .str.replace(r'^serge$', 'sergeant', regex=True)\
        .str.replace(r'^detective detective$', 'detective', regex=True)\
        .str.replace(r'^sergeant patrol sergeant$', 'patrol sergeant', regex=True)\
        .str.replace(r'\bpatroi\b', 'patrol', regex=True)\
        .str.replace(r'\:$', '', regex=True)\
        .str.replace('commanderssary', 'commander', regex=False)\
        .str.replace(r' ?\-$', '', regex=True)\
        .str.replace('coordinato', 'coordinator', regex=False)\
        .str.replace(r'^repairman repairman$', 'repairman', regex=True)\
        .str.replace('systmanager', 'systems manager', regex=False)
    return df


def clean():
    df = pd.read_csv(data_file_path('raw/jefferson_so/jefferson_parish_so_pprr_2020.csv'))\
        .pipe(clean_column_names)\
        .pipe(extract_department_desc)\
        .pipe(clean_district_desc)\
        .pipe(split_names)\
        .pipe(clean_rank_desc)\
        .pipe(standardize_desc_cols, ['rank_desc', 'department_desc'])\
        .pipe(set_values, {
            'agency': 'Jefferson SO'
        })\
        .pipe(gen_uid, ['agency', 'first_name', 'middle_name', 'last_name'])
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/pprr_jefferson_so_2020.csv'), index=False)
