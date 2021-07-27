from pandas.io.parsers import read_csv
from lib.columns import clean_column_names, float_to_int_str
from lib.clean import clean_dates, standardize_desc_cols
from lib.uid import gen_uid
from lib.standardize import standardize_from_lookup_table
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import re
import sys
sys.path.append("../")

actions_lookup = [
    ['exonerated'],
    ['not sustained'],
    ['resigned'],
    ['office investigation'],
    ['hold in abeyance'],
    ['counseled'],
    ['30-day suspension'],
    ['letter of caution'],
    ['2-day suspension'],
    ['verbal judo training', 'verbal judo class'],
    ['attaining respect class'],
    ['early intervention'],
    ['tactical training in reasonable suspicion & felony stops'],
    ['letter of reprimand'],
    ['peer intervention training'],
    ['21-day suspension'],
    ['1-day suspension'],
    ['termination'],
    ['letter of caution'],
    ['firearm safety training'],
    ['range master'],
    ['1 day driving school'],
    ['letter of instruction'],
    ['conference worksheet'],
    ['9-day suspension'],
    ['65-day suspension'],
    ['demotion - from sgt. to cpl.'],
    ['7-day suspension'],
    ['de-escalation, r/s & p/c training'],
    ['uof training'],
    ['5-day suspension']
]


def realign_18():
    df = pd.read_csv(
        data_file_path("baton_rouge_pd/baton_rouge_pd_cprr_2018.csv"),
        encoding="latin1")
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.rename(columns={"Comptaint": "Complaint"}, inplace=True)

    # manual edits
    df.iloc[4, 2] = "Administrative Review"
    df.iloc[12, 5] = df.iloc[12, 5] + "P10467 Patrol 3rd District"
    df.iloc[12, 6] = df.iloc[12, 6] + "Orders - (Pursuit) - 40"
    for ind in [16, 23, 25, 29]:
        m = re.match(r"^(.+) (\d+\:\d+) *$", df.iloc[ind-1, 6])
        df.iloc[ind-1, 6] = m.group(1)
        df.iloc[ind, 6] = "%s %s" % (m.group(2), df.iloc[ind, 6])
    for ind in [39]:
        m = re.match(r"^(\d+) (.+)$", df.iloc[ind+1, 6])
        df.iloc[ind, 6] = df.iloc[ind, 6] + m.group(1)
        df.iloc[ind+1, 6] = m.group(2)
    df.iloc[24, 5] = df.iloc[24, 5][21:]
    df.iloc[38, 5] = df.iloc[38, 5] + "Operational Services"
    df.iloc[66, 6] = df.iloc[66, 6] + "Orders - (Pursuit) - 40"
    df.iloc[72, 0] = "2018"
    df.iloc[72, 1] = "35"
    df.iloc[72, 2] = "Administrative Review"
    df.iloc[72, 5] = df.iloc[72, 5][:-1]
    df.iloc[72, 6] = df.iloc[72, 6][:-1]
    df.iloc[83, 5] = "%s%s" % (df.iloc[83, 5], df.iloc[84, 5])
    df.iloc[83, 6] = "%s%s" % (df.iloc[83, 6], df.iloc[84, 6])
    df.iloc[128, 5] = "%s%s" % (df.iloc[128, 5], df.iloc[133, 4])
    df.iloc[128, 6] = "%s%s" % (df.iloc[128, 6], df.iloc[133, 5])
    df.iloc[115, 6] = "%s%s" % (df.iloc[115, 6], df.iloc[129, 6])
    for ind in [134, 135]:
        df.iloc[ind, 6] = df.iloc[ind, 5]
        df.iloc[ind, 5] = df.iloc[ind, 4]
        df.iloc[ind, 4] = ""
    df.iloc[142, 5] = df.iloc[144, 5]
    df.iloc[143, 5] = df.iloc[144, 5]

    # insert missing rows in page 2
    df.loc[len(df)] = [
        "2018", "010", "Administrative Review", "1/26/18", "1/26/18",
        "ROBERTSON JASON R., P10639 Patrol 1st District",
        "3:17 Carrying Out Orders / General Orders - (Pursuit) - 40",
        "Not Sustained", "Not Sustained"]
    df.loc[len(df)] = [
        "2018", "011", "Administrative Review", "2/8/18", "2/8/18",
        "CARTER. JR. DARRELL J., P10511 Patrol 4th District",
        "3:20 Use of Force / Intermediate Weapon - 52",
        "Not Sustained", "Not Sustained"]

    # normalize
    df.loc[:, "IA Year"] = df["IA Year"].str.replace("-", "").str.strip()
    df.loc[:, "Status"] = df["Status"].str.replace("-", "").str.strip()\
        .str.title()
    df.loc[:, "Received"] = df["Received"].str.replace("-", "").str.strip()
    df.loc[:, "Occur Date"] = df["Occur Date"].str.replace("-", "")\
        .str.strip().fillna("")
    df.loc[:, "Officer Name"] = df["Officer Name"].str.strip()\
        .str.replace(r"(\.|,) +(PC?\d+)", r", \2")\
        .str.replace(", JR", ". JR", regex=False)\
        .str.replace(" Il ", " II ", regex=False)\
        .str.replace(r"(\d+)\. ", r"\1 ")\
        .str.replace("Distnct", "District", regex=False)\
        .str.replace("none entered", "", regex=False).fillna("")
    df.loc[:, "Complaint"] = df.loc[:, "Complaint"]\
        .str.replace(r"\< *\d+.+$", "").str.strip()\
        .str.replace(r"^(\d+)(?:\.|,)(\d+)", r"\1:\2")\
        .str.replace(r" \.(\d+)", r" \1")\
        .str.replace(r" of$", "").str.replace("Use Force", "Use of Force", regex=False)\
        .str.replace(r" ?- ", " ").str.replace(" lo ", " to ", regex=False)\
        .str.replace("Emply", "Empty", regex=False)\
        .str.replace("Oul ", "Out ", regex=False).fillna("")
    df.loc[:, "Action"] = df["Action"]\
        .where(~df["Action"].str.isupper().fillna(False), df["Action"].str.title())\
        .str.replace(" lo ", " to ", regex=False)\
        .str.replace(r"^ *- *$", "")\
        .str.replace("Suspension.", "Suspension,", regex=False)\
        .str.replace("Exonerated;", "Exonerated:", regex=False)\
        .str.replace("Sgl. ", "Sgt. ", regex=False).str.strip().fillna("")
    df.loc[:, "Disposition"] = df["Disposition"].str.replace(r" ?- ", " ")\
        .str.replace(r"\< *\d+.+$", "").str.replace(r"^\.", "")\
        .str.replace("Exoneraled", "Exonerated", regex=False)\
        .str.replace("Sustaned", "Sustained", regex=False).str.strip().fillna("")
    df.loc[:, "Action"] = df["Action"].str.replace(
        "Slops", "Stops", regex=False)

    df.drop([84, 129, 133], inplace=True)
    df.loc[:, "1A Seq"] = df["1A Seq"].str.rjust(3, "0")
    df.reset_index(drop=True, inplace=True)
    return df


def parse_complaint_18(df):
    complaint = df.complaint.str.replace(
        r"^(\d+\:\d+) (.+) (\d+)", r"\1@@\2@@\3").str.split("@@", expand=True)
    complaint.columns = ["rule_code", "rule_paragraph", "paragraph_code"]
    df = pd.concat([df, complaint], axis=1)
    df.loc[(df.rule_code == "3:17") & (df.paragraph_code == "40"),
           "rule_paragraph"] = 'Carrying Out Orders / General Orders (Pursuit)'
    rule_paragraph = df.rule_paragraph.str.split(" / ", expand=True)
    rule_paragraph.columns = ["rule_violation", "paragraph_violation"]
    df = pd.concat([df, rule_paragraph], axis=1)
    df = df.drop(columns=["complaint", "rule_paragraph"])
    df.loc[:, "paragraph_violation"] = df.paragraph_violation.fillna("")
    return df


def parse_officer_name_18(df):
    dep = df.officer_name.str.replace(
        r"^(.+), (PC?\d+) (.+)$", r"\1 # \2 # \3").str.split(" # ", expand=True)
    dep.columns = ["name", "department_code", "department_desc"]
    dep.loc[:, "name"] = dep["name"].str.replace(
        r"\.", "").str.strip().str.lower()

    names = dep["name"].str.lower().str.replace(r"\s+", " ").str.replace(
        r"^(\w+(?: (?:iii?|iv|v|jr|sr))?) (\w+)(?: (\w+|n\/a))?$", r"\1 # \2 # \3").str.split(" # ", expand=True)
    names.columns = ["last_name", "first_name", "middle_initial"]
    names.loc[:, "middle_initial"] = names["middle_initial"]\
        .str.replace("n/a", "", regex=False).fillna("")
    names.loc[:, "middle_name"] = names.middle_initial.map(
        lambda v: "" if len(v) < 2 else v)
    names.loc[:, "middle_initial"] = names.middle_initial.map(lambda v: v[:1])

    df = pd.concat([df, dep, names], axis=1)
    df.drop(columns=["officer_name", "name"], inplace=True)
    return df


def assign_tracking_num_18(df):
    df.loc[:, "tracking_number"] = df.ia_year + "-" + df["1a_seq"]
    df = df.drop(columns=["1a_seq", "ia_year"])
    return df


def standardize_action_18(df):
    df.loc[:, "action"] = df.action\
        .str.replace(r"(\d) day suspension", r"\1-day suspension")\
        .str.replace("de- escalation", "de-escalation", regex=False)
    return standardize_from_lookup_table(df, "action", actions_lookup)


def combine_rule_and_paragraph(df):
    def combine(row):
        rule = ' '.join(filter(None, [row.rule_code, row.rule_violation]))
        paragraph = ' '.join(
            filter(None, [row.paragraph_code, row.paragraph_violation]))
        return ' - '.join(filter(None, [rule, paragraph]))
    df.loc[:, 'charges'] = df.apply(combine, axis=1, result_type='reduce')
    df = df.drop(columns=['rule_code', 'rule_violation',
                          'paragraph_code', 'paragraph_violation'])
    return df


def assign_data_production_year_18(df):
    df.loc[:, "data_production_year"] = df.occur_year.where(
        df.occur_year != "", df.receive_year)
    return df


def assign_agency_18(df):
    df.loc[:, "agency"] = "Baton Rouge PD"
    return df


def drop_office_investigation_rows(df):
    return df[~(df.action == 'office investigation')].reset_index(drop=True)


def clean_status(df):
    df.loc[:, 'investigation_status'] = df.status.fillna('').str.lower().str.strip()\
        .str.replace(r'invesligation$', 'investigation', regex=True)\
        .str.replace(r'(- ?|\.)', '', regex=True)\
        .str.replace('&', 'and', regex=False)
    return df.drop(columns='status')


def create_tracking_number(df):
    df.loc[:, "tracking_number"] = df.ia_year.apply(str) + '-' + df.ia_seq.apply(str)
    return df.drop(columns=['ia_seq', 'ia_year'])


def clean_receive_incident_dates(df):
    df.receive_date = df.receive_date\
        .str.replace('- ', '', regex=False)
    df.incident_date = df.incident_date\
        .str.replace('- ', '', regex=False)\
        .str.replace('. ', '', regex=False)\
        .str.replace('13-Oct', '10/13/2015', regex=False)\
        .str.replace('16-Feb', '2/16/2016', regex=False)\
        .str.replace('16-Jul', '7/16/2017', regex=False)\
        .str.replace('17-Jul', '7/17/2017', regex=False)\
        .str.replace('1092019', '10/9/2019', regex=False)\
        .str.replace('2019-06', '', regex=False)
    return df


def clean_complainant(df):
    df.complainant = df.complainant.fillna('').str.lower().str.strip()\
        .str.replace(r'\< ?(\d+) ?\-? (\d+) \>', '', regex=True)\
        .str.replace('-', '', regex=False)\
        .str.replace(r'(h/)?[8b](p|r)(p|r)d?o?i?/? ?', 'brpd', regex=True)\
        .str.replace('damaging dept. equipment', '', regex=False)

    return df


def clean_charges(df):
    df.loc[:, 'charges'] = df.complaint.str.lower().str.strip().fillna('')\
        .str.replace(r' >i? ', ' - ', regex=True)\
        .str.replace('.', '', regex=False)\
        .str.replace(',', '', regex=False)\
        .str.replace(r'^use ', '3:20 use ', regex=True)\
        .str.replace(r' [rd]ep[lt]\.? ?', ' department ', regex=True)\
        .str.replace(r'(\d+)\.(\d+)', r'\1:\2', regex=True)\
        .str.replace(r'(2:1[43])? ?d?dmvr ?(violation)? ?(-)? ?(68)?', '2:14 dmvr violation - 68', regex=True)\
        .str.replace(' - - ', ' - ', regex=False)\
        .str.replace(' / - ', ' / ', regex=False)\
        .str.replace(r'(2:[23])? ?shirking ?(duties)? ?(14)?', '2:2 shirking duties - 14', regex=True)\
        .str.replace(r'^-$', '', regex=True)\
        .str.replace(r'(\w+)/(\w+)', r'\1 / \2', regex=True)\
        .str.replace(r'^3:22$', '3:22 violation of known laws', regex=True)\
        .str.replace(r'(failure to report)? ?(2:[87])? ?damag(ed?|ing)? ?(to)? ?(department)? ?(equip?(ment)?)? ?(- 18)?',
        '2:7 damaging department equipment - 18', regex=True)\
        .str.replace(r'(\w+) - (\(?\w+\)?)', r'\1 \2', regex=True)\
        .str.replace('shooling', 'shooting', regex=False)\
        .str.replace(r'^carr[ry]?(ing)? out orders\b', '3:17 carrying out orders', regex=True)\
        .str.replace(r'\((veh)? ?(pursuit)?\)$', '(pursuit) - 40', regex=True)\
        .str.replace(r'(^22?:?[12]?2?)? ?command ?(of)? temper ?(13)?', '2:2 command of temper - 13', regex=True)\
        .str.replace(r'^(viol)?(ation)? ?of (known)? ?laws', '3:22 violation of known laws', regex=True)\
        .str.replace(r' \/(\w+)', r' / \1', regex=True)\
        .str.replace(r' (\d+) (\d+):(\d+)', r'\1', regex=True)\
        .str.replace(r'(2:?1[12])? ?(conduct)? ?unbecoming ?(an)? ?(officer)? ?(\(?harrassment\)?)? ?(21)? ?(violation)?',
        '2:12 conduct unbecoming an officer - 21 ', regex=True)\
        .str.replace(r'(1:7)? ?(fail)?(ure)? ?(to)? ?(comp(lete?)?(ion)?)? ?(required)? ?(and)? ?&? ?/? ?(submissions?)? ?(of)? ?(required)? forms? ?(8)?',
        '1:7 failure to submit required forms', regex=True)\
        .str.replace(r'^insubordination$', '3:18 insubordination - 43', regex=True)\
        .str.replace(r'(2:5)? ?awol ?(15)?', '2:5 absent without leave - 15', regex=True)\
        .str.replace(r'^punctuality$', '1:5 punctuality - 6', regex=True)\
        .str.replace(r'^truthfulness$', '3:23 truthfulness - 58', regex=True)\
        .str.replace(r'^sexual harrassment$', '3:14 sexual harassment - 37', regex=True)\
        .str.replace(r'^(release of prisoner)? ?/? ?allow(ing)? ?escape ?(30)?', 
        '3:7 release of prisoners / allowing escape - 30', regex=True)\
        .str.replace(r'(2:1[23])? ?respect ?(of)? ?(fellow)? ?(officers|members)? ?(22)?', '2:13 respect of fellow officers - 22')\
        .str.replace(r'(3:9)? ?failure to provide info(rmation)? ?(to superior)? ?(32)?', '3:9 failure to provide information to superior - 39', regex=True)\
        .str.replace(r'(\w+)-', r'\1', regex=True)\
        .str.replace('incar', 'in car', regex=False)\
        .str.replace('accidentl discharge', 'accidental shooting', regex=False)\
        .str.replace(r'^assc with known criminals$', '3:21 association with known criminals - 55', regex=True)\
        .str.replace(r'\(contact person\)45 failure to$', '(contact person) - 45', regex=True)\
        .str.replace(r'desertion', '2:5 absent without leave - 15', regex=False)\
        .str.replace(r'^cowardice$', '3:15 cowardice - 38', regex=True)\
        .str.replace(r'^confidentiality$', '3:8 confidentiality - 31', regex=True)\
        .str.replace(r'^unauth public statements$', '3:5 unauthorized public statements - 27', regex=True)\
        .str.replace(r'^fals[ei]f?i?(cation)? ?(of)? documents', '3:19 falsification of documents - 44')\
        .str.replace(r'dl suspension', "suspended driver's license", regex=False)\
        .str.replace(' i intermediate weapon ', ' / intermediate weapon - ', regex=False)\
        .str.replace(r'^320\b', '3:20', regex=True)\
        .str.replace(r' a shooting', ' / shooting', regex=False)\
        .str.replace(r'\binvest\b', 'investigation', regex=True)\
        .str.replace(r' (\(drugs\))? ', ' - ', regex=True)\
        .str.replace(r'^fai[tl](urr?e)? ?(to)? ?(secu[tr]e)? ?(property)? ?/? ?(or)? ?(evid(ence)?)?$',
         '3:4 failure to secure property or evidence - 26', regex=True)
    return df.drop(columns='complaint')


def parse_officer_name_2021(df):
    dep = df.officer_name.str.replace(r'^(.+), (PC?\d+) (.+)$', r'\1 # \2 # \3').str.split(' # ', expand = True)
    dep.columns = ['name', 'department_code', 'dept_description']
    dep.loc[:, 'name'] = dep['name'].str.lower().str.strip()

    names = dep["name"].str.lower().str.replace(r"\s+", " ").str.replace(
        r"^(\w+(?: (?:iii?|iv|v|jr|sr))?) (\w+)(?: (\w+|n\/a))?$", r"\1 # \2 # \3").str.split(" # ", expand=True)
    names.columns = ["last_name", "first_name", "middle_initial"]
    names.loc[:, "middle_initial"] = names["middle_initial"]\
        .str.replace("n/a", "", regex=False).fillna("")
    names.loc[:, "middle_name"] = names.middle_initial.map(
        lambda v: "" if len(v) < 2 else v)
    names.loc[:, "middle_initial"] = names.middle_initial.map(lambda v: v[:1])

    df = pd.concat([df, dep, names], axis=1)
    df.drop(columns=["officer_name", "name"], inplace=True)
    return df


def clean_disposition(df):
    df.disposition = df.disposition.str.lower().str.strip().fillna('')\
        .str.replace(r'/?admin\.?,?', 'admin', regex=True)\
        .str.replace('.', '', regex=True)\
        .str.replace(r'^/?', '', regex=True)\
        .str.replace(r'pre-? ?disc ?(hearing)?', 'pre-disciplinary hearing', regex=True)\
        .str.replace(r'sust\.?\b', 'sustained', regex=True)\
        .str.replace(r'(\w+)- (\w+)', r'\1-\2', regex=True)\
        .str.replace(r'(\w+)/ (\w+)', r'\1/\2', regex=True)\
        .str.replace(r'pre-? ?term(ination)? ?(hearing)?', 'pre-termination hearing', regex=True)\
        .str.replace(r'\binv\b', 'investigation', regex=True)\
        .str.replace(r'\bofc\b', 'office', regex=True)\
        .str.replace('referraladmin', 'referral/admin', regex=False)\
        .str.replace(r'^- ?-?$', '', regex=True)\
        .str.replace('referra)', 'referral', regex=False)
    return df


def combine_action_and_disposition(df):
    df.disposition = df.disposition.str.cat(df.action, sep='|')\
        .str.replace(r'^/', '', regex=True)\
        .str.replace('loc', 'letter of caution', regex=False)\
        .str.replace('lor', 'letter of reprimand', regex=False)\
        .str.replace(r'l\.?o\.?u\.?', 'loss of unit', regex=True)\
        .str.replace(r' ?sust?(alned)?\.?,?(l?aine?d)?\b', 'sustained', regex=True)\
        .str.replace(r'hr\.?\b', 'hour', regex=True)\
        .str.replace(r'(\d+)-? (\w+)', r'\1-\2', regex=True)\
        .str.replace(r',|\.', '', regex=True)\
        .str.replace(r'\bnotsustained\b', 'not sustained', regex=True)\
        .str.replace(r'^hord/not sustained blust/not sustained$', 'not sustained', regex=True)\
        .str.replace(r'^-$', '', regex=True)
    df.disposition = df.disposition.str.extract(r'(sustained|not sustained|exonerated)')
    return df

def clean_action(df):
    df.action = df.action.str.lower().str.strip()\
        .str.replace(r'not ?(sustained)? ?', '', regex=True)\
        .str.replace(r' ?sust?(alned)?\.?,?(l?aine?d)?\b', '', regex=True)\
        .str.replace(r'exonerated', '', regex=True)\
        .str.replace(r'^\.', '', regex=True)\
        .str.replace(r'^,', '', regex=True)\
        .str.replace(r'^/', '', regex=True)\
        .str.replace(r'^-', '', regex=True)\
        .str.replace(r'^;', '', regex=True)\
        .str.replace(r'(\d+)-? (\w+)', r'\1-\2', regex=True)\
        .str.replace('loc', 'letter of caution', regex=False)\
        .str.replace('lor', 'letter of reprimand', regex=False)\
        .str.replace(r'l\.?o\.?u\.?', 'loss of unit', regex=True)\
        .str.replace(r'hr\.?\b', 'hour', regex=True)\
        .str.replace(';', '/', regex=False)\
        .str.replace('.', '', regex=False)\
        .str.replace(',', '/', regex=False)\
        .str.replace(':', '/', regex=False)\
        .str.replace('&', '/', regex=False)\
        .str.replace(r' dri?v?\.? \bsch\b', 'driving school', regex=True)
    return df


def clean_2021():
    df = pd.read_csv(data_file_path(
        'baton_rouge_pd/baton_rouge_pd_2021.csv'))\
        .pipe(clean_column_names)
    df = df\
        .rename(columns={
            'received': 'receive_date',
            'occur_date': 'incident_date'
        })\
        .pipe(clean_status)\
        .pipe(float_to_int_str, ['ia_seq', 'ia_year'])\
        .pipe(create_tracking_number)\
        .pipe(clean_receive_incident_dates)\
        .pipe(clean_complainant)\
        .pipe(clean_dates, ['receive_date', 'incident_date'])\
        .pipe(clean_charges)\
        .pipe(clean_action)\
        .pipe(clean_disposition)\
        .pipe(parse_officer_name_2021)\
        .pipe(combine_action_and_disposition)\
        .pipe(standardize_desc_cols, ['charges', 'action', 'disposition'])\
        .pipe(standardize_from_lookup_table, 'action', [
            ['letter of caution', 'letter of caution(carrying out orders)/use of force ()',
            'lette of caution', 'young(letter of caution)/ adkins(n/s)/ fonte(n/s)',
            'conduct unbecoming/letter of caution', 'coo//letter of caution/ uof/',
            'letter of cautin'
            ],
            ['10-day suspension/letter of reprimand/letter of caution',
            'crawford(10-day susp)/iverson(letter of reprimand)/srantz(letter of caution)',
            ],
            ['letter of caution/60-day loss of unit', 'letter of caution/ 60-day loss of unit'
            ],
            ['letter of caution/8-hour driving school', 'letter of caution/8-hourdriving school',
             'letter of caution/ 8-hour drv school', 'letter of caution/ 8-hour driv school',
             'letter of caution/ 8-hourdriving school', 'letter of caution/ 8-hour driving school',
             'letter of caution / 8-hour drv school', 'letter of caution/8-hour drv school'
            ],
            ['letter of caution/8-hour driving school/5-day loss of unit',
            'letter of caution/ 8-hourdriving school/ 5-day loss of unit'
            ],
            ['letter of reprimand/20-day loss of unit', 'letter of reprimand/ 20-day loss of unit'
            ],
            ['letter of reprimand/10-day vehicle suspension', 'letter of reprimand/ vehicle susp 10-days'
            ],
            ['letter of reprimand/15-day loss of unit', 'letter of reprimand / 15-day loss of unit'
            ],
            ['letter of reprimand/5-day vehicle suspension', 'letter of reprimand/veh susp 5-days'
            ],
            ['letter of reprimand/8-hour driving school', 'letter of reprimand/ 8-hour driving school',
             'letter of reprimand/ 8-hourdriving school', 'letter of reprimand / 8-hour driving school',
             'letter of reprimand/ 8-hourdriving school'
            ],
            ['letter of reprimand/8-hour driving school/30-day loss of unit',
             'letter of reprimand/8hourdriving school/30-day loss of unit',
             'letter of reprimand/ 8-hour driving school/ 30-day loss of unit',
             'letter of reprimand/ 8-hourdriving school/ 30-day loss of unit'
            ],
            ['letter of reprimand/8-hour driving school/10-day loss of unit',
             'letter of reprimand/ hour school/ 10-loss of unit',
             'letter of reprimand/ 8-hour driving school/ 10-day loss of unit',
             'letter of reprimand/ 8-hourdriving school/ 10-day loss of unit'
            ],
            ['letter of reprimand/10-day loss of unit', 'letter of reprimand/loss of unit- 10day'
            ],
            ['letter of reprimand/8-hour driving school/15-day loss of unit',
             'letter of reprimand/ 8hourdriving school/ 15-day loss of unit', 
             'letter of reprimand/8-hour driving school/15-loss of unit',
             'letter of reprimand/8-hourdriving school/15-day loss of unit'
            ],
            ['letter of reprimand/8-hour driving school/5-day loss of unit',
            'letter of reprimand/8hourdriving school/ 5-day loss of unit',
            'letter of reprimand/ 8-hourdriving school/ 5-day loss of unit'
            ],
            ['letter of reprimand/8-hour driving school/45-day loss of unit',
             'letter of reprimand/ 8-hourdriving school/ 45-day loss of unit'
            ],
            ['letter of reprimand',
             'letter of reprimand / dwi/roll call training on good samaritan law'
            ],
            ['letter of instruction',
             'letter of instruction - mandatory roli call training on crime scene securing witnesses',
             'letter of instruction - mandatory roll call training on crime scene securing witnesses'
            ],
            ['2-day suspension', '2-day suspension (conduct)'
            ],
            ['20-day suspension/suspension overturned on 1/19/12', '20-day suspension(suspension overturned 1/19/12)'
            ],
            ['30-day suspension/6-month loss of unit', '30-day suspension (consent)/6-mo loss of unit'
            ],
            ['30-day suspension', '30-day susp'
            ],
            ['1-day suspension', 'insub/respect/conduct( 1-day susp)/sexual harrass(n/s)'
            ],
            ['1-day suspension/60-day loss of unit', '1-day suspension-60-day loss of unit'
            ],
            ['5-day suspension without pay', 'conduct(5-day w/o pay)/truthful-'
            ],
            ['5-day suspension', '5-day susp (consent discipline)', '5-day suspension (truthfulness)'
            ],
            ['8-hour driving school/45-day loss of unit', '8-hour class/ 45-day loss of unit'
            ],
            ['3-day suspension', '/ 3-day suspension'
            ],
            ['7-day suspension', 'coo(7-day susp)'
            ],
            ['20-day suspension',
            ],
            ['60-day suspension', '(60-day)', '60-day', 
            ],
            ['65-day suspension/demotion', '65-day susp / demotion/truth(n/s)',
            '65-day susp / demotion/ truth(n/s)'
            ],
            ['10-day suspension',
            ],
            ['90-day suspension'
            ],
            ['80-day suspension',
            ],
            ['25-day suspension', '25-day susp'
            ],
            ['60-day suspension/6-month loss of unit', '60-day suspension / 6-mo loss of unit'
            ],
            ['70-day suspension/30-day loss of unit'
            ],
            ['demotion', '/ demotion' 
            ],
            ['15-day loss of unit', '15-day loss of unit (for no dmvr)'
            ],
            ['5-day loss of unit'
            ],
            ['1-day driving school'
            ],
            ['suspended', 'suspension from rso'
            ],
            ['resigned', 'office investigation/officer resigned', 'office investigation/resigned',
            'officer resigned 6-1-13'
            ],
            ['resigned in lieu of termination'
            ],
            ['2-week loss of extra duty', '/ 2-week loss of extra duty / unit'
            ],
            ['5-day loss of unit', '/ 5-day loss of unit'
            ],
            ['30-day loss of unit', '/ 30-day loss of unit'
            ],
            ['counseling', 'counselec', 'oral reprimand', 'counseled'
            ],
            ['verbal counseling/30-day loss of unit'
            ],
            ['verbal counseling'
            ],
            ['resigned in lieu of suspension', 'ofc investigation/ resigned in lieu of suspension'
            ],
            ['terminated', 'terminated 11/09/12', 'termination'
            ],
            ['mandatory training', 'mandatory training - accident investigation /de- escalation',
            'mandatory training accident investigation /de- escalation'
            ],
            ['firearms training', 'advanced training from firearms supervisor'
            ],
            ['dismissed', 'charges dismissed'
            ],
            ['deferred', 'deferred/ handeled upon rehire', '/ handeled upon re'
            ],
            ['suspension rescinded', 'suspension recinded'
            ],
            ['crisis intervention training', 'cit training / effective decision making']
            
        ])
    return df


def clean_18():
    df = realign_18()
    df = clean_column_names(df)
    df = df.rename(columns={
        "status": "investigation_status",
        "received": "receive_date",
    })
    df = df\
        .pipe(parse_officer_name_18)\
        .pipe(parse_complaint_18)\
        .pipe(
            standardize_desc_cols,
            ["department_desc", "action", "disposition", "rule_violation",
             "paragraph_violation", "investigation_status"])\
        .pipe(drop_office_investigation_rows)\
        .pipe(clean_dates, ["receive_date", "occur_date"])\
        .pipe(assign_tracking_num_18)\
        .pipe(standardize_action_18)\
        .pipe(combine_rule_and_paragraph)\
        .pipe(assign_agency_18)\
        .pipe(assign_data_production_year_18)\
        .pipe(gen_uid, ["agency", "first_name", "middle_initial", "last_name"])\
        .pipe(gen_uid, ['agency', 'tracking_number', 'uid', 'action', 'charges'], 'charge_uid')\
        .pipe(gen_uid, ['charge_uid'], 'complaint_uid')

    return df


if __name__ == "__main__":
    df = clean_18()
    ensure_data_dir("clean")
    df.to_csv(
        data_file_path("clean/cprr_baton_rouge_pd_2018.csv"),
        index=False)
