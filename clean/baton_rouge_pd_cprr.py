from lib.columns import clean_column_names
from lib.clean import clean_dates, standardize_desc_cols, float_to_int_str
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
        data_file_path("raw/baton_rouge_pd/baton_rouge_pd_cprr_2018.csv"),
        encoding="latin1")
    df.rename(columns=lambda x: x.strip(), inplace=True)
    df.rename(columns={"Comptaint": "Complaint"}, inplace=True)

    # manual edits
    df.iloc[4, 2] = "Administrative Review"
    df.iloc[12, 5] = df.iloc[12, 5] + "P10467 Patrol 3rd District"
    df.iloc[12, 6] = df.iloc[12, 6] + "Orders - (Pursuit) - 40"
    for ind in [16, 23, 25, 29]:
        m = re.match(r"^(.+) (\d+\:\d+) *$", df.iloc[ind - 1, 6])
        df.iloc[ind - 1, 6] = m.group(1)
        df.iloc[ind, 6] = "%s %s" % (m.group(2), df.iloc[ind, 6])
    for ind in [39]:
        m = re.match(r"^(\d+) (.+)$", df.iloc[ind + 1, 6])
        df.iloc[ind, 6] = df.iloc[ind, 6] + m.group(1)
        df.iloc[ind + 1, 6] = m.group(2)
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


def drop_office_investigation_rows(df):
    return df[~(df.action == 'office investigation')].reset_index(drop=True)


def clean_status_21(df):
    df.loc[:, 'investigation_status'] = df.status.fillna('').str.lower().str.strip()\
        .str.replace(r'invesligation$', 'investigation', regex=True)\
        .str.replace(r'(- ?|\.)', '', regex=True)\
        .str.replace('&', 'and', regex=False)
    return df.drop(columns='status')


def create_tracking_number_21(df):
    df.loc[:, "tracking_number"] = df.ia_year.apply(str) + '-' + df.ia_seq.apply(str)
    return df.drop(columns=['ia_seq', 'ia_year'])


def clean_receive_occur_dates_21(df):
    df.receive_date = df.receive_date\
        .str.replace('- ', '', regex=False)
    df.occur_date = df.occur_date\
        .str.replace('- ', '', regex=False)\
        .str.replace('. ', '', regex=False)\
        .str.replace('13-Oct', '10/13/2015', regex=False)\
        .str.replace('16-Feb', '2/16/2016', regex=False)\
        .str.replace('16-Jul', '7/16/2017', regex=False)\
        .str.replace('17-Jul', '7/17/2017', regex=False)\
        .str.replace('1092019', '10/9/2019', regex=False)\
        .str.replace('2019-06', '', regex=False)
    return df


def clean_complainant_21(df):
    df.complainant = df.complainant.fillna('').str.lower().str.strip()\
        .str.replace(r'\< ?(\d+) ?\-? (\d+) \>', '', regex=True)\
        .str.replace('-', '', regex=False)\
        .str.replace(r'(h/)?[8b](p|r)(p|r)d?o?i?/? ?', 'brpd', regex=True)\
        .str.replace('damaging dept. equipment', '', regex=False)\
        .str.replace('brpdtim browning', 'brpd/tim browning', regex=False)
    return df


def clean_charges_21(df):
    df.loc[:, 'charges'] = df.complaint.str.lower().str.strip().fillna('')\
        .str.replace(r' >i? ', ' - ', regex=True)\
        .str.replace('.', '', regex=False)\
        .str.replace(',', '', regex=False)\
        .str.replace(r'^use ', '3:20 use ', regex=True)\
        .str.replace(r' [rd]ep[lt]\.? ?', ' department ', regex=True)\
        .str.replace(r'(\d+)\.(\d+)', r'\1:\2', regex=True)\
        .str.replace(r'(2:1[43])? ?d?dmvr ?(violation)? ?(-)? ?(68)?', 
                     '2:14 dmvr violation - 68', regex=True)\
        .str.replace(' - - ', ' - ', regex=False)\
        .str.replace(' / - ', ' / ', regex=False)\
        .str.replace(r'(2:[23])? ?shirking ?(duties)? ?(14)?',
                     '2:2 shirking duties - 14', regex=True)\
        .str.replace(r'^-$', '', regex=True)\
        .str.replace(r'(\w+)/(\w+)', r'\1 / \2', regex=True)\
        .str.replace(r'^3:22$', '3:22 violation of known laws', regex=True)\
        .str.replace(r'(failure to report)? ?(2:[87])? ?damag(ed?|ing)? ?(to)? '
                     r'?(department)? ?(equip?(ment)?)? ?(- 18)?',
                     '2:7 damaging department equipment - 18', regex=True)\
        .str.replace(r'(\w+) - (\(?\w+\)?)', r'\1 \2', regex=True)\
        .str.replace('shooling', 'shooting', regex=False)\
        .str.replace(r'^carr[ry]?(ing)? out orders\b', 
                     '3:17 carrying out orders', regex=True)\
        .str.replace(r'\((veh)? ?(pursuit)?\)$', '(pursuit) - 40', regex=True)\
        .str.replace(r'(^22?:?[12]?2?)? ?command ?(of)? temper ?(13)?', 
                     '2:2 command of temper - 13', regex=True)\
        .str.replace(r'^(viol)?(ation)? ?of (known)? ?laws', 
                     '3:22 violation of known laws', regex=True)\
        .str.replace(r' \/(\w+)', r' / \1', regex=True)\
        .str.replace(r' (\d+) (\d+):(\d+)', r'\1', regex=True)\
        .str.replace(r'(1:7)? ?(fail)?(ure)? ?(to)? ?(comp(lete?)?(ion)?)? '
                     r'?(required)? ?(and)? ?&? ?/? ?(submissions?)? ?(of)? ?(required)? forms? ?(8)?',
                     '1:7 failure to submit required forms', regex=True)\
        .str.replace(r'^insubordination$', '3:18 insubordination - 43', regex=True)\
        .str.replace(r'(2:5)? ?awol ?(15)?', '2:5 absent without leave - 15', regex=True)\
        .str.replace(r'^punctuality$', '1:5 punctuality - 6', regex=True)\
        .str.replace(r'^truthfulness$', '3:23 truthfulness - 58', regex=True)\
        .str.replace(r'^sexual harrassment$', '3:14 sexual harassment - 37', regex=True)\
        .str.replace(r'^(release of prisoner)? ?/? ?allow(ing)? ?escape ?(30)?', 
                     '3:7 release of prisoners / allowing escape - 30', regex=True)\
        .str.replace(r'(2:1[23])? ?respect ?(of)? ?(fellow)? ?(officers|members)? '
                     r'?(22)?', '2:13 respect of fellow officers - 22', regex=True)\
        .str.replace(r'(3:9)? ?fail(ure)? ?(to)? ?(provide)? '
                     r'?info(rmation)? ?(to superior)? ?(32)?',
                     '3:9 failure to provide information to superior - 39', regex=True)\
        .str.replace(r'(\w+)-', r'\1', regex=True)\
        .str.replace('incar', 'in car', regex=False)\
        .str.replace('accidentl discharge', 'accidental shooting', regex=False)\
        .str.replace(r'^assc with known criminals$', 
                     '3:21 association with known criminals - 55', regex=True)\
        .str.replace(r'\(contact person\)45 failure to$', 
                     '(contact person) - 45', regex=True)\
        .str.replace(r'desertion', '2:5 absent without leave - 15', regex=False)\
        .str.replace(r'^cowardice$', '3:15 cowardice - 38', regex=True)\
        .str.replace(r'^confidentiality$', '3:8 confidentiality - 31', regex=True)\
        .str.replace(r'^unauth public statements$', 
                     '3:5 unauthorized public statements - 27', regex=True)\
        .str.replace(r'^fals[ei]f?i?(cation)? ?(of)? documents', 
                     '3:19 falsification of documents - 44')\
        .str.replace(r'dl suspension', "suspended driver's license", regex=False)\
        .str.replace(' i intermediate weapon ', ' / intermediate weapon - ', regex=False)\
        .str.replace(r'^320\b', '3:20', regex=True)\
        .str.replace(r' a shooting', ' / shooting', regex=False)\
        .str.replace(r'\binvest\b', 'investigation', regex=True)\
        .str.replace(r' (\(drugs\))? ', ' - ', regex=True)\
        .str.replace(r'^(fai[tl])?(urr?e)? ?(to)? ?(secu[tr]e)? '
                     r'?(property)? ?/? ?(or)? ?(evid(ence)?)? ?-? ?2?6?$',
                     '3:4 failure to secure property or evidence - 26', regex=True)\
        .str.replace(r' (\(?\w+\)?) (\d+)$', r' \1 - \2 ', regex=True)\
        .str.replace('discharge firearm', 'firearm discharge', regex=False)\
        .str.replace(r'non contact', 'non-contact', regex=False)\
        .str.replace('failure to report lost / 2:7 damaging department equipment - 18',
                     '2:7 damaging department equipment - 18 / failure to report lost', regex=False)\
        .str.replace(r'-(\d+)$', r'- \1', regex=True)\
        .str.replace(r' (\w+)(\d{2})$', r'\1 - \2', regex=True)\
        .str.replace('1818', '18', regex=False)\
        .str.replace(r'^fail notify superv$', 'failure to notify supervisor', regex=True)\
        .str.replace(r'(2:?1[12])? ?(conduct)? ?unbecoming ?(an)? ?(officer)? ?-? ?(21)? ?(violation)?',
                     '2:12 conduct unbecoming an officer - 21', regex=True)\
        .str.replace(r'(\w+) - (\d+) (\(\w+\))', r'\1 \3 - \2', regex=True)\
        .str.replace(r'^abuse of sick leave$', '2:3 abuse of sick leave - 66', regex=True)\
        .str.replace(r'(\d+)(\(\w+\))$', r'\1 \2', regex=True)
    return df.drop(columns='complaint')


def parse_officer_name_21(df):
    dep = df.officer_name.str.replace(
        r'^(.+), (PC?\d+) (.+)$', r'\1 # \2 # \3').str.split(' # ', expand = True)
    dep.columns = ['name', 'department_code', 'department_desc']
    dep.loc[:, 'name'] = dep['name'].str.lower().str.strip()

    df = pd.concat([df, dep], axis=1)
    return df.dropna(subset=['name']).reset_index(drop=True).drop(columns='officer_name')


def split_name_21(df):
    df.name = df.name.str.lower().str.strip()\
        .str.replace(r'(\w+), (\w+)', r'\2 \1', regex=True)\
        .str.replace(',', '', regex=False)\
        .str.replace('.', '', regex=False)\
        .str.replace(r'(\w+) (\b\w{1}) (\b\w{2}\b) (?:(p\d+) )?', 
                     r'\2 \1 \3 \4 ', regex=True)\
        .str.replace(r' (\w+) (\b\w{1}\b) (?:(p\d+) )?', 
                     r' \2 \1 \3 ', regex=True)\
        .str.replace(r'^(\w+) (\w+) (\w{1})$', r'\1 \3 \2', regex=True)\
        .str.replace(r'^(\w{2}) (\w+) (\w+) (\w{1})$',
                     r'\2 \4 \3 \1', regex=True)\
        .str.replace(r'^(\w+) (\w{2}) (\w+) (\w+)$', 
                     r'\1 \3 \4 \2', regex=True)\
        .str.replace(r'^(\w+) (\w{2}) (\w+) (p\d+) (\w{1})$', 
                     r'\1 \5 \3 \2 \4', regex=True)\
        .str.replace(r'^(\w+) (\w+) (\w{1}) (\w{2})', 
                     r'\1 \3 \2 \4', regex=True)\
        .str.replace(r'^(\w+) (\w{3}) (\w+) (\w+)', 
                     r'\3 \4 \1 \2', regex=True)\
        .str.replace(r'(\w{3}) (\w{1}) (\w+)  (\w+)', 
                     r'\1 \3 \2 \4', regex=True)\
        .str.replace(r'^(\w+) (\w+) (\w{3})$', 
                     r'\1 \3 \2', regex=True)\
        .str.replace(r'^(\w{1}) (\w+) (\w+)$', 
                     r'\2 \1 \3', regex=True)\
        .str.replace(r'^special operations traffic homicide$', '', regex=True)\
        .str.replace(r' (p\d+) ?(.+)?', '', regex=True)\
        .str.replace('none', '', regex=False)\
        .str.replace('duane lt scrantz', 'lt duane scrantz', regex=False)\
        .str.replace(r'^\bll\b', 'lt', regex=True)\
        .str.replace(r'^\bcpi\b', 'cpl', regex=True)\
        .str.replace('task force', '', regex=False)\
        .str.replace('-', '', regex=False)\
        .str.replace('krumm amy e', 'amy e krumm', regex=False)\
        .str.replace(r'passman (m|jon) (jon|m)', 'jon m passman', regex=True)
    names = df.name.str.lower().str.strip().str.extract(
        r'(?:(lt|cpl|capt|ofc|sgt|maj|det) )?(?:([^ ]+) )?(\w+ )?(.+)')
    df.loc[:, 'rank_desc'] = names[0].replace({
            'sgt': 'sergeant',
            'lt': 'lieutenant',
            'cpl': 'corporal',
            'ofc': 'officer',
            'capt': 'captain',
            'maj': 'major',
            'det': 'detective'
        })
    df.loc[:, 'first_name'] = names[1]\
        .str.strip()
    df.loc[:, 'last_name'] = names[3]\
        .str.strip()
    df.loc[:, 'middle_name'] = names.loc[:, 2].str.strip().fillna('')\
        .map(lambda s: '' if len(s) < 2 else s)
    df.loc[:, 'middle_initial'] = names.loc[:, 2].str.strip().fillna('')\
        .map(lambda s: '' if len(s) > 2 else s)
    return df.drop(columns=['name'])


def split_department_and_division_desc_21(df):
    df.department_desc = df.department_desc.str.lower().str.strip().fillna('')\
        .str.replace('patro)', 'patrol', regex=False)\
        .str.replace('&', 'and', regex=False)\
        .str.replace(r'\bop\b', 'operations', regex=True)\
        .str.replace(r'((operation service|communications)? comm center)$', 
                     'operations service communications center', regex=True)\
        .str.replace(r'\bcib\b', 'criminal investigations', regex=True)\
        .str.replace(r'^special$|^special operations tru$', 'special operations', regex=True)\
        .str.replace(r'\bcommunications communications\b', 'communications', regex=True)\
        .str.replace('criminal investigations criminal investigations', 
        'criminal investigations', regex=False)
    names = df.department_desc\
        .str.extract\
    (r'(patrol|operation service|administration|'
     r'special operations|criminal investigations) (.+)')
    df.department_desc = names[0]
    df.loc[:, 'division_desc'] = names[1]
    return df


def clean_disposition_21(df):
    df.disposition = df.disposition.str.lower().str.strip().fillna('')\
        .str.replace(r'/?admin\.?,?', 'admin', regex=True)\
        .str.replace('.', '', regex=True)\
        .str.replace(r'^/?', '', regex=True)\
        .str.replace(r'pre-? ?disc ?(hearing)?', 
                     'pre-disciplinary hearing', regex=True)\
        .str.replace(r'sust\.?\b', 'sustained', regex=True)\
        .str.replace(r'(\w+)- (\w+)', r'\1-\2', regex=True)\
        .str.replace(r'(\w+)/ (\w+)', r'\1/\2', regex=True)\
        .str.replace(r'pre-? ?term(ination)? ?(hearing)?', 
                     'pre-termination hearing', regex=True)\
        .str.replace(r'\binv\b', 'investigation', regex=True)\
        .str.replace(r'\bofc\b', 'office', regex=True)\
        .str.replace('referraladmin', 'referral/admin', regex=False)\
        .str.replace(r'^- ?-?$', '', regex=True)\
        .str.replace('referra)', 'referral', regex=False)
    return df


def consolidate_action_and_disposition_21(df):
    df.disposition = df.disposition.str.cat(df.action, sep='|')\
        .str.replace(r'^/', '', regex=True)\
        .str.replace('loc', 'letter of caution', regex=False)\
        .str.replace('lor', 'letter of reprimand', regex=False)\
        .str.replace(r'l\.?o\.?u\.?', 'loss of unit', regex=True)\
        .str.replace(r' ?sust?(alned)?\.?,?(l?aine?d)?\b', 
                     'sustained', regex=True)\
        .str.replace(r'hr\.?\b', 'hour', regex=True)\
        .str.replace(r'(\d+)-? (\w+)', r'\1-\2', regex=True)\
        .str.replace(r',|\.', '', regex=True)\
        .str.replace(r'\bnotsustained\b', 'not sustained', regex=True)\
        .str.replace(r'^hord/not sustained blust/not sustained$', 
                     'not sustained', regex=True)\
        .str.replace(r'^-$', '', regex=True)
    df.disposition = df.disposition.str.extract(
                     r'(sustained|not sustained|exonerated)')
    return df


def clean_action_21(df):
    df.action = df.action.str.lower().str.strip().fillna('')\
        .str.replace(r'^//?/?', '', regex=True)\
        .str.replace(';', '/', regex=False)\
        .str.replace(r' ?(\w+); (\w+) ?', r'\1/\2', regex=True)\
        .str.replace(r' ?(\w+) ?/ (\w+)/? ?', r'\1/\2', regex=True)\
        .str.replace(r' (\w+) /(\w+)-? ', r' \1/\2 ', regex=True)\
        .str.replace('loc', 'letter of caution', regex=False)\
        .str.replace('lor', 'letter of reprimand', regex=False)\
        .str.replace(r'l\.?o\.?u\.?', 'loss of unit', regex=True)\
        .str.replace(r' ?sust?/?(alned)?\.?,?(l?aine?d)?/?\b', '', regex=True)\
        .str.replace(r'/?not ?(sustained)?/? ?/?', '', regex=True)\
        .str.replace(r'(\d+)-? (\w+)', r'\1-\2', regex=True)\
        .str.replace(r' ?(8)?(-)?hr\.? ?(dr)?i?v?\.?(iv?ng)? ?(sch)?(ool)? ?', 
                     '8-hour driving school', regex=True)\
        .str.replace(',', '/', regex=False)\
        .str.replace('.', '', regex=False)\
        .str.replace(':', '/', regex=False)\
        .str.replace(r' ?& ?', '  ', regex=True)\
        .str.replace(r'^#name\?$', '', regex=True)\
        .str.replace(r' ?\bsusp\b ?', ' suspension', regex=True)\
        .str.replace('effective decision making', '', regex=False)\
        .str.replace(r'mandatory training -? ?(accident)? ?investigation/de escalation',
                     'mandatory accident and de-escalation training', regex=True)\
        .str.replace('advanced training from firearms supervisor',
                     'advanced firearms training', regex=False)\
        .str.replace('letterof', 'letter of', regex=True)\
        .str.replace(r'/? ?conduct|/? ?truthfulness?|/? ?\bn/s\b', '', regex=True)\
        .str.replace(r'(\d)-mo', r'\1-month', regex=True)\
        .str.replace(r'(\w+)- (\d+)(\w+)', r'\1 \2-\3', regex=True)\
        .str.replace('uof', 'use of force', regex=False)\
        .str.replace(r'/?crawford|/?iverson|/?srantz|/?hernandez|'
                     r'/?thomas|/?ofc|/?cowardice|/?adkins', '', regex=True)\
        .str.replace(r'/?(unbecoming/?|/?officer?|/? ?investig?ation/? ?'
                     r'|/? ?coo/?| from rso)', '', regex=True)\
        .str.replace(r'/? ?shirking ?|/? ?truth(ful?)? ?|hord/?|'
                     r'blust/?|/?insub/?|/?respect/?|/?sexual harrass', '', regex=True)\
        .str.replace(r'/?consent|/?handled by lt/? dabadie|/?discipline'
                     r'|/?carrying out orders|/?use of force/?', '', regex=True)\
        .str.replace('vehiclesusp 10-days' ,' 10-day loss of unit', regex=True)\
        .str.replace('veh suspension5-days', '5-day loss of unit')\
        .str.replace(r'(\d{2})(\w{3})\b', r'\1-\2', regex=True)\
        .str.replace(r'^(\d{2})-(\w{3})(\w+)', r'\1-\2 \3', regex=True)\
        .str.replace(r'(\w+)-(\d+)', r'\1/\2', regex=True)\
        .str.replace(r'(\w+)  (\d{2})', r'\1/\2', regex=True)\
        .str.replace(r'(\w+) (\d{2})', r'\1/\2', regex=True)\
        .str.replace('oral reprimand', 'verbal counseling', regex=False)\
        .str.replace(r'letter? ?(of)? c?autio?n/?', 
                     'letter of caution', regex=True)\
        .str.replace('counselec', 'counseled', regex=False)\
        .str.replace('resignedin', 'resigned in', regex=False)\
        .str.replace('suspensionrecinded', 'suspension rescinded', regex=False)\
        .str.replace(r'/?no action taken/|\(|\)?|^//?|^-$|^-', '', regex=True)\
        .str.replace(r' (\w+)/ (\d+)(\w{3}) ', r'\1/\2-\3 ', regex=True)\
        .str.replace(r'-loss', '-day loss', regex=False)\
        .str.replace('5day', '5-day', regex=False)\
        .str.replace(r' ?(\w+)/ (\d+)-(\w+) ', r'\1/\2-\3 ', regex=True)\
        .str.replace(r'^/', '', regex=True)\
        .str.replace('suspensionsuspension', 'suspension/suspension', regex=False)\
        .str.replace(r'(\w+) /(\d+)-(\w+)', r'\1/\2-\3', regex=True)\
        .str.replace(r' (\w+) demotion', r' \1/demotion', regex=True)\
        .str.replace(r'(\w+)/ (\w+)', r'\1/\2', regex=True)\
        .str.replace(r' (\w+)  (\d+)-(\w+) ', r' \1/\2-\3 ', regex=True)\
        .str.replace(r'letter of instruction - mandatory roll?i? '
                     r'call training on crime scene securing witnesses',
                     'letter of instruction/mandatory crime scene '
                     'and securing witnesses training', regex=True)\
        .str.replace('letter of reprimand  dwi/roll call training on good samaritan law', 
                     'letter of reprimand/good samaritan law training', regex=False)\
        .str.replace(r'extra duty  unit', 'extra duty and unit', regex=False)\
        .str.replace('drivingschool', 'driving school', regex=False)\
        .str.replace('loss of unit/10-day', '10-day loss of unit', regex=False)\
        .str.replace('terminated', 'termination', regex=False)\
        .str.replace(r'/11/09/12| 6/1-13|per capt bloom|young|/ adkins|'
                     r'/ fonte|class|/?conference worksheet/?', '', regex=True)\
        .str.replace(r'/$', '', regex=True)\
        .str.replace('exonerated', '', regex=False)\
        .str.replace(r'5-day w/o pay-', '5-day without pay', regex=True)\
        .str.replace('10-day suspensionletter of reprimandletter of caution',
                     '10-day suspension/letter of reprimand/letter of caution', regex=False)\
        .str.replace(r' (\w+)(\d{2})', r'\1/\2', regex=True)\
        .str.replace(r'ofreprimand', 'of reprimand', regex=True)\
        .str.replace(r' ?of ?caution/?', 'of caution', regex=True)\
        .str.replace(r' caution(\d{1})', r' caution/\1', regex=True)\
        .str.replace(r'\bletterof\b ', 'letter of ', regex=True)
    return df


def drop_rows_with_charges_disposition_action_all_empty_21(df):
    return df[~((df.charges == '') & (df.disposition == '') & (df.action == ''))]


def assign_prod_year(df, year):
    df.loc[:, "data_production_year"] = year
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "Baton Rouge PD"
    return df


def clean_21():
    df = pd.read_csv(data_file_path(
        'raw/baton_rouge_pd/baton_rouge_pd_cprr_2021.csv'))\
        .pipe(clean_column_names)
    df = df\
        .rename(columns={
            'received': 'receive_date'
        })\
        .pipe(clean_status_21)\
        .pipe(float_to_int_str, ['ia_seq', 'ia_year'])\
        .pipe(create_tracking_number_21)\
        .pipe(clean_receive_occur_dates_21)\
        .pipe(clean_complainant_21)\
        .pipe(clean_dates, ['receive_date', 'occur_date'])\
        .pipe(clean_charges_21)\
        .pipe(clean_action_21)\
        .pipe(parse_officer_name_21)\
        .pipe(split_name_21)\
        .pipe(split_department_and_division_desc_21)\
        .pipe(clean_disposition_21)\
        .pipe(consolidate_action_and_disposition_21)\
        .pipe(standardize_desc_cols, 
            ['charges', 'action', 'disposition',
            'department_code', 'department_desc'])\
        .pipe(drop_rows_with_charges_disposition_action_all_empty_21)\
        .pipe(assign_agency)\
        .pipe(assign_prod_year, '2021')\
        .pipe(gen_uid, ["agency", "first_name", "middle_initial", "last_name"])\
        .drop_duplicates(subset=['uid', 'tracking_number', 'charges', 'disposition', 'action'], keep='first')\
        .pipe(gen_uid, ['agency','uid', 'charges', 'tracking_number', 'action'], 'complaint_uid')\
        .pipe(standardize_from_lookup_table, 'charges', [
            ['1:5 punctuality - 6'],
            ['1:7 failure to submit required forms'],
            ['1:8 wearing of uniforms / uniforms - 9'],
            ['1:9 chain of command - 63'],
            ['2:1 use of alcohol / drugs - 12'],
            ['2:12 conduct unbecoming an officer - 21', 'conduct'],
            ['2:12 conduct unbecoming an officer - 21 (harassment)'],
            ['2:13 respect of fellow officers - 22'],
            ['2:14 dmvr violation - 68'],
            ['2:2 command of temper - 13'],
            ['2:2 shirking duties - 14'],
            ['abuse of sick leave - 66', '2:4 abuse of sick leave - 66',
            '2:3 abuse of sick leave - 66'],
            ['2:5 absent without leave - 15'],
            ['2:7 damaging department equipment - 18'],
            ['2:7 damaging department equipment - 18 / failure to report lost'],
            ['3:13 assault on a member - 36'],
            ['3:14 sexual harassment - 37'],
            ['3:15 cowardice - 38'],
            ['3:17 carrying out orders'],
            ['3:17 carrying out orders (in car camera)'],
            ['3:17 carrying out orders (pursuit) - 40'],
            ['3:17 carrying out orders / general orders (pursuit) - 40',
            '3:17 carrying out orders a general orders (pursuit) - 40'],
            ['3:17 carrying out orders / memorandums - 41'],
            ['3:17 carrying out orders / verbal orders - 42'],
            ['3:18 insubordination - 43'],
            ['3:19 falsification of documents - 44'],
            ['3:20 use of alcohol / drugs'],
            ['3:20 use of force'],
            ['3:20 use of force (accidental discharge)'],
            ['3:20 use of force (accidental shooting)'],
            ['3:20 use of force (bean bag department oy)'],
            ['3:20 use of force (contact person)'],
            ['3:20 use of force (dog bite)'],
            ['3:20 use of force (dog shooting)'],
            ['3:20 use of force (firearm discharge)'],
            ['3:20 use of force (shooting non-contact animal)'],
            ['3:20 use of force (shooting non-contact)'],
            ['3:20 use of force (shooting)'],
            ['3:20 use of force / hard empty hand - 53'],
            ['3:20 use of force / in custody no officer contact - 64'],
            ['3:20 use of force / intermediate weapon - 52'],
            ['3:20 use of force / shooting (animal) - 48'],
            ['3:20 use of force / shooting (contact person) - 45'],
            ['3:20 use of force / shooting (non-contact) - 47'],
            ['3:20 use of force / soft empty hand - 54'],
            ['3:20 use of narcotics'],
            ['3:20 use of tobacco'],
            ['3:21 association with known criminals - 55'],
            ['3:22 violation of known laws'],
            ['3:22 violation of known laws / reporting arrests or summons - 57'],
            ['3:22 violation of known laws / violations - 56'],
            ['3:23 truthfulness - 58'],
            ['3:4 failure to secure property or evidence - 26'],
            ['3:5 unauthorized public statements - 27'],
            ['3:6 unauthorized investigations / instituting investigation - 28'],
            ['3:7 release of prisoners / allowing escape - 30'],
            ['3:8 confidentiality - 31'],
            ['3:9 failure to provide information to superior - 39'],
            ['coo'],
            ['failure to notify supervisor'],
            ['failure to report accident'],
            ['information only'],
            ['interdepartmental cooperation between agencies'],
            ['residence and telephone'],
            ["suspended driver's license"],
            ['traffic violations']
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
        .pipe(assign_agency)\
        .pipe(assign_prod_year, '2020')\
        .pipe(gen_uid, ["agency", "first_name", "middle_initial", "last_name"])\
        .pipe(gen_uid, ['agency', 'tracking_number', 'uid', 'action', 'charges'], 'complaint_uid')
    return df


if __name__ == "__main__":
    df18 = clean_18()
    df21 = clean_21()
    ensure_data_dir("clean")
    df18.to_csv(
        data_file_path("clean/cprr_baton_rouge_pd_2018.csv"),
        index=False)
    df21.to_csv(
        data_file_path("clean/cprr_baton_rouge_pd_2021.csv"),
        index=False)
