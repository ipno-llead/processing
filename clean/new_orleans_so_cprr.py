from os import pipe
import sys
sys.path.append("../")
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.path import data_file_path, ensure_data_dir
from lib.clean import clean_dates, clean_names, standardize_desc_cols
from lib.uid import gen_uid
from lib.standardize import standardize_from_lookup_table


charges_lookup = [
  ['adherence to law professionalism'],
  ['associations'],
  ['associations knowledge of policies procedures'],
  ['ceasing to perform work before end of tour of duty'],
  ['cooperation/instructions from a supervisor or higher ranked staff member/leaving assigned area'],
  ['cooperation/instructions from supervisor/neglect of duty'],
  ['courage/instructions from a supervisor or higher ranked staff member/neglect of duty'
  ,'courage instructions from a supervisor or higher ranked staff member/neglect of duty'],
  ['courage/instructions from a supervisor or higher ranked staff member/neglect of duty-general'],
  ['courtesy'],
  ['courtesy/professionalism', 'courtesy professionalism'],
  ['courtesy/instructions from supervisor/neglect of duty'],
  ['courtesy/intimidation/professionalism'],
  ['courtesy/neglect of duty-failure to act/neglect of duty/instructions from supervisor or higher ranking staff member'],
  ['courtesy/professionalism/cooperation'],
  ['courtesy/professionalism/cooperation/instructions from supervisor or higher ranked staff member'],
  ['courtesy/professionalism/cooperation/instructions from supervisor or higher ranked staff member/neglect of duty/failure to act'],
  ['courtesy/professionalism/cooperation/neglect of duty'],
  ['courtesy/professionalism/instructions from supervisor or higher ranked staff member'],
  ['courtesy/professionalism/neglect of duty/neglect of duty-failure to act/false or inaccurate reports knowledge of policies procedures',
  'courtesy/professionalism/neglect of duty/neglect of duty-failure to act/// /false or inaccurate reports knowledge of policy procedures'],
  ['courtesy/truthfulness/professionalism/instructions from supervisor or higher ranking staff member/false or inaccurate reports',
  'courtesy/truthfulness/professionalism/instructions from supervisor or higher/false or inaccurate reports'],
  ['courtesy/unauthorized foece/physical intimidation professionalism'],
  ['devoting entire time to duty'],
  ['devoting entire time to duty neglect of duty-failure to act',
  'devoting entire time to duty neglect of duty - failure to act '],
  ['devoting entire time to duty/neglect of duty-failure to act/false or inaccurate reports',
  'devoting entire time to duty/neglect of duty /failure to act   false or inaccurate reports'],
  ['devoting entire time to duty/neglect of duty/neglect of duty-failure to act/ceaseing to perform work before end of tour of duty/leaving assigned area',
  'devoting entire time to duty/neglect of duty/neglect of duty-failure to act  /ceaseing to perform work before end of tour/leaving assigned area'],
  ['devoting entire time to duty/neglect of duty-supervisory responsibilities/knowledge of policy procedures'],
  ['instructions from a supersior or higher ranked staff member','instructions from a supervisor or higher ranked staff member',
  'insstruction from a supervisor or higher ranked staff member'],
  ['instructions from a supervisor or higher ranked staff member/knowledge of policy procedures',
  'instructions from a supervisor or higher ranked staff member knowledge of policies procedures'],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty-general',
  'instructions from a supervisor or higher ranked staff member neglect of duty-general'],
  ['instructions from a supervisor or higher ranked satff member'],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty-general',
  'instructions from a supervisor or higher ranked staff member  neglect of duty-general'],
  ['instructions from a supervisor or higher ranked staff member/devoting entire time to duty/possession of cellular phones/device in secure custody area'],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty-general/knowledge of orleans parish sheriff office policies procedures',
  'instructions from a supervisor or higher ranked staff member/neglect of duty-general/knowledge of opso policies procedures'],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty/failure to act'
  ,'instructions from a supervisor or higher ranked staff member/neglect of duty/failure to act #'],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty-general'],
  ["instructions from a supervisor or higher ranked staff member/neglect of duty-general/ceasing to perform work before end of tour of duty/leaving assigned area/knowledge orleans parish sheriff's office policies procedures"],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty-general/neglect of duty-failure to act '],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty-general/neglect of duty-failure to act '],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty/leaving assigned area'],
  ['instructions from a supervisor or higher ranked staff member/neglect of duty/supervisory responsibilities'],
  ['instructions from a supervisor or hiigher ranked staff member'],
  ["instructions from a suspervisor or higher ranked staff member/neglect of duty/failure to act/sick leave/worker's compensation/injury reporting"
  ,"instructions from a suspervisor  or higher ranked staff member/neglect of duty/failure to act/sick leave/worker's compensation/injury reporting"],
  ['instructions from authoritative source/leaving assigned area/ceasing to perform work before the end of tour of duty'],
  ['instructions from higer ranking staff member'],
  ['instructions from higher ranking staff member/ceasing to perform before the end of tour of duty'],
  ['instructions from supervisor or higher ranked staff member'
   ,'instructions from supervisor of higher ranked staff member'],
  ['instructions from supervisor of higher ranked staff member/neglect of duty','instructions from supervisor of higher ranked staff member neglect of duty'],
  ['instructions from supervisor or higher ranked staff member',
  'instructions froma a supervisor or higher ranked staff member',
  'instructions froma supervisor or higher ranked staff member'],
  ['instructions from supervisor or higher ranked staff member/supervisory responsibilities',
  'instructions from supervisor or higher ranked staff member  supervisory responsibilities'],
  ['instructions from supervisor or higher ranked staff member/devoting entire time to duty/use of departmental property'],
  ['instructions from supervisor or higher ranked staff member/neglect of duty/leaving assigned area'],
  ['instuctions from a supervisor or higher ranked staff mmeber/neglect of duty-general'],
  ['intimidation/professionalism', 'intimidation professionalism'],
  ['intimidation/professionalism/instructions from a supervisor or higher ranking staff member/neglect of duty'],
  ['intimidation/professionalism/knowledge of policy procedures',
  'knowledge of policiy procedures'],
  ['leaving assigned area'],
  ['moral conduct/professionalism/devoting entire time to duty'],
  ['neatness attire'],
  ['neatness attire/instructions from a supervisor or higher ranked staff member'],
  ['negelct of duty/ceasing to perform work before end of tour of duty/knowledge of policy procedures'],
  ['negelct of duty/supervisory responsibilities/knowledge of policy procedures',
  'negelct of duty-supervisory responsibilities/knowledge of policy procedures'],
  ['negelect of duty-failure to act'],
  ['neglect of duty-failure to act/instructions from rank',
  'neglect of dtuy-failure to act instructions from rank'],
  ['neglect of duty'],
  ['neglect of duty-failure to act/leaving assigned area',
  'neglect of duty- failure to act/leaving assigned area'],
  ['neglect of duty-general/neglet of duty-failure to act'],
  ['neglect of duty/leaving assigned area',
  'neglect of duty leaving assigned area'],
  ['neglect of duty-failure to act', 'neglect of duty neglect of duty-failure to act'],
  ['neglect of duty/possessesion of cellular device',
  'neglect of duty possessesion of cellular device'],
  ['neglect of duty/failure to act',
  'neglect of duty-failure to act'],
  ['neglect of duty/failure to act/leaving assigned area',
  'neglect of duty/failure to act  leaving assigned area'],
  ['neglect of duty-general',
  'neglect of duty?general'],
  ['neglect of duty-general/leaving assigned area/ceasing to perform before the end of tour of duty',
  'neglect of duty-general/leaving assigned area/ceassing to perform beofore the end of tour of duty'],
  ['neglect of duty-general/neglect of duty-failure to act'],
  ['neglect of duty-general/neglect of duty-failure to act/knowledge of policy procedures'],
  ['neglect of duty-general/neglect of duty-failure to act'],
  ["neglect of duty/instructions from supervisor or rank/sick leave/worker's compensation/injury reporting"],
  ['neglect of duty/neglect of duty failure to act/ceasing to perform work before end of tour of duty'],
  ['neglect of duty/neglect of duty-failure to act',
  'neglect of duty/neglect of duty-failure to act/neglect of duty-failure to act neglect of duty-failure to act'],
  ["neglect of duty/neglect of duty-failure to act/knowledge of orleans parish sheriff's office policy procedures"],
  ["neglect of duty/neglect of duty-failure to act/knowledge of orleans parish sheriff's office policy procedures",
  'neglect of duty/neglect of duty-failure to act/knowledge of policy procedures'],
  ['neglect of duty/supervisory responsibilities',
  'neglect of duty/supervisosry responsibilities'],
  ['neglect of duty/supervisory responsibilities/neglect of duty-failure to act',
  'neglect of duty/supervisosry responsibilities neglect of duty/failure to act'],
  ['posession of cellular phones/devices in secure custody areas'],
  ['professionalism/neglect of duty/neglect of duty-failure to act/ceasing to perform work before end of tour of duty/leaving assigned area',
  'proferssionalism/neglect of duty/neglect of duty/failure to act/ceasing to perform work before end of tour of duty/leaving assigned area'],
  ['professionalism'],
  ['professionalism/instructions from a supervisor or higher ranked staff member',
  'professionalism instructions from a supervisor or higher ranked staff member'],
  ['professionalism/interferring with investigations',
  'professionalism interferring with investigations'],
  ['professionalism/knowledge of policy procedures',
  'professionalism knowledge of policy procedures'],
  ['professionalism/neatness attire/instructions from a supervisor or higher ranked staff member',
  'professionalism neatness attire/ instructions from a supervisor or higher ranked staff member'],
  ['professionalism/neglect of duty',
  'professionalism neglect of duty'],
  ['professionalism/sexual harassment', 'professionalism- sexual harassment'],
  ['professionalism/use of departmental property',
  'professionalism use of departmental property'],
  ['professionalism/associations'],
  ['professionalism/cooperation/instructions from supervisor or higher ranked staff member',
  'professionalism/cooperation/instructions froma supervisor or higher ranked staff member'],
  ['professionalism/devoting entire time to duty/neglect of duty/knowledge of policy procedures'],
  ['professionalism/instructions from a supervisor or higher ranked staff member/ceasing to perform before the end of tour of duty',
  'professionalism/instructions from a supervisor or higher ranked staff member/ceasing to perfrom before the end of tour of duty'],
  ['professionalism/neglect of duty/ceasing to perform before end of tour of duty'],
  ['professionalism/neglect of duty/ceasing to perform before end of tour of duty/leaving assigned area',
  'professionalism/neglect of duty/ceasing to perform before end of tyour of duty/leaving assigned area'],
  ['professionalism/neglect of duty-general/instructions from a supervisor or higher ranked satff member'],
  ['professionalism/neglect of duty-failure to act'],
  ['professionalism/neglect of duty/policies procedures',
  'professionalism/neglect ofduty/policies procedures'],
  ['professionalism/supervisory responsibilities/knowledge of policy procedures',
  'professionalism/professionalism/professionalism/supervisory responsibilities/knowledge of policy procedures'],
  ['professionalism/reporting for duty/instructions from supervisors/neglect of duty-failure to act/cooperation'],
  ['prohibited discussions'],
  ['reporting for duty/instructions from a supervisor or higer ranked staff member/neglect of duty',
  'reeporting for duty/instructions from a supervisor or higer ranked staff member/neglect of duty'],
  ['reporting for duty', 'reportiing for duty', 'reportimg for duty', 'reporting  for duty', 'reporting foe duty',
  'reporting for  duty', 'reportitng for duty', 'repoting for duty'],
  ['reporting for duty/neglect of duty', 'reporting for duty neglect of duty'],
  ['reporting for duty/neglect of duty-failure to act',
  'reporting for duty neglect of duty-failure to act'],
  ['reporting for duty/instructions from supervisor or higher ranked staff member/neglect of duty-failure to act',
  'reporting for duty/instrsuctions from a supervisor or higher ranked staff member/neglect of duty-failure to act'],
  ['reporting for duty/instructions from a supervisor or higher ranked staff member/neglect of duty-failure to act',
  'reporting for duty/instructions from a superviosr or higher ranked staff member/neglect of duty-failure to act',
  'reporting for duty/instructions from a superviosr or higher ranked staff member/neglect of duty-failure to act '],
  ['reporting for duty/instructions from supervisor or higher ranked staff member'],
  ['reporting for duty/instructions from supervisor or higher ranked staff member/neglect of duty'],
  ['reporting for duty/instructions from supervisor or higher ranked staff member/neglect of duty/neglect of duty-failure to act'],
  ['sick leave/workers compensation/injuring reporting'],
  ['supervisory resopnsibility'],
  ['truthfulness'],
  ['truthfulness instructions/from a supervisor or higher ranked staff member',
  'truthfulness instructions from a supervisor or higher ranked staff member',
  'truthfulness instructions from higher ranked staff member'],
  ['truthfulness/neglect of duty', 'truthfulness neglect of duty'],
  ['truthfulness/professionalism/associations', 'truthfulness professionalism associations'],
  ["truthfulness/sick leave/worker's compensation",
  "truthfulness sick leave/worker's compensation"],
  ['truthfulness/courage/neglect of duty'],
  ['truthfulness/devoting entire time to duty/neglect of duty/false or inaccurate reports/knowledge of policy procedures'],
  ['truthfulness/false or inaccurate reports/neglect of duty-general'],
  ["truthfulness/instructions from a supervisor or higher ranked staff member/neglect of duty-general/knowledge of orleans parish sheriff's office policy procedures",
  "truthfulness/instructions from a supervisor  or higher ranked staff member/neglect of duty-general/knowledge of orleans parish sheriff's office policy procedures"],
  ['truthfulness/instructions from supervisor or higher ranked staff member staff member/neglect of duty-general',
  'truthfulness/instructions from a supervisor or higher ranked staff member staff member/ neglect of duty-general'],
  ['truthfulness/instructions from supervisor or higher ranked staff member/neglect of duty-failure to act',
  'truthfulness/instructions from supervisor or higher ranked staff member/ neglect of duty-failure to act'],
  ['truthfulness/neglect of duty-general/professionalism/instructions from supervisor or higher ranked staff member',
  'truthfulness/neglect of duty-general/professionalism/instructions from a supervisor or higher ranked staff member'],
  ['truthfulness/neglect of duty/neglect of duty-failure to act'],
  ['truthfulness/professionalism/instructions from supervisor'],
  ['truthfulness/prohibited discussions/devoting entire time to duty'],
  ['truthfulness/prohibited discussions/devoting entire time to duty',
  'truthfulness/truthfulness/prohibited discussions/devoting entire time to duty'],
  ['unauthorized force/physical intimidation', 'unauthorized foece/physical intimidation'],
  ['unauthorized force/physcial intimidation/instructions from a supervisor or higher ranked staff member', 
  'unauthorized force /physcial intimidation/ instructions from a supervios or higher ranked staff member'],
  ['unauthorized force/phsyical intimidation',
  'unauthorized force phsyical intimidation'],
  ['unauthorized force/physical intimidation/knowledge of policy procudures',
  'unauthorized force/physical intimidation knowledge of policy procudures'],
  ['use of drug/drug testing/instructions from a supervisor/neglect of duty-failure to act'],
  ['/policy procedures'],
  ['/use of departmental property'],
  ['/instructions from a supervisor or higher ranked staff mmeber', 
  '/instructions from a supervisor'],
  ['knowledge of policy procedures'],
  ['reporting late for duty'],
  ['/false or inaccurate reports/'],
  ['false or inaccurate reports', '   false or inaccurate reports'],
  ['instructions from a suspervisor or higher ranked staff member/'],
  ['/policy procedures'],
  ['cooperation'],
  ['instructions from a supervisor or higher ranking staff member/ceasing to perform work before the end of tour of duty',
  'instructions from a supervisor or higher ranking staff member ranked staff member ceasing to perform work before the end of tour of duty'],
  ['or higher ranking staff member/false or inaccurate reports',
  'or higher/false or inaccurate reports'],
  ["/knowledge of orleans parish sheriff's office policy procedures",
  "knowledge of orleans parish sheriff's office policy procedures"],
  ['ceasing to perform work before the end of tour of duty'],
  ['nstructions from a supervisor or higher ranking staff member ranked staff member'],
  ['unfounded'],
  [' general'],
  ['supervisory responsibilities', ' supervisory responsibilities'],
  ["/sick leave/worker's compensation/injury reporting"]
]

#### 
## general should be preceeded by a dash not slash 
### stripped late, spaces in look up table might be unneeded 
### consolidate end of tour duty 

action_lookup = [
    ['arrested/terminated on 04/20/2020 on case d-003-2020'],
    ['case withdrawn by colonel colvin'],
    ['currently awaiting fit criminal report'], 
    ['currently awaiting fit report'],
    ['currently out awaiting covid testing'],
    ['currently out due to covid downgraded to a dm-1', 
    'currently out due to covid/downgraded to a dm-1'],
    ['currently out due to covid/fmla terminated on case k-052-20 on 12/02/2020 by human resources',
    'currently out due to covid/fmla terminated on case k-052-20 on 12/02/2020 by hr'],
    ['currently out on fmla during no call no show'],
    ['currently under investigation', 'currently under investigaiton'],
    ['currently under investigation with tdc'],
    ['deputy rixner apologized to lieutenant armwood prior to write-up', 
    'dep rixner apologized to lt armwood prior to writeup'], 
    ['downgraded to a dm-1', 'downgraded to dm-1'],
    ['downgraded to a dm-1 by chief bruno','downgraded to dm-1 per chief bruno'], 
    ['downgraded to verbal counsel by chief bruno',
    'downgraded to verbal councel per chief bruno'],
    ['dropped from payroll on 03/26/2021'],
    ['non-sustained'],
    ["ojc conducting investiagtion haven't received case yet",
    "ojc conducting investiagtion haven t received case yet"],
    ["ojc conducting investiagtion haven't received case yet/resigned under investigation on 03/17/2021",
    'ojc conducting investiagtion haven t received case yet resigned under investigation on 03/17/2021'],
    ['currently out due to covid-19/downgraded to dm-1',
    'out due to covid-19 downgraded to dm-1'], 
    ['currently out due to covid-19/resigned under investigation on 07/22/2020',
    'out due to covid-19/resigned under investigation 07/22/2020'],
    ['currently out due to failure to take covid vaccination', 
    'out due to failure to take covid vaccination'],
    ['resigned under investigation'],
    ['resigned on 11/11/2019'],
    ['resigned under investigation on 01/25/2020',
    'resigned under investigation 01/25/2020'], 
    ['resigned under investigation 04/24/2020 prior to going to disciplinary review board'],
    ['resigned under investigation on 11/27/2020',
    'resigned under investigation 11/27/2020'],
    ['resigned under investigation on 03/25/2020'],
    ['resigned under investigation on 03/27/2020'],
    ['resigned under investigation on 11/11/19'],
    ['resigned under investigation on 5/27/2020'],
    ['suspended on 01/06/2020/terminated by disciplinary review board on 01/23/2020'],
    ['suspended on 01/28/2020/terminated by human resources',
    'suspended on 01/28/2020/terminated by hr'],
    ['suspended on 02/05/2020/returned to duty on 03/05/2020',
    'suspended on 02/05/2020/return to duty on 03/05/2020'],
    ['suspended on 02/05/2020/terminated by disciplinary review board on 03/03/2020'],
    ['suspended on 03/17/2020/terminated by human resources on 03/18/2020',
    'suspended on 03/17/2020/terminated by hr on 03/18/2020'],
    ['suspended on 03/18/2020/terminated on 04/21/2020'],
    ['suspended on 03/24/2020/terminaed by disciplinary review board on 04/22/2020'],
    ['suspended on 04/02/2020/terminated by disciplinary review board on 04/21/2020',
    'suspended on 04/02/2020/terminated by disciplinary review board 04/21/2020'],
    ['suspended on 04/20/2020/terminated by disciplinary review board on 06/03/2020'],
    ['suspended on 04/24/2020'],
    ['suspended on 04/24/2020/resigned under investigation on 04/24/2020',
    'suspended on 02/04/2020/resigned under investigation 04/24/2020'],
    ['suspended on 05/08/2020/awaiting criminal investigation',
    'suspended on 05/08/2020/awaiting criminal s investigation'],
    ['suspended on 06/02/2020/terminated by human resources on 06/17/2020',
    'suspended on 06/02/2020/terminated by hr on 06/17/2020'],
    ['suspended on 07/20/2020/case currently pending in criminal court',
    'suspended on 07/20/2020 case currently pending in criminal court'],
    ['suspended on 07/30/2020', 'suspended on 07/30/2020/returned to duty on'],
    ['suspended on 08/04/2020/terminated by disciplinary review board on 09/02/2020'],
    ['suspended on 08/19/2020', 'suspended on 08/19/2020/returned to duty on'],
    ['suspended on 09/14/2020 terminated on 10/01/2020/termination was rescinded on 10/1/2020',
    'suspended on 09/14/2020 terminated on 10/01/2020 termination was rescinded on 10/1/2020'],
    ['suspended on 09/16/2020/terminated on 10/01/2020/termination was rescinded on 10/1/2020',
    'suspended on 09/16/2020 terminated on 10/01/2020 termination was rescinded on 10/1/2020'],
    ['suspended 09/30/2020-10/25/2020/returned to duty 10/28/2020',
    'suspended on 09/30/2020-10/25/2020 return to duty 10/28/2020'],
    ['suspended 10/01/2020-10/25/2020/returned to duty on 10/28/2020',
    'suspended on 10/01/2020-10/25/2020 return to dudty on 10/28/2020'],
    ['suspended 10/21/20-10/30/20/returned to duty on 10/31/20',
    'suspended on 10/21/20-10/30/20 return to duty on 10/31/20'],
    ['suspended on 10/21/2020/terminated by disciplinary review board on 11/02/2020'],
    ['suspended on 10/21/2020/terminated by human resources on 10/26/2020',
    'suspended on 10/21/2020/terminated by hr on 10/26/2020'],
    ['suspended on 10/21/2020/terminated by human resources on 10/26/2020 on case j-052-20',
    'suspended on 10/21/2020/terminated by hr on 10/26/2020 on case j-052-20'],
    ['suspended 10/25/20-11/5/20/returned to duty on 11/8/20',
    'suspended on 10/25/20-11/5/20 return to duty on 11/8/20'],
    ['suspended on 10/27/20-11/9/20/returned to duty on 11/10/20',
    'suspended on 10/27/20-11/9/20 return to duty on 11/10/20'],
    ['suspended on 11/16/2020 due to arrest/pending court case'],
    ['suspended on 11/23/2020/returned to duty on 12/01/2020'],
    ['suspended on 11/25/2020/terminated by human resources on 12/02/2020',
    'suspended on 11/25/2020/terminated by hr on 12/02/2020'],
    ['suspended on 12/04/2020'],
    ['suspended on 12/04/2020 on case l-011-20'],
    ['suspended on 12/04/2020/terminated on 01/14/2021',
    'suspended on 12/04/2020 terminated on 01/14/2021'],
    ['suspended on 12/15/2020'],
    ['suspended on 12/21/2020'],
    ['suspended on 12/21/2020 on case l-039-2020'],
    ['suspended on 12/22/2020'],
    ['suspended on 12/31/2020/resigned under investigation on 01/04/2020'],
    ['suspended on 12/8/2020'],
    ['suspended 1-6-21-1/8/21/returned to duty on 1/11/21',
    'suspended on 1-6-21 end date 1/8/21 return to duty on 1/11/21'],
    ['suspended on 2/10/21/returned on 2/11/2021',
    'suspended on 2/10/21 return on 2/11/2021'],
    ['suspended 2/10/21-2/11/2021/returned to duty on 2/15/2021',
    'suspended on 2/10/21-2/11/2021 return to duty on 2/15/2021'],
    ['suspended 2/15/2021-02/16/2021/returned to duty on 2/19/2021',
    'suspended on 2/15/2021-02/16/2021 return to duty on 2/19/2021'],
    ['suspended on 6/15/2020/terminated by disciplinary review board on 07/29/2020'],
    ['suspended on 7/23/2020/resigned under investigation on 07/31/2020'],
    ['suspended on 8/24/2020/terminated on 10/1/2020',
    'suspended on 8/24/2020 terminated on 10/1/2020'],
    ['suspended on 8/25/2020/terminated on 9/17/2020',
    'suspended on 8/25/2020 terminated on 9/17/2020'],
    ['suspended 9/14/2020-9/17/2020/returned to duty on 9/18/2020',
    'suspended on 9/14/2020-9/17/2020 return to duty on 9/18/2020'],
    ['suspended on 9/30/20-10/5/20/returned to duty on 10/6/2020',
    'suspended on 9/30/20-10/5/20 return to duty on 10/6/2020'],
    ['suspended 9/4/2020-9/6/2020/returned to duty on 9/9/2020',
    'suspended on 9/4/2020-9/6/2020 return to duty on 9/9/2020'],
    ['suspended 9/9/2020-9/10/2020/returned to duty on 9/11/2020',
    'suspended on 9/9/2020-9/10/2020 return to duty on 9/11/2020'],
    ['suspended/arrested on 04/13/2020/terminated on 04/20/2020',
    'suspended/arrested on 04/13/2020 terminaed on 04/20/2020'],
    ['suspended/arrested on 07/01/2020/resigned under investigation on 07/13/2020'],
    ['suspension overturned by sheriff marlin gusman on 07/10/2020'],
    ['suspended 3/18/21/returned to duty on 3/21/2021',
    'susupended 3/18/21 returned on 3/21/2021'],
    ['terminated on 05/20/2019', 'terminated 05/20/2019'],
    ['terminated by human resources on 03/17/2020',
    'terminated by hr 03/17/2020'],
    ['terminated by human resources on 06/11/2020', 'terminated by hr 06/11/2020'],
    ['terminated by human resources on 05/11/2020', 'terminated by hr on 05/11/2020'],
    ['terminated by human resources on 06/10/2020', 'terminated by hr on 06/10/2020'],
    ['terminated by human resources on 07/31/2020', 'terminated by hr on 07/31/2020'],
    ['terminated on case h-060-19 prior to receving case l-032-2020'],
    ['unfounded all charges'],
    ['suspended on 09/16/2020/terminated on 10/01/2020/termination was rescinded on 10/1/2020']
    
]


def remove_header_rows(df):
    return df[
        df.date_received.str.strip() != "Date\rReceived"
    ].reset_index(drop=True)


def remove_carriage_return(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.replace(r"-\r", r"-", regex=True)\
            .str.replace(r"\r", r" ", regex=True)
    return df


def split_name_19(df):
    series = df.name_of_accused.fillna('').str.strip()
    for col, pat in [('first_name', r"^([\w'-]+)(.*)$"), ('middle_initial', r'^(\w\.) (.*)$')]:
        names = series[series.str.match(pat)].str.extract(pat)
        df.loc[series.str.match(pat), col] = names[0]\
            .str.replace('various', '', regex=False)
        series = series.str.replace(pat, r'\2', regex=True).str.strip()
    df.loc[:, 'last_name'] = series
    return df.drop(columns=['name_of_accused']).dropna(subset=['first_name', 'last_name'])

## na's wont drop 

def split_name_20(df):
    df.name_of_accused = df.name_of_accused.str.lower().str.strip()\
        .str.replace('la shanda ezidor', 'lashanda ezidor', regex=False)\
        .str.replace('lieutenant latoya armwood', 'latoya armwood', regex=False)\
        .str.replace('various', '', regex=False)
    names = df.name_of_accused.str.lower().str.strip().str.extract(r'^(\w+) (\w+ )? ?(.+)$')
    df.loc[:, 'first_name'] = names[0]
    df.loc[:, 'middle_name'] = names[1]
    df.loc[:, 'last_name'] = names[2]
    return df.drop(columns=['name_of_accused']).fillna('')


def drop_rows_missing_name(df):
    return df[~((df.first_name == '') & (df.middle_name == '') & (df.last_name == ''))]


def clean_employee_id(df):
    df.loc[:, 'employee_id'] = df.emp_no.str.lower().str.strip()\
        .str.replace('608199n', '608199', regex=False)
    return df.drop(columns='emp_no')


def fix_rank_desc_(df):
    df.loc[
        (df.rank_desc == 'recruit') & (df.last_name == 'armwood'),
        'rank_desc',
    ] = 'lieutenant'
    return df


def clean_action_19(df):
    df.loc[:, 'action'] = df.action.str.lower().str.strip()\
        .str.replace(r'\s+', ' ', regex=True)\
        .str.replace('rui', 'resigned under investigation', regex=False)\
        .str.replace('rtd', 'Return to duty', regex=False)\
        .str.replace('drb', 'Disciplinary review board', regex=False)\
        .str.replace('iad', 'Internal affairs department', regex=False)\
        .str.replace(r'\bhr\b', 'human resources', regex=True)\
        .str.replace('unfouned all charges', 'unfouded', regex=False)\
        .str.replace(r'^n$', '', regex=True)\
        .str.replace(' and ', '/', regex=False)
    return df


## should resign date be a new column or remain in action column

def clean_action_20(df):
    df.action = df.action.str.lower().str.strip()\
        .str.replace(r' ^(\w+)', r'\1', regex=True)\
        .str.replace('.', '', regex=False)\
        .str.replace(r'on  (\d+)', r'on \1', regex=True)\
        .str.replace(' and ', '/', regex=False)\
        .str.replace(r'board|drb|dbr', 'disciplinary review board', regex=True)\
        .str.replace(r'(\w+)  (\w+)', r'\1 \2', regex=True)\
        .str.replace('rui', 'resigned under investigation', regex=False)\
        .str.replace(r'(\d+) (\d+) (\d+)', r'\1/\2/\3', regex=True)
    return df


def clean_summary_20(df):
    df.summary = df.summary.str.lower().str.strip()\
        .str.replace(r'\brui\b', 'resigned under investigation', regex=True)
    return df 

def clean_disposition_20(df):
    df.disposition = df.disposition.str.lower().str.strip()
   
    names = df.disposition.str.extract(r'(non-? ?sustaine?d?|sustaine?d?|exonerated|unfounded|founded)')

    df.loc[:, 'disposition'] = names[0]\
        .str.replace(r'non ?sustained', 'non-sustained', regex=True)
    return df 


def process_disposition(df):
    df.loc[:, 'disposition'] = df.disposition.str.strip()
    df.loc[:, 'charges'] = df.charges.str.strip()\
        .str.replace(r',$', r'', regex=True)
    for _, row in df.iterrows():
        if pd.isnull(row.disposition) or pd.isnull(row.charges):
            continue
        charges = row.charges.lower()
        if row.disposition.lower().startswith(charges):
            row.disposition = row.disposition[len(charges):]
    df.loc[:, 'disposition'] = df.disposition.str.strip()\
        .str.replace(r'^[- ]+', '', regex=True)
    return df


def clean_department_desc(df):
    df.loc[:, 'department_desc'] = df.department_desc.str.lower()\
        .str.strip().str.replace('mechani c', 'mechanic', regex=False)\
        .str.replace('plannin g', 'planning', regex=False)\
        .str.replace('complai nce', 'complaince', regex=False)\
        .str.replace(r'mainten( ance)?$', 'maintenance', regex=True)\
        .str.replace('grievnce', 'grievance', regex=False)\
        .str.replace('h.r.', 'hr', regex=False)
    return df


def set_empty_uid_for_anonymous_officers(df):
    df.loc[df.employee_id.isna() & (df.first_name == ''), "uid"] = ""
    return df


def fix_date_typos(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.strip()\
            .str.replace('//', '/', regex=False)\
            .str.replace(r'^(\d{2})(\d{2})', r'\1/\2', regex=True)\
            .str.replace(r'2011(\d)$', r'201\1', regex=True)
    return df


def split_investigating_supervisor(df):
    df.loc[:, 'investigating_supervisor'] = df.investigating_supervisor\
        .fillna('').str.strip().str.lower()\
        .str.replace(r'serrgeant|ssergeant|sergenat|sergant|sergent|sergeat', 'sergeant', regex=True)\
        .str.replace(r'lt\.?|lieutenatn|lieutenat|lieuttenant|lieuteant', 'lieutenant', regex=True)\
        .str.replace('ms', '', regex=False)\
        .str.replace('augusuts', 'augustus', regex=False)\
        .str.replace('karengant', 'karen gant', regex=False)\
        .str.replace(r'^(\w+)  (\w+)', r'\1 \2', regex=True)\
        .str.replace(r'^ (\w+)', r'\1', regex=True)\
        .str.replace(r'^sergeant$', '', regex=True)\
        .str.replace(r'^colvin', 'colonel', regex=True)\
        .str.replace(r'dwi (shana)? ', 'dwishana ', regex=True)\
        .str.replace('unassigned', '', regex=False)\
        .str.replace('.', '', regex=False)\
        .str.replace('captaiin', 'captain', regex=False)
    ranks = set([
        'major', 'agent', 'sergeant', 'captain', 'lieutenant',
        'colonel', 'chief', 'director', 'admin'
    ])
    for idx, s in df.investigating_supervisor.items():
        if ' ' not in s:
            continue
        first_word = s[:s.index(' ')]
        if first_word in ranks:
            df.loc[idx, 'supervisor_rank'] = first_word
            df.loc[idx, 'supervisor_name'] = s[s.index(' ')+1:]
        else:
            df.loc[idx, 'supervisor_name'] = s
    df.loc[:, 'supervisor_name'] = df.supervisor_name.str.replace(
        ' -', '-', regex=False
    )
    names = df.supervisor_name.fillna('')\
        .str.extract(r'^([^ ]+) (.+)')
    df.loc[:, 'supervisor_first_name'] = names[0]
    df.loc[:, 'supervisor_last_name'] = names[1]
    return df.drop(columns=['investigating_supervisor', 'supervisor_name'])


def clean_receive_date(df):
    df.loc[:, 'receive_date'] = df.date_received.fillna('')\
        .str.replace(r'(\d+)/(\d+)//(\d+)', r'\1/\2/\3', regex=True)\
        .str.replace('!2', '12', regex=False)\
        .str.replace('9/142020', '9/14/2020', regex=False)
    return df.drop(columns='date_received')


def clean_investigation_start_date(df):
    df.loc[:, 'investigation_start_date'] = df.date_started.fillna('')\
        .str.replace(r'(\d+)//(\d+)$', r'\1/\2', regex=True)
    return df.drop(columns='date_started')


def clean_investigation_complete_date(df):
    df.loc[:, 'investigation_complete_date'] = df.date_completed.fillna('')\
        .str.replace(r'^(\d+)//(\d+)', r'\1/\2', regex=True)\
        .str.replace('031/2/2020', '3/12/2020', regex=False)\
        .str.replace('3/42021', '3/4/2021', regex=False)
    return df.drop(columns='date_completed')


def clean_rank_desc(df):
    df.rank_desc = df.rank_desc.str.lower().str.strip().fillna('')\
        .str.replace(r'recu?c?r?u?it', 'recruit', regex=True)\
        .str.replace(r'deo?u?p?u?i?ti?e?s?y?', 'deputy', regex=True)\
        .str.replace('lieuteant', 'lieutenant', regex=False)\
        .str.replace('various', '', regex=False)\
        .str.replace('cmt', 'certified medical technician', regex=False)\
        .str.replace('policies', 'policy', regex=False)
    return df 


def clean_charges(df):
    df.charges = df.charges.str.strip()\
        .str.replace(r', ?', '/', regex=True)\
        .str.replace('dtuy', 'duty', regex=False)\
        .str.replace('#', '', regex=False)\
        .str.replace('.', '', regex=False)\
        .str.replace(r'(\w+)/ (\w+)', r'\1/\2', regex=True)\
        .str.replace('"', '', regex=False)\
        .str.replace(r'(\d+)', '', regex=True)\
        .str.replace('&', '', regex=False)\
        .str.replace(r'\blaw and professionalism\b', 'law/professionalism', regex=True )\
        .str.replace(r' ?/and neglect' , '/neglect', regex=False)\
        .str.replace(r' ?/$', '', regex=True)\
        .str.replace(r'\b ?(from)? ?(or)? ?(higher)? rank\b', ' higher ranked staff member', regex=True)\
        .str.replace(r'duty ?/?-? ?failure', 'duty-failure', regex=True)\
        .str.replace(r'duty;? ?\??/? ?general', 'duty-general', regex=True)\
        .str.replace(r'\bresp(onsibi?u?l?i?t?i?e?s?y?)?\b', 'responsibilities', regex=True)\
        .str.replace(r'sus?pp?erv?i?s?i?os?r?', 'supervisor', regex=True)\
        .str.replace('opso', "orleans parish sherrif's office", regex=False)\
        .str.replace('policies procedures', 'policy procedures', regex=False)\
        .str.replace(r'\bcomp\b', 'compensation', regex=True)\
        .str.replace(r'(\w+)  (\w+)', r'\1 \2', regex=True)\
        .str.replace('supervisor higher', 'supervisor or higher', regex=False)\
        .str.replace('ofduty', 'of duty', regex=False)\
        .str.replace(r'sr?at?ff', 'staff', regex=True)\
        .str.replace("sherrif's", "sheriff's", regex=False)\
        .str.replace('proferssionalism', 'professionalism', regex=False)\
        .str.replace('instructions from supervisor', 'instructions from a supervisor', regex=False)\
        .str.replace('supervisor of higher', 'supervisor or higher', regex=False)\
        .str.replace(r'^ ? ?(\w+)', r'\1', regex=True)\
        .str.replace(r'/?ceasing to perform before', 'ceasing to perform work before', regex=True)\
        .str.replace(',', '', regex=False)\
        .str.replace('beofore', 'before', regex=False)\
        .str.replace('tyour', 'tour', regex=False)\
        .str.replace('duty-supervisory', 'duty/supervisory', regex=False)\
        .str.replace(r'poli?ciy', 'policy', regex=True)\
        .str.replace('ceassing', 'ceasing', regex=False)\
        .str.replace('neglet', 'neglect', regex=False)\
        .str.replace('procudures', 'procedures', regex=False)\
        .str.replace('instructions higher', 
                     'instructions from a supervisor or higher ranking staff member', regex=False)\
        .str.replace('; ', '/', regex=False)\
        .str.replace('foece', 'force', regex=False)\
        .str.replace('hiigher', 'higher', regex=False)\
        .str.replace('higher ranking staff member ranked staff member', 
        'higher ranking staff member', regex=False)\
        .str.replace(r'(\w+) neglect', r' \1/neglect', regex=True)\
        .str.replace('froma', 'from a', regex=False)\
        .str.replace(r'(\w+) instructions', r'\1/instructions', regex=True)\
        .str.replace(r'(\w+) knowledge', r'\1/knowledge', regex=True)\
        .str.replace(r'(\w+)  ?false', r'\1/false', regex=True)\
        .str.replace(r'(\w+) supervisory', r'\1/supervisory', regex=True)\
        .str.replace(r'(\w+) ceasing', r'\1/ceasing', regex=True)\
        .str.replace(r'(\w+) professionalism', r'\1/professionalism', regex=True)\
        .str.replace(r' ?(\w+) leaving', r'/leaving', regex=True)\
        .str.replace(r'(\w+) use', r'\1/use', regex=True)\
        .str.replace(r'(\w+) cooperation', r'\1/cooperation', regex=True)\
        .str.replace(r' (\w+) sick', r'\1/sick', regex=True)\
        .str.replace(r'(\w+) neatness', r'\1/neatness', regex=True)\
        .str.replace(r' (\w+) interferring', r'/interfering', regex=True)\
        .str.replace(r' (\w+) possessesion', r'/possession', regex=True)\
        .str.replace('professionalism- ', 'professionalism/', regex=False)\
        .str.replace(r'^professionalism/professionalism/professionalism',
        'professionalism/', regex=True)\
        .str.replace('neglect of duty-failure to act/neglect of duty-failure to act /neglect of duty-failure to act', 
        'neglect of duty-failure to act', regex=False)\
        .str.replace('actceasing', 'act/ceasing', regex=False)\
        .str.replace('dutyceasing', 'duty/ceasing', regex=False)\
        .str.replace('foe', 'for', regex=False)\
        .str.replace(r'ree?por?ti[tim]?ng', 'reporting', regex=True)\
        .str.replace(r'ins?strs?uctions?', 'instructions', regex=True)\
        .str.replace(r'(\w+)/ ?//?(\w+)', r'\1/\2', regex=True)\
        .str.replace(r'(\w+) ?/ ?(\w+)', r'\1/\2', regex=True)\
        .str.replace('a a', 'a', regex=False)\
        .str.replace(r' ?/?/?/? ?and/', '/', regex=True)\
        .str.replace('/and', '/', regex=False)\
        .str.replace(r' ?//', '/', regex=True)\
        .str.replace(r'and$', '', regex=True)
    return df


def standardize_charges_20(df):
    df.charges = df.charges
    return standardize_from_lookup_table(df, 'charges', charges_lookup)


def standardize_action_20(df):
    df.action = df.action
    return standardize_from_lookup_table(df, 'action', action_lookup)


def clean19():
    df = pd.read_csv(data_file_path(
        "raw/new_orleans_so/new_orleans_so_cprr_2019_tabula.csv"))
    df = clean_column_names(df)
    df = df\
        .pipe(remove_header_rows)\
        .drop(columns=[
            'month', 'quarter', 'numb_er_of_cases', 'related_item_number', 'a_i',
            'intial_action', 'inmate_grievance', 'referred_by', 'date_of_board'
        ])\
        .rename(columns={
            'date_received': 'receive_date',
            'case_number': 'tracking_number',
            'job_title': 'rank_desc',
            'charge_disposition': 'disposition',
            'location_or_facility': 'department_desc',
            'assigned_agent': 'investigating_supervisor',
            'emp_no': 'employee_id',
            'date_started': 'investigation_start_date',
            'date_completed': 'investigation_complete_date',
            'terminated_resigned': 'action'
        })\
        .pipe(remove_carriage_return, [
            'name_of_accused', 'disposition', 'charges', 'summary', 'investigating_supervisor',
            'action', 'department_desc', 'rank_desc'
        ])\
        .pipe(clean_department_desc)\
        .pipe(standardize_desc_cols, ['rank_desc'])\
        .pipe(split_name_19)\
        .pipe(clean_names, ['first_name', 'last_name', 'middle_initial'])\
        .pipe(clean_action_19)\
        .pipe(set_values, {
            'agency': 'New Orleans SO',
            'data_production_year': '2019'
        })\
        .pipe(process_disposition)\
        .pipe(fix_date_typos, ['receive_date', 'investigation_start_date', 'investigation_complete_date'])\
        .pipe(gen_uid, ['agency', 'employee_id', 'first_name', 'last_name', 'middle_initial'])\
        .pipe(set_empty_uid_for_anonymous_officers)\
        .pipe(gen_uid, ['agency', 'tracking_number', 'uid'], 'complaint_uid')\
        .sort_values(['tracking_number', 'investigation_complete_date'])\
        .drop_duplicates(subset=['complaint_uid'], keep='last', ignore_index=True)\
        .pipe(split_investigating_supervisor)
    return df 


def clean20():
    df = pd.read_csv(data_file_path(
        "raw/new_orleans_so/new_orleans_so_cprr_2020.csv")).dropna(how='all')
    df = clean_column_names(df)
    df = df\
        .drop(columns=['month', 'quarter', 'intial_action',
        'location_or_facility', 'a_i', 'number_of_cases',
        'date_of_board', 'inmate_grievance', 'related_item_number'])\
        .rename(columns={
            'case_number': 'tracking_number',
            'job_title': 'rank_desc',
            'charge_disposition': 'disposition',
            'location_or_facility': 'department_desc',
            'assigned_agent': 'investigating_supervisor',
            'terminated_resigned': 'action',
            'referred_by': 'complainant'
        })\
        .pipe(clean_employee_id)\
        .pipe(standardize_desc_cols, 
        ['charges', 'action', 'summary','employee_id'
        ])\
        .pipe(clean_receive_date)\
        .pipe(clean_names, [
            'investigating_supervisor', 'name_of_accused',
            'complainant', 'action'
        ])\
        .pipe(clean_investigation_start_date)\
        .pipe(split_investigating_supervisor)\
        .pipe(clean_rank_desc)\
        .pipe(clean_dates, ['receive_date', 'investigation_start_date'])\
        .pipe(clean_charges)\
        .pipe(standardize_desc_cols, ['charges'])\
        .pipe(clean_action_20)\
        .pipe(process_disposition)\
        .pipe(clean_disposition_20)\
        .pipe(standardize_action_20)\
        .pipe(split_name_20)\
        .pipe(drop_rows_missing_name)\
        .pipe(set_values, {
            'agency': 'New Orleans SO',
            'data_production_year': '2020'
        })\
        .pipe(fix_rank_desc_)\
        .pipe(gen_uid, ['agency', 'first_name', 'middle_name', 'last_name'])\
        .pipe(gen_uid, ['tracking_number', 'action', 'employee_id'], 'complaint_uid')
    return df 


if __name__ == '__main__':
    df19 = clean19()
    df20 = clean20()
    ensure_data_dir('clean')
    df19.to_csv(data_file_path('clean/cprr_new_orleans_so_2019.csv'), index=False)
    df20.to_csv(data_file_path('clean/cprr_new_orleans_so_2020.csv'), index=False)
