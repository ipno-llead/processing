import os
import re

import deba
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_names
from lib.uid import gen_uid
import pandas as pd


def join_allegation_cols(df):
    df.loc[:, "allegation_rule"] = (
        df.allegation_rule.str.lower()
        .str.strip()
        .str.replace(r"perf\b", "performance", regex=True)
        .str.replace(r"prof\b", "professional", regex=True)
        .str.replace(r"rule 1:(.+)", r"rule 1: \1", regex=True)
        .str.replace(r"chief admin\. office", "", regex=True)
        .str.replace(
            r"^(no violation observed|policy|civil service rules|no allegation assigned at this time)$",
            "",
            regex=True,
        )
        .str.replace(r"dept", "department", regex=False)
    )

    df.loc[:, "allegation_paragraph"] = (
        df.allegation_paragraph.str.lower()
        .str.strip()
        .str.replace(
            r"paragraph 13 - social networking (.+)",
            "paragraph 13 - social networking, websites, print or transmitted media, etc.",
            regex=True,
        )
        .str.replace(
            r"^paragraph 14 - social networking, websites, facebook, myspace, print or transmitted media, etc.$",
            "paragraph 14 - social networking, websites, print or transmitted media, etc.",
            regex=True,
        )
        .str.replace(r"paragraph 2-effective$", r"paragraph 2 - effective", regex=True)
    )

    df.loc[:, "allegation"] = df.allegation_rule.str.cat(
        df.allegation_paragraph, sep="; "
    )
    return df.drop(columns=["allegation_rule", "allegation_paragraph"])[
        ~((df.allegation.fillna("") == ""))
    ]


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.strip()
        .str.lower()
        .str.replace(
            r"(.+)?\brui\b(.+)?",
            "resigned or retired while under investigation",
            regex=True,
        )
        .str.replace(r"^nfim case$", "no further investigation merited", regex=True)
        .str.replace(r"^withdrawn- mediation$", "mediation", regex=True)
        .str.replace(r"duplicate (.+)", "", regex=True)
        .str.replace(r"dui-(.+)", "dismissed under investigation", regex=True)
        .str.replace(
            r"charges proven resigned",
            "resigned or retired while under investigation",
            regex=True,
        )
        .str.replace(r"invest\.$", "investigation", regex=True)
        .str.replace(r"\.$", "", regex=True)
        .str.replace(r"^pending$", "pending investigation", regex=True)
        .str.replace(r"^charges proven$", "sustained", regex=True)
        .str.replace(r"(cancelled|info\b(.+)?)", "", regex=True)
        .str.replace(r"reclassified as di-2", "di-2", regex=False)
        .str.replace(r"^dui$", "dismissed under investigation", regex=True)
        .str.replace(r"^dui sustained$", "sustained", regex=True)
        .str.replace(r"reclassified as info$", "info", regex=True)
        .str.replace(r"^investigation cancelled$", "cancelled", regex=True)
        .str.replace(r"^sustained -(.+)", "sustained", regex=True)
        .str.replace(r"charges withdrawn", "withdrawn", regex=False)
        .str.replace(
            r"^resigned$", "resigned or retired while under investigation", regex=True
        )
        .str.replace(r"^di-3 nfim$", "no further investigation merited", regex=True)
        .str.replace(
            r"^retired under investigation$",
            "resigned or retired while under investigation",
            regex=True,
        )
        .str.replace(r"dismissal -(.+)", "dismissed under investigation", regex=True)
        .str.replace(r"sustained-(.+)", "sustained", regex=True)
        .str.replace(r"^ di-3", r"di-3", regex=True)
        .str.replace(r"unfounded- dui", "unfounded", regex=False)
        .str.replace(
            r"^(nat|moot(.+)?|investigation|reclassified as|redirect(.+)|bwc(.+)|grievance|awaiting(.+)|non-applicable|deceased|other)",
            "",
            regex=True,
        )
        .str.replace(r"^nfim$", "no further investigation merited", regex=True)
    )
    return df


def extract_receive_date(df):
    df.loc[:, "receive_date"] = df.tracking_id_og.str.replace(
        r"^(\w{4})-(.+)", r"\1", regex=True
    )

    df.loc[:, "receive_date"] = df.receive_date.str.replace(r"(.+)", r"12/31/\1")
    return df

def filter_by_year(df):
    return df[df['tracking_id_og'].str.startswith(('2019', '2020', '2021', '2022'))]


def clean():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/cprr_new_orleans_pd_2005_2023.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "2023_2086_p": "tracking_id_og",
                "8913": "employee_id",
                "jeffrey": "first_name",
                "vappie": "last_name",
                "public_initiated": "complainant_type",
                "initial": "status",
            }
        )
        .drop(columns=["allegation_number"])
        .pipe(join_allegation_cols)
        .pipe(clean_disposition)
        .pipe(extract_receive_date)
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(
            standardize_desc_cols, ["tracking_id_og", "allegation_desc", "disposition", "complainant_type", "status"]
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "tracking_id"], "allegation_uid")
        .pipe(filter_by_year)
    )
    return df


DISPOSITION_VALUES = ['unfounded', 'exonerated', 'not sustained', 'sustained']
def correct_disposition_23(df):
    df['allegation_desc'] = df['allegation_desc'].str.lower().str.strip()
    df['disposition'] = df['disposition'].str.lower().str.strip()
    disposition_mask = df['allegation_desc'].str.contains(r'\b(unfounded|exonerated|not sustained|sustained)\b', regex=True)

    extracted_dispositions = df['allegation_desc'].str.extract(r'(\b(unfounded|exonerated|not sustained|sustained)\b)', expand=False)[0]

    df['disposition'] = df['disposition'].fillna(extracted_dispositions)

    df.loc[disposition_mask, 'allegation_desc'] = df.loc[disposition_mask, 'allegation_desc'].str.replace(r'\b(unfounded|exonerated|not sustained|sustained)\b', '', regex=True).str.strip(' -')

    df.loc[:, "allegation_desc"] = df.allegation_desc.str.replace(r"(unfounded|exonerated|not sustained|sustained)", "", regex=True)


    df.loc[:, "allegation_desc"] = (df.allegation_desc
                                    .str.replace(r"(\w+) $", r"\1", regex=True)
                                    .str.replace(r"^ (\w+)", r"\1", regex=True)
                                    .str.replace(r'^[^\w\s]+$', '', regex=True)
                                    .str.replace(r"^rs", r"r.s.", regex=True)

    )
    mask = ~df['allegation_desc'].str.contains(r'\b(policy|chapter|paragraph|r\.?s\.?)\b', regex=True)
    df.loc[mask, 'allegation_desc'] = ''
    return df

def join_allegation_cols_23(df):
    df.loc[:, "allegation_rule"] = (
        df.allegation_rule.str.lower()
        .str.strip()
        .str.replace(r"perf\b", "performance", regex=True)
        .str.replace(r"prof\b", "professional", regex=True)
        .str.replace(r"rule 1:(.+)", r"rule 1: \1", regex=True)
        .str.replace(r"chief admin\. office", "", regex=True)
        .str.replace(
            r"^(no violation observed|policy|civil service rules|no allegation assigned at this time)$",
            "",
            regex=True,
        )
        .str.replace(r"dept", "department", regex=False)
    )

    df.loc[:, "allegation_paragraph"] = (
        df.allegation_paragraph.str.lower()
        .str.strip()
        .str.replace(
            r"paragraph 13 - social networking (.+)",
            "paragraph 13 - social networking, websites, print or transmitted media, etc.",
            regex=True,
        )
        .str.replace(
            r"^paragraph 14 - social networking, websites, facebook, myspace, print or transmitted media, etc.$",
            "paragraph 14 - social networking, websites, print or transmitted media, etc.",
            regex=True,
        )
        .str.replace(r"paragraph 2-effective$", r"paragraph 2 - effective", regex=True)
    )

    df.loc[:, "allegation"] = df.allegation_rule.str.cat(
        df.allegation_paragraph, sep="; "
    )
    return df.drop(columns=["allegation_rule", "allegation_paragraph"])[
        ~((df.allegation.fillna("") == ""))
    ]

def strip_leading_commas_23(df):
    df = df.apply(lambda col: col.astype(str).str.replace(r"\'", "", regex=True))
    return df 

def clean_complainant_type(df):
    df.loc[:, "complainant_type"] = (df.complainant_type
                                     .str.lower()
                                     .str.strip()
                                     .str.replace(r"^(?!public initiated|rank initiated).*$", "", regex=True)
    )
    return df


def clean_names_23(df):
    # Remove digits from first and last names
    df['first_name'] = df['accused_first'].str.lower().str.strip().str.replace(r'\d', '', regex=True)
    df['last_name'] = df['accused_last'].str.lower().str.strip().str.replace(r'\d', '', regex=True)
    
    df.loc[:, "first_name"] = (df.first_name
                               .str.replace(r"(none|unknown|dmeekco|dmeeko|clemons)", "", regex=True)
                               .str.replace(r"^leroy-joseph$", "leroy-joseph", regex=True)
                               .str.replace(r"^angel s\.$", "angel", regex=True)
                               .str.replace(r"(\w+)- (\w+)", r"\1-\2", regex=True)

    )
    df.loc[:, "last_name"] = (df.last_name
                              .str.replace(r"(\w+)- (\w+)", r"\1-\2", regex=True)
                              .str.replace(r"(\w+) (\w{1})$", r"\1\2", regex=True)
    )
    return df.drop(columns=['accused_first', 'accused_last'])

def clean_disposition_23(df):
    df.loc[:, "disposition"] = (df.allegation_finding
                                .fillna("")
                                .str.lower()
                                .str.strip()
                                .str.replace(r"(.+)?(dismiss|\bdui\b)(.+)?", "terminated", regex=True)
                                .str.replace(r"(.+)?\brui\b(.+)?", "resigned or retired while under invwstigation", regex=True)
                                .str.replace(r"^retired(.+)", "retired while under investigation", regex=True)  
                                .str.replace(r"(-|\.)", "", regex=True)
                                .str.replace(r"(.+)?(chapter|rs|paragraph|info|redirection)(.+)?", "", regex=True)
                                .str.replace(r"^ (\w+)", r"\1", regex=True)
                                .str.replace(r"withdrawn mediation", "mediation", regex=True)
                                .str.replace(r"^(.+)?fim(.+)?", "no further investigation merited", regex=True)
                                .str.replace(r"(.+)?neim(.+)?", "no further investigation", regex=True)
                                .str.replace(r"(.+)?di-?2(.+)?", "di-2", regex=True)
                                .str.replace(r"^sustained(.+)", "sustained", regex=True)
                                .str.replace(r"^not(.+)", "not sustained", regex=True)
                                .str.replace(r"di3", "di-2", regex=True)
                                .str.replace(r"nonapplicable", "non-applicable", regex=True)
                                .str.replace(r"(\w+) $", r"\1", regex=True)
                                .str.replace(r"^moot(.+)?", "moot", regex=True)
                                .str.replace(r"(.+)?duplicate(.+)?", "duplicate investigation", regex=True)
                                .str.replace(r"pending(.+)", "pending", regex=True)
                                .str.replace(r"^nan$", "", regex=True)                           
    )
    return df.drop(columns=["allegation_finding"]) 

def filter_years_23(df):
    df = df[df.tracking_id_og.str.contains("(2023)")] #removed 2024 
    return df

def clean_tracking_id_23(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id_og.str.replace(r"(\w+)- (\w+)", r"\1-\2", regex=True)
    return df 

def clean_allegation_23(df):
    df.loc[:, "allegation_rule"] = (df.allegation_rule
                                    .str.lower()
                                    .str.strip()
                                    .str.replace(r"chief admin\. office", "", regex=True)
                                    .str.replace(r"^policy$", "", regex=True)
                                    .str.replace(r"^no(.+)", "", regex=True)
                                    .str.replace(r":(\w+)", r": \1", regex=True)
                                    .str.replace(r" perf ", " performance ", regex=False)
                                    .str.replace(r" prof ", " professionalism ", regex=False)
                                    .str.replace(r"professionalism conduct", "professionalism", regex=False)
                                    )
    
    df.loc[:, "allegation_paragraph"] = (df.allegation_paragraph
                                         .str.lower()
                                         .str.strip()
                                         .str.replace(r"- -", "-", regex=False)
                                         .str.replace(r"paragraph \b(\w{2})\b ?-? (.+)", r"paragraph \1: \2", regex=True)
        
                                         )
    
    df.loc[:, "allegation"] = df.allegation_rule.str.cat(df.allegation_paragraph, sep=" - ")

    df.loc[:, "allegation"] = (df.allegation
                               .str.replace(r" - $", "", regex=True)    
                               .str.replace(r"withholdin g", "withholding", regex=False)
                               .str.replace(r"^ - (.+)", r"\1", regex=True)
    )
    return df.drop(columns=["allegation_rule", "allegation_paragraph"])

def clean25():
    df = (pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_cprr_2025.csv"))
            .pipe(clean_column_names)
            .rename(
            columns={
                "fdi": "tracking_id_og",
                "incident_type": "complainant_type",
                "accused_employee_id": "employee_id",
                "relevant_policy": "allegation_desc"
            }
        )
        .pipe(clean_tracking_id_23)
        .pipe(strip_leading_commas_23)
        .pipe(clean_complainant_type)
        .pipe(clean_names_23)
        .pipe(clean_disposition_23)
        .pipe(clean_allegation_23)
        .pipe(correct_disposition_23)
        .pipe(
            standardize_desc_cols, ["tracking_id_og", "investigation_status"]
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "tracking_id"], "allegation_uid")
    
    )
    return df 

def filter_by_year_2024 (df):
    return df[df['tracking_id_og'].str.startswith(('2024'))]

def clean24():
    df_24 = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_cprr_2024_2025.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "fdi": "tracking_id_og",
                "incident_type": "complainant_type",
                "accused_employee_id": "employee_id",
                "relevant_policy": "allegation_desc"
            }
        )
        .pipe(clean_tracking_id_23)
        .pipe(strip_leading_commas_23)
        .pipe(filter_by_year_2024)
        .pipe(clean_complainant_type)
        .pipe(clean_names_23)
        .pipe(clean_disposition_23)
        .pipe(clean_allegation_23)
        .pipe(correct_disposition_23)
        .pipe(
            standardize_desc_cols, ["tracking_id_og", "investigation_status"]
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "tracking_id"], "allegation_uid")
    
    )
    return df_24


def clean23():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_cprr_2005_2023.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "fdi": "tracking_id_og",
                "incident_type": "complainant_type",
                "accused_employee_id": "employee_id",
                "relevant_policy": "allegation_desc"
            }
        )
        .drop(columns=["allegation", "employee_id", "year"])
        .pipe(filter_years_23)
        .pipe(clean_tracking_id_23)
        .pipe(strip_leading_commas_23)
        .pipe(clean_complainant_type)
        .pipe(clean_names_23)
        .pipe(clean_disposition_23)
        .pipe(clean_allegation_23)
        .pipe(correct_disposition_23)
        .pipe(
            standardize_desc_cols, ["tracking_id_og", "investigation_status"]
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "tracking_id"], "allegation_uid")
    
    )
    return df


_OFFICER_TITLES = re.compile(
    r'(?i)\b(?:officer|sergeant|sgt|detective|det|lieutenant|lt|captain|cpt'
    r'|corporal|cpl|deputy|chief|commander|superintendent|trooper|recruit'
    r'|technician|dispatcher|agent)\b'
)

_SA_DV_PATTERN = re.compile(
    r'(?i)(?:sexual\s*assault|rape|domestic\s*(?:violence|abuse|battery)'
    r'|molestation|indecent\s*behavior|carnal\s*knowledge)',
)

_MINOR_PATTERN = re.compile(
    r'(?i)\b(?:minor|juvenile|child|infant|toddler)\b'
)

_PROPER_NAME = re.compile(r'\b([A-Z][a-z]{2,})\s+([A-Z][a-z]{2,})\b')

_NAME_STOPWORDS = {
    'The', 'This', 'That', 'These', 'Those', 'When', 'Where', 'While',
    'After', 'Before', 'During', 'Since', 'Until', 'About', 'Above',
    'Below', 'Between', 'Through', 'Under', 'Over', 'Into', 'Upon',
    'From', 'With', 'Without', 'Against', 'Along', 'Around', 'Behind',
    'Beyond', 'Toward', 'Within', 'According', 'Also', 'Another',
    'Because', 'Being', 'Both', 'Could', 'Each', 'Even', 'Every',
    'First', 'Second', 'Third', 'Other', 'Same', 'Some', 'Such', 'Than',
    'Their', 'Then', 'There', 'They', 'Very', 'Were', 'What', 'Which',
    'Would', 'Your', 'Have', 'Just', 'Like', 'Make', 'Many', 'More',
    'Most', 'Much', 'Only', 'Still', 'Take', 'Time', 'Will', 'Page',
    'New', 'Orleans', 'Police', 'Department', 'District', 'Officer',
    'Sergeant', 'Detective', 'Lieutenant', 'Captain', 'Corporal',
    'Deputy', 'Chief', 'Commander', 'Superintendent', 'Internal',
    'Affairs', 'Bureau', 'Public', 'Integrity', 'Commission', 'City',
    'Parish', 'State', 'Street', 'Avenue', 'Road', 'Highway', 'Drive',
    'Court', 'Place', 'Boulevard', 'Lane', 'Complainant', 'Investigation',
    'Preliminary', 'Domestic', 'Violence', 'Battery', 'Abuse', 'Sexual',
    'Assault', 'Simple', 'Aggravated', 'False', 'Imprisonment', 'Special',
    'Victims', 'Section', 'Unit', 'Division', 'County', 'East', 'West',
    'North', 'South', 'Central', 'Chapter', 'Paragraph', 'Rule',
    'Performance', 'Professional', 'Conduct', 'Duty',
}


def redact_gist(text):
    """Replace sensitive material in gist text with ***."""
    if not isinstance(text, str):
        return text
    # strip PDF pagination artifacts ("Page 106 of 113")
    text = re.sub(r'\s*Page \d+ of \d+\s*', ' ', text)
    # strip "GIST OF COMPLAINTS RECEIVED:" headers and "Week of ..." lines
    text = re.sub(r'\s*GIST OF COMPLAINTS RECEIVED:\s*', ' ', text)
    text = re.sub(r'\s*WEEKLY COMSTAT\s*', ' ', text)
    text = re.sub(r'\s*Week of [A-Za-z0-9,\s\-]+(?:\d{4})?\s*', ' ', text)
    # phone numbers
    text = re.sub(r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', '***', text)
    # email addresses
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b', '***', text)
    # license plate numbers (keep "plate"/"tag" prefix, redact the number)
    text = re.sub(
        r'(?i)((?:plate|tag|LP)\s*#?\s*)[A-Z]{2,3}\d{2,4}[A-Z]?\d*',
        r'\1***', text
    )
    # street addresses
    text = re.sub(
        r'\b\d{2,5}\s+(?:(?:N|S|E|W)\.?\s+)?'
        r'(?:[A-Z][a-z]+\s+){1,3}'
        r'(?:Street|St\.?|Avenue|Ave\.?|Blvd\.?|Boulevard|Drive|Dr\.?'
        r'|Road|Rd\.?|Lane|Ln\.?|Way|Court|Ct\.?|Place|Pl\.?|Highway|Hwy\.?)\b',
        '***', text
    )
    # full civilian names: Mr./Ms./Mrs./Miss First Last
    text = re.sub(
        r'\b(?:Mr|Ms|Mrs|Miss)\.?\s+[A-Z][a-z]+\s+[A-Z][a-z]+',
        '***', text
    )
    # partial civilian names: Mr./Ms./Mrs./Miss Last
    text = re.sub(
        r'\b(?:Mr|Ms|Mrs|Miss)\.?\s+[A-Z][a-z]+',
        '***', text
    )
    # age + name of minors (e.g. "13 year old Kyla", "13-year-old son, Shante")
    text = re.sub(
        r'(\d{1,2})[\s-]*year[\s-]*old\s*(?:son|daughter|child|boy|girl'
        r'|male|female)?\s*,?\s*([A-Z][a-z]+)',
        r'\1-year-old ***', text
    )
    # redact non-officer proper names in SA/DV and minor/juvenile gists
    if _SA_DV_PATTERN.search(text) or _MINOR_PATTERN.search(text):
        def _redact_name(m):
            first, last = m.group(1), m.group(2)
            if first in _NAME_STOPWORDS or last in _NAME_STOPWORDS:
                return m.group(0)
            # check if preceded by officer title
            start = max(0, m.start() - 40)
            prefix = text[start:m.start()]
            if _OFFICER_TITLES.search(prefix):
                return m.group(0)
            return '***'
        text = _PROPER_NAME.sub(_redact_name, text)
    # collapse multiple whitespace/newlines left by removals
    text = re.sub(r'\n\s*\n', '\n', text)
    text = re.sub(r'  +', ' ', text)
    return text.strip()


def load_gist():
    """Load and normalize all gist CSVs into a lookup DataFrame."""
    gist_dir = deba.data("raw/new_orleans_pd/gist")
    frames = []
    for f in os.listdir(gist_dir):
        if f.endswith('.csv'):
            g = pd.read_csv(os.path.join(gist_dir, f))
            g = g.dropna(subset=['tracking_id'])
            g = g[g['gist'].notna()]
            frames.append(g)
    gist = pd.concat(frames, ignore_index=True)
    gist['gist'] = gist['gist'].apply(redact_gist)

    def norm(tid):
        tid = str(tid).strip().upper().replace('.', '')
        tid = re.sub(r'^(\d{4}-\d{3,4})([A-Z])$', r'\1-\2', tid)
        m = re.match(r'^(\d{4})-(\d{1,3})-([A-Z])$', tid)
        if m:
            tid = f'{m.group(1)}-{m.group(2).zfill(4)}-{m.group(3)}'
        return tid.lower()

    gist['tracking_id_og'] = gist['tracking_id'].apply(norm)
    gist = gist.drop(columns=['tracking_id']).drop_duplicates(subset=['tracking_id_og'], keep='first')
    return gist


def _redact_names_in_sensitive(text):
    """Redact non-officer proper names in text that mentions SA/DV or minors."""
    if not isinstance(text, str):
        return text
    if _SA_DV_PATTERN.search(text) or _MINOR_PATTERN.search(text):
        def _redact_name(m):
            first, last = m.group(1), m.group(2)
            if first in _NAME_STOPWORDS or last in _NAME_STOPWORDS:
                return m.group(0)
            start = max(0, m.start() - 40)
            prefix = text[start:m.start()]
            if _OFFICER_TITLES.search(prefix):
                return m.group(0)
            return '***'
        return _PROPER_NAME.sub(_redact_name, text)
    return text


def join_gist(df, gist):
    """Append gist text to allegation_desc by tracking_id_og."""
    df = df.merge(gist[['tracking_id_og', 'gist']], on='tracking_id_og', how='left')
    mask = df['gist'].notna()
    df.loc[mask, 'allegation_desc'] = (
        df.loc[mask, 'allegation_desc'].fillna('').str.strip()
        + '\n'
        + df.loc[mask, 'gist'].str.strip()
    ).str.strip()
    df['allegation_desc'] = df['allegation_desc'].apply(_redact_names_in_sensitive)
    return df.drop(columns=['gist'])


if __name__ == "__main__":
    gist = load_gist()
    dfa = clean()
    dfb = clean23()
    dfc = clean25()
    dfd = clean24()
    df = pd.concat([dfa, dfb, dfc, dfd])
    df = join_gist(df, gist)
    df.to_csv(deba.data("clean/cprr_new_orleans_pd_2005_2025.csv"), index=False)
