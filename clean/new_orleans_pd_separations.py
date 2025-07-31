import deba
import re
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import standardize_desc_cols, clean_names, clean_dates, clean_names
from lib.uid import gen_uid


def split_names(df):
    names = (
        df.name.str.lower()
        .str.strip()
        .str.replace(r"(\w+)\.(\w+)", r"\1\2", regex=True)
        .str.replace(r"(\w+?\'?\.?\w+) \, \'(\w+)", r"\2 \1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\w+) ?(\w+?-?\'?\w+)? ?-?(iv|los angeles|charles|ortiz)?")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")

    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["name", "suffix"])


def strip_leading_commas(df):
    for col in df.columns:
        df[col] = df[col].str.replace(r"^\'", "", regex=True)
    return df


def sanitize_dates(df):
    df.loc[:, "left_date"] = df.separation_date.str.replace(
        r"(\w+) $", r"\1", regex=True
    ).str.replace(r"^(\w+)$", "", regex=True)
    df.loc[:, "hire_date"] = df.hire_date.str.replace(
        r"(\w+) $", r"\1", regex=True
    ).str.replace(r"^(\w+)$", "", regex=True)
    return df.drop(columns=["separation_date"])


def clean_rank_desc(df):
    df.loc[:, "rank_desc"] = (
        df.job_title.str.lower().str.strip().str.replace(r" ?police ?", " ", regex=True)
    )
    return df.drop(columns=["job_title"])


def clean_left_reason_desc21(df):
    df.loc[:, "left_reason_desc"] = df.reason.str.cat(df.resignation_reason, sep=" ")
    df.loc[:, "left_reason_desc"] = (
        df.left_reason_desc.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"(\w+) +", r"\1 ", regex=True)
        .str.replace(
            r"(deceased|resign(ed)?-?|terminat(ed|ion)|retired|involuntary|dismissal ?-? ?)-? ? ?",
            "",
            regex=True,
        )
        .str.replace(r"better job better job", "better job", regex=False)
        .str.replace(r"(\w+)\((.+)\)", r"\1 \2", regex=True)
        .str.replace(r"unknown other", "unknown/other", regex=False)
        .str.replace(r"oft", "off", regex=True)
        .str.replace(r"obligation\b", "obligations", regex=True)
        .str.replace(r"other (.+)", r"other/\1", regex=True)
        .str.replace(r"better job (.+)", r"better job/\1", regex=True)
        .str.replace(r"personal- (.+)", r"personal/\1", regex=True)
        .str.replace(r"home-(\w+)", r"home \1", regex=True)
        .str.replace(
            r"dissatisfied &better job", "better job/dissatisfied with job", regex=False
        )
        .str.replace(r"(\w+)\, (\w+)", r"\1/\2", regex=True)
        .str.replace(r"(\w+)job", r"\1 job", regex=True)
        .str.replace(r"unsatifactory", "unsatisfactory", regex=False)
        .str.replace(r"city unknown", "city/unknown", regex=False)
    )
    return df.drop(columns=["resignation_reason"])


def extract_left_reason_desc18(df):
    df.loc[:, "left_reason_desc"] = (
        df.reason.str.lower()
        .str.strip()
        .str.replace(r"(\w+)-? ?(.+)", r"\2", regex=True)
        .str.replace(r"w\/", "with", regex=True)
    )
    return df


def clean_left_reason(df):
    reasons = (
        df.reason.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r" $", "", regex=True)
        .str.replace(r"^etired", "retired", regex=True)
        .str.replace(r"^esign", "resigned", regex=True)
        .str.extract(
            r"(deceased-? ?|resigne?d?-? ?|retirement-? ?|involuntary.+|dismissal-? ?|termination|retired)"
        )
    )

    df.loc[:, "left_reason"] = (
        reasons[0]
        .fillna("")
        .str.replace(r"(-| $)", "", regex=True)
        .str.replace(r"^resign$", "resigned", regex=True)
    )
    return df.drop(columns=["reason"])


def clean_rank_desc22(df):
    df.loc[:, "rank_desc"] = (
        df.job_title.str.lower()
        .str.strip()
        .str.replace("police", "", regex=True)
        .str.replace(r"(\w+) +(\w+)", r"\1 \2", regex=True)
        .str.replace(r"(\w+) +$", r"\1", regex=True)
        .str.replace(r"^ +(\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) senior$", r"\1", regex=True)
        .str.replace(r"supt\. of", "superintendant", regex=True)
    )
    return df.drop(columns=["job_title"])


def correct_dates_2022(df):
    df.loc[:, "hire_date"] = df.hire_date.str.replace(
        r"12\/13\/015", r"12/13/2015", regex=True
    )
    return df


def clean_left_reason22(df):
    df.loc[:, "left_reason"] = (
        df.reason.str.lower()
        .str.strip()
        .str.replace(r"(\w+) +$", r"\1", regex=True)
        .str.replace(r"deceas", "deceased", regex=False)
        .str.replace(r"^retirement$", "retired", regex=True)
        .str.replace(r"\/", "; ", regex=True)
    )
    return df.drop(columns=["reason"])


def clean_left_reason_desc22(df):
    df.loc[:, "left_reason_desc"] = (
        df.reason_1.str.lower()
        .str.strip()
        .str.replace(r"job d$", "job", regex=True)
        .str.replace(r"(\/|\,)", "; ", regex=True)
        .str.replace(r"relocation better", "relocation; better", regex=False)
        .str.replace(r"^better job better job\,?$", "better job", regex=True)
    )
    return df.drop(columns=["reason_1"])


def clean_years_of_service(df):
    df.loc[:, "years_of_service"] = (
        df.years_of_service.str.lower()
        .str.strip()
        .str.replace(r"(\w+)(yrs?|mos?)", r"\1 \2", regex=True)
        .str.replace(r"yrs?\.?", "years", regex=True)
        .str.replace(r"mos?", "months", regex=True)
    )
    return df


def clean21():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/nopd_cprr_separations_2019-2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "class": "class_id",
                "assignment": "assignment_id",
                "date": "left_date",
            }
        )
        .pipe(strip_leading_commas)
        .pipe(split_names)
        .pipe(clean_rank_desc)
        .pipe(clean_left_reason_desc21)
        .pipe(clean_left_reason)
        .pipe(sanitize_dates)
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["employee_id", "rank_desc", "assignment_id"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["left_reason", "left_reason_desc", "left_date"], "separation_uid"
        )
    )
    return df


def clean18():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_separations_2018.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "class": "class_id",
                "assign": "assignment_id",
                "date": "left_date",
            }
        )
        .pipe(extract_left_reason_desc18)
        .pipe(clean_left_reason)
        .pipe(clean_dates, ["hire_date"])
        .pipe(standardize_desc_cols, ["class_id", "assignment_id"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["left_reason", "left_reason_desc", "left_date"], "separation_uid"
        )
    )
    return df


def clean22():
    df = (
        pd.read_csv(deba.data("raw/new_orleans_pd/nopd_cprr_separations_2022.csv"))
        .pipe(clean_column_names)
        .rename(columns={"separation_date": "left_date"})
        .pipe(strip_leading_commas)
        .pipe(correct_dates_2022)
        .pipe(split_names)
        .pipe(clean_dates, ["hire_date"])
        .pipe(clean_rank_desc22)
        .pipe(clean_left_reason22)
        .pipe(clean_left_reason_desc22)
        .pipe(clean_years_of_service)
        .pipe(
            standardize_desc_cols,
            [
                "left_reason",
                "left_reason_desc",
                "years_of_service",
                "employee_id",
                "rank_desc",
                "left_date",
            ],
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["left_reason", "left_reason_desc", "left_date"], "separation_uid"
        )
    )
    return df


def clean(df18, df21, df22):
    df = pd.concat([df18, df21, df22], axis=0).drop_duplicates(subset=["uid"])
    return df

def split_names_25(df):
    names = (
        df.officer_name.str.lower()
        .str.strip()
        .str.replace(r"(\w+)\.(\w+)", r"\1\2", regex=True)
        .str.replace(r"(\w+?\'?\.?\w+) \, \'(\w+)", r"\2 \1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\w+) ?(\w+?-?\'?\w+)? ?-?(iv|los angeles|charles|ortiz)?")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")

    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["officer_name"])

def clean_employee_id_format(df):
    """
    Cleans the employee_id column:
    - Removes single quotes and whitespace
    - Removes leading zeroes
    - Replaces real NaNs with empty string
    """
    df = df.copy()

    # First, replace real NaNs with empty string
    df['employee_id'] = df['employee_id'].fillna("")

    # Then, clean up the rest
    df['employee_id'] = (
        df['employee_id']
        .astype(str)
        .str.strip()
        .str.replace("'", "")
        .str.lstrip("0")
        .replace("nan", "")  # In case any slipped through
    )

    return df

def clean_years_of_service(df):
    """
    Cleans and converts 'years_of_service' strings to float values (as string).
    Modifies the 'years_of_service' column in place.
    """
    df = df.copy()

    def parse_duration(value):
        if not isinstance(value, str):
            return ''

        # Normalize spacing and lowercase
        value = value.lower().strip()
        value = re.sub(r'\s+', ' ', value)

        # Remove stray non-time strings
        if not re.search(r'\d', value):
            return ''
        if re.match(r'^\d{4,}$', value):  # e.g. '37572'
            return ''

        # Extract years and months
        year_match = re.search(r'(\d+)\s*(yrs|years|year|yr)', value)
        month_match = re.search(r'(\d+)\s*(mos|months|month|mo)', value)

        years = int(year_match.group(1)) if year_match else 0
        months = int(month_match.group(1)) if month_match else 0

        return str(round(years + months / 12, 2))

    df['years_of_service'] = df['years_of_service'].apply(parse_duration)

    return df

def clean_rank_desc_25(df):
    """
    Cleans and standardizes the 'rank_desc' column in place.
    """
    df = df.copy()

    rank_map = {
        'police sergeant': 'sergeant',
        'police sgt.': 'sergeant',
        'sergeant': 'sergeant',
        'police lieutenant': 'lieutenant',
        'lieutenant': 'lieutenant',
        'captain': 'captain',
        'senior police officer': 'senior officer',
        'senior police office': 'senior officer',
        'police officer ii': 'police officer',
        'police officer': 'police officer',
        'police recruit': 'recruit',
        'police recuit': 'recruit',
        'recruit': 'recruit',
        'police aide': 'aide',
        'deputy chief of staff': 'deputy chief of staff',
        'asst. supt. of police': 'assistant superintendent',
        'superintendent': 'superintendent',
        '': ''
    }

    df['rank_desc'] = (
        df['rank_desc']
        .astype(str)
        .str.strip()
        .str.lower()
        .map(rank_map)
        .fillna(df['rank_desc'])  # fallback to original if no match
    )

    return df

def clean_left_reason_25(df):
    """
    Cleans and standardizes the 'left_reason' column in place.
    """
    df = df.copy()

    reason_map = {
        'resigned': 'resigned',
        'resignation': 'resigned',
        'retired': 'retired',
        'retirement': 'retired',
        'reteired': 'retired',
        'terminated': 'terminated',
        'terminaton': 'terminated',
        'deceased': 'deceased',
        'death': 'deceased',
        'rule ix': 'rule ix',
        'unknown': 'unknown',
        'other': 'other',
    }

    def standardize_reason(val):
        val = str(val).strip().lower()

        if val in reason_map:
            return reason_map[val]

        # Group by patterns
        if any(k in val for k in ['better job', 'another job', 'dissatisfied', 'entrepreneur']):
            return 'job-related'
        elif any(k in val for k in ['home obligations', 'relocation', 'financial', 'moved', 'care']):
            return 'personal'
        elif any(k in val for k in ['health', 'school', 'military']):
            return 'personal'
        elif val == '':
            return ''
        else:
            return ''

    df['left_reason'] = df['left_reason'].apply(standardize_reason)

    return df

def clean_left_reason_desc_25(df):
    """
    Cleans and standardizes 'left_reason_desc' in place.
    """
    df = df.copy()

    def standardize_desc(val):
        val = str(val).strip().lower()

        if val in ['', 'no reason known']:
            return ''
        elif val in ['retired', 'retirement']:
            return 'retirement'
        elif val == 'resignation':
            return 'resignation'
        elif val == 'death':
            return 'death'
        elif val in ['home obligations', 'better job', 'personal reasons', 'pursuing different career p', 'entered school']:
            return 'personal'
        elif 'probationary failure' in val or 'training' in val or 'traini' in val:
            return 'probationary failure'
        elif 'moral conduct' in val or 'misconduct' in val:
            return 'misconduct'
        else:
            return 'other'

    df['left_reason_desc'] = df['left_reason_desc'].apply(standardize_desc)

    return df

def replace_nan_with_empty_string(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna("")

def drop_name_duplicates(df25: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops rows from df25 where first_name + last_name match any in df.
    """
    df = df.copy()
    df25 = df25.copy()

    # Normalize names
    df["full_name"] = df["first_name"].str.lower().str.strip() + "|" + df["last_name"].str.lower().str.strip()
    df25["full_name"] = df25["first_name"].str.lower().str.strip() + "|" + df25["last_name"].str.lower().str.strip()

    # Filter out matches
    df25 = df25[~df25["full_name"].isin(df["full_name"])].drop(columns="full_name")

    return df25

def clean25():
    df25 = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_separations_2025.csv"))
        .pipe(clean_column_names)
        .drop(columns=["unnamed"])
        .rename(columns={"separation_date": "left_date", "job_title": "rank_desc", "reason_main": "left_reason", "reason_detail": "left_reason_desc"})
        .pipe(strip_leading_commas)
        .pipe(correct_dates_2022)
        .pipe(split_names_25)
        .pipe(correct_dates_2022)
        .pipe(clean_dates, ["hire_date"])
        .pipe(clean_employee_id_format)
        .pipe(clean_dates, ["left_date"])
        .pipe(clean_years_of_service)
        .pipe(clean_rank_desc_25)
        .pipe(clean_left_reason_25)
        .pipe(clean_left_reason_desc_25)
        .pipe(
            standardize_desc_cols,
            [
                "left_reason",
                "left_reason_desc",
                "years_of_service",
                "employee_id",
                "rank_desc",
            ],
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid, ["left_reason", "left_reason_desc", "left_year"], "separation_uid"
        )
        .pipe(replace_nan_with_empty_string)
        .pipe(drop_name_duplicates, df)
        .drop_duplicates(subset=["uid"])
        .drop_duplicates(subset=["separation_uid"])
    )
    return df25

if __name__ == "__main__":
    df18 = clean18()
    df21 = clean21()
    df22 = clean22()
    df = clean(df18, df21, df22)
    df25 = clean25()
    df.to_csv(deba.data("clean/pprr_seps_new_orleans_pd_2018_2022.csv"), index=False)
    df25.to_csv(deba.data("clean/pprr_seps_new_orleans_pd_2022_2025.csv"), index=False)
