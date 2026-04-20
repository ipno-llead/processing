import deba
import pandas as pd
import re
import csv
from lib.columns import clean_column_names, set_values
from lib.clean import clean_names, standardize_desc_cols, clean_dates
from lib.uid import gen_uid
from datetime import datetime


def extract_case_disposition(df):
    case_disp = (
        df.reason_explanation.str.lower()
        .str.strip()
        .str.extract(r"reason: (\w+) explanation: ")
    )

    df.loc[:, "case_disposition"] = case_disp[0].str.replace(
        r"judgment", "", regex=True
    )
    return df


def extract_officer_name(df):
    names = (
        df.reason_explanation.str.lower()
        .str.strip()
        .str.replace(r"brpd (officer|employee)", "brpd", regex=True)
        .str.replace(r"police officer\,?", "brpd", regex=True)
        .str.replace(r"brpd vehicle operated by", "brpd", regex=False)
        .str.replace(r"when police officer\,", "brpd", regex=True)
        .str.replace(r"ofc\.?", "brpd", regex=True)
        .str.extract(r"brpd (\w+) (\w+) ?\((\w+)?\)?")
    )

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    df.loc[:, "badge_number"] = names[2]
    return df.drop(columns=["reason_explanation"])


def split_attorney_date_col(df):
    data = (
        df.attorney_petition_rec_incident_date.str.lower()
        .str.strip()
        .str.extract(r"attorney: (.+) petition received: (.+) incident date: (.+)")
    )

    df.loc[:, "attorney_name"] = data[0]
    df.loc[:, "claim_receive_date"] = data[1]
    df.loc[:, "claim_occur_date"] = data[2]
    return df


def split_attorney_name(df):
    names = df.attorney_name.str.replace(r"\.", "", regex=True).str.extract(
        r"(\w+) ?(?:(\w) )?(\w+)?$"
    )

    df.loc[:, "attorney_first_name"] = names[0]
    df.loc[:, "attorney_middle_name"] = names[1]
    df.loc[:, "attorney_last_name"] = names[2]
    return df.drop(columns=["attorney_name"])


def clean_claim_receive_date(df):
    df.loc[:, "claim_receive_date"] = (
        df.claim_receive_date.str.replace(
            r"(\w+) (\w+)\, (\w+)", r"\1/\2/\3", regex=True
        )
        .str.replace(r"mar", "3", regex=False)
        .str.replace(r"nov", "11", regex=False)
        .str.replace("may", "5", regex=False)
        .str.replace(r"apr", "4", regex=False)
        .str.replace(r"aug", "8", regex=False)
        .str.replace(r"sep", "9", regex=False)
        .str.replace(r"jun", "6", regex=False)
        .str.replace(r"jul", "7", regex=False)
        .str.replace(r"oct", "10", regex=False)
        .str.replace(r"dec", "12", regex=False)
    )
    return df


def clean_claim_occur_date(df):
    df.loc[:, "claim_occur_date"] = (
        df.claim_occur_date.str.replace(r"(\w+) (\w+)\, (\w+)", r"\1/\2/\3", regex=True)
        .str.replace(r"mar", "3", regex=False)
        .str.replace(r"nov", "11", regex=False)
        .str.replace(r"may", "5", regex=False)
        .str.replace(r"apr", "4", regex=False)
        .str.replace(r"aug", "8", regex=False)
        .str.replace(r"sep", "9", regex=False)
        .str.replace(r"jun", "6", regex=False)
        .str.replace(r"jul", "7", regex=False)
        .str.replace(r"oct", "10", regex=False)
        .str.replace(r"dec", "12", regex=False)
        .str.replace(r"jan", "1", regex=False)
    )
    return df.drop(columns=["attorney_petition_rec_incident_date"])


def split_suit_no_defendant_risk_col(df):
    col = (
        df.suit_no_plaintiff_defendant_risk.str.lower()
        .str.strip()
        .str.extract(r"suit no\. (.+) plaintiff: (.+) police (.+)")
    )
    df.loc[:, "case_number"] = col[0]
    df.loc[:, "case_name"] = col[1].str.replace(r":$", "", regex=True)
    df.loc[:, "risk_number"] = (
        col[2]
        .str.replace(r"(.+):(.+)", r"\2", regex=True)
        .str.replace(r"(n\/a|none)", "", regex=True)
        .str.replace(r"risk #:", "", regex=False)
    )
    return df.drop(columns=["suit_no_plaintiff_defendant_risk"])


def split_date_payment_col(df):
    col = (
        df.closed_date_amount_awarded_description_of_payment.str.lower()
        .str.strip()
        .str.extract(r"closed date:(.+) amount awarded:(.+)description(.+)")
    )
    df.loc[:, "claim_close_date"] = (
        col[0]
        .str.replace(r"^ ", "", regex=True)
        .str.replace(r"^(\w+)\, ", "", regex=True)
        .str.replace(r"(\w+) (\w+)\, (\w+)", r"\1/\2/\3", regex=True)
        .str.replace(r"mar(\w+)", "3", regex=True)
        .str.replace(r"nov(\w+)", "11", regex=True)
        .str.replace(r"may", "5", regex=True)
        .str.replace(r"apr(\w+)", "4", regex=True)
        .str.replace(r"aug(\w+)", "8", regex=True)
        .str.replace(r"sep(\w+)", "9", regex=True)
        .str.replace(r"jun(\w+)", "6", regex=True)
        .str.replace(r"jul(\w+)", "7", regex=True)
        .str.replace(r"oct(\w+)", "10", regex=True)
        .str.replace(r"dec(\w+)", "12", regex=True)
        .str.replace(r"jan(\w+)", "1", regex=True)
        .str.replace(r"feb(\w+)", "1", regex=True)
    )
    df.loc[:, "settlement_amount"] = col[1]
    df.loc[:, "settlement_amount_desc"] = col[2].str.replace(
        r"of payment:", "", regex=False
    )
    return df.drop(columns=["closed_date_amount_awarded_description_of_payment"])


# def clean():
#     df = (
#         pd.read_csv(deba.data("raw/baton_rouge_pd/baton_rouge_pd_settlements_2020.csv"))
#         .pipe(clean_column_names)
#         .pipe(extract_officer_name)
#         .pipe(extract_case_disposition)
#         .pipe(split_attorney_date_col)
#         .pipe(split_attorney_name)
#         .pipe(clean_claim_receive_date)
#         .pipe(clean_claim_occur_date)
#         .pipe(split_suit_no_defendant_risk_col)
#         .pipe(split_date_payment_col)
#         .pipe(set_values, {"agency": "baton-rouge-pd"})
#         .pipe(gen_uid, ["first_name", "last_name", "agency"])
#         .pipe(
#             gen_uid,
#             ["case_name", "case_number", "risk_number", "settlement_amount", "uid"],
#             "settlement_uid",
#         )
#     )
#     return df

def extract_fields(df: pd.DataFrame) -> pd.DataFrame:
    case_numbers = []
    case_names = []
    risk_numbers = []

    col = df["suit_no_plaintiff_defendant_risk"]

    for entry in col:
        entry = str(entry).strip().strip("'")

        case_number_match = re.search(r"Suit No\.?\s*(.*?)\s+Plaintiff:", entry)
        case_number = case_number_match.group(1).strip() if case_number_match else None
        case_numbers.append(case_number)

        case_name_match = re.search(r"Plaintiff:\s*(.+?)\s+Defendant:", entry)
        case_name = case_name_match.group(1).strip().rstrip(",") if case_name_match else None
        case_names.append(case_name)

        risk_number_match = re.search(r"Risk\s+#:\s*(\S+)", entry)
        raw_risk_number = risk_number_match.group(1) if risk_number_match else None
        if raw_risk_number in ("N/A", "none", "None"):
            raw_risk_number = None
        risk_numbers.append(raw_risk_number)

    df = df.copy()
    df["case_number"] = case_numbers
    df["case_name"] = case_names
    df["risk_number"] = risk_numbers

    return df.drop(columns=["suit_no_plaintiff_defendant_risk"])

def extract_attorney_and_dates(df: pd.DataFrame) -> pd.DataFrame:
    first_names = []
    middle_names = []
    last_names = []
    receive_dates = []
    occur_dates = []

    col = df["attorney_petition_rec_incident_date"]

    for entry in col:
        entry = str(entry).strip().strip("'")

        if "Attorney/Petition Rec." in entry:
            first_names.append(None)
            middle_names.append(None)
            last_names.append(None)
            receive_dates.append(None)
            occur_dates.append(None)
            continue

        attorney_match = re.search(
            r"Attorney:\s+(.+?)\s+Petition Received:", entry
        )
        if attorney_match:
            name_tokens = attorney_match.group(1).strip().split()
            if len(name_tokens) == 1:
                first_name, middle_name, last_name = None, None, name_tokens[0]
            elif len(name_tokens) == 2:
                first_name, middle_name, last_name = name_tokens[0], None, name_tokens[1]
            else:
                first_name = name_tokens[0]
                middle_name = " ".join(name_tokens[1:-1])
                last_name = name_tokens[-1]
        else:
            first_name = middle_name = last_name = None

        receive_match = re.search(r"Petition Received:\s+([A-Za-z]{3} \d{2}, \d{4})", entry)
        if receive_match:
            try:
                receive_date = datetime.strptime(receive_match.group(1), "%b %d, %Y").strftime("%m/%d/%Y")
            except ValueError:
                receive_date = None
        else:
            receive_date = None

        occur_match = re.search(r"Incident Date:\s+([A-Za-z]{3} \d{2}, \d{4})", entry)
        if occur_match:
            try:
                occur_date = datetime.strptime(occur_match.group(1), "%b %d, %Y").strftime("%m/%d/%Y")
            except ValueError:
                occur_date = None
        else:
            occur_date = None

        first_names.append(first_name)
        middle_names.append(middle_name)
        last_names.append(last_name)
        receive_dates.append(receive_date)
        occur_dates.append(occur_date)

    df = df.copy()
    df["attorney_first_name"] = first_names
    df["attorney_middle_name"] = middle_names
    df["attorney_last_name"] = last_names
    df["claim_receive_date"] = receive_dates
    df["claim_occur_date"] = occur_dates

    return df.drop(columns=["attorney_petition_rec_incident_date"])

def extract_settlement_fields(df: pd.DataFrame) -> pd.DataFrame:
    close_dates = []
    settlement_amounts = []
    settlement_descriptions = []

    col = df["closed_date_amount_awarded_description_of_payment"]

    for entry in col:
        entry = str(entry).strip().strip("'")

        close_match = re.search(r"Closed Date:\s+\w+,\s+([A-Za-z]+ \d{2}, \d{4})", entry)
        if close_match:
            try:
                close_date = datetime.strptime(close_match.group(1), "%B %d, %Y").strftime("%m/%d/%Y")
            except ValueError:
                close_date = ''
        else:
            close_date = ''
        close_dates.append(close_date)

        amount_match = re.search(r"Amount Awarded:\s+\$([0-9,]+(?:\.\d{2})?)", entry)
        amount = amount_match.group(1) if amount_match else ''
        settlement_amounts.append(amount)

        desc_match = re.search(r"Description of Payment:\s+(.*)", entry)
        desc = desc_match.group(1).strip() if desc_match else ''
        settlement_descriptions.append(desc)

    df = df.copy()
    df["claim_close_date"] = close_dates
    df["settlement_amount"] = settlement_amounts
    df["settlement_amount_desc"] = settlement_descriptions

    return df.drop(columns=["closed_date_amount_awarded_description_of_payment"])

def extract_officers_from_explanation(df: pd.DataFrame) -> pd.DataFrame:
    """Pull officer names from the explanation text and explode to one row per officer.

    The suit caption defendant is almost always 'City of Baton Rouge, et al', so
    officers must be mined from 'Explanation:'. Typical patterns:
        BRPD Shawn Delaney (P594)
        BRPD officer Eric Kenny (P855)    (case varies)
        BRPD Corporal Hilton Riley         (rank varies)
        BRCPD Nathan Davis
        BRCP Officer Rucker                (last-name only)
        officer Jody Ledoux                (no agency)
        BRPD Officers Robert Moruzzi and Jeremy Bourgeois  (multiple)
        anthony augustine with BRPD        (trailing agency)
    Rows with no extractable officer are kept with empty name fields.
    """
    NOISE = {
        "officer", "officers", "police", "public", "chief", "sgt", "sergeant",
        "lieutenant", "lt", "captain", "cpt", "corporal", "cpl", "deputy",
        "detective", "det", "plaintiff", "defendant", "pl", "def", "ck",
        "baton", "rouge", "city", "parish", "east", "west", "north", "south",
        "ms", "mr", "mrs", "dr", "jr", "sr", "the", "a", "an",
    }
    # name_word MUST be case-sensitive — names start with capital letter
    name_word = r"[A-Z][a-z'\-]+"
    badge_pat = r"(?:\s*\(([A-Z]?\d+)\))?"
    # Agency/rank use inline (?i:...) groups so they stay case-insensitive
    # even when the surrounding regex is case-sensitive.
    agency_i = r"(?i:BRPD|BRCPD|BRCP|BRC|BR\s+Police)"
    rank_i = (
        r"(?i:Officers?|Ofc\.?|Corporal|Cpl\.?|Sergeant|Sgt\.?|"
        r"Lieutenant|Lt\.?|Captain|Cpt\.?|Chief|Detective|Det\.?|Deputy)"
    )

    # Pattern A: "<agency> [rank] First Last [and First Last] [(badge)]"
    pat_first_last = re.compile(
        rf"\b{agency_i}\s+(?:{rank_i}\s+)?"
        rf"({name_word})\s+({name_word})"
        rf"(?:\s+and\s+({name_word})\s+({name_word}))?"
        rf"{badge_pat}"
    )
    # Pattern B: "[rank] First Last" without agency
    pat_rank_first_last = re.compile(
        rf"\b{rank_i}\s+({name_word})\s+({name_word})"
        rf"(?:\s+and\s+({name_word})\s+({name_word}))?"
        rf"{badge_pat}"
    )
    # Pattern C: "<agency> [rank] Last" (single-name when agency+rank marker present)
    pat_single = re.compile(
        rf"\b{agency_i}\s+{rank_i}\s+({name_word})"
        rf"{badge_pat}"
    )
    # Pattern D: "First Last[,]? with|of <agency>"
    pat_trailing_agency = re.compile(
        rf"\b({name_word})\s+({name_word}),?\s+(?:with|of)\s+{agency_i}\b"
    )
    # Pattern E: "[preposition] [police]? officer/ofc LastName" — single last-name only
    # Requires a contextual word before "officer" to reduce false positives.
    # The captured name must begin with a capital letter, naturally excluding
    # "Officer was ...", "Officer arrested ...", etc.
    pat_officer_single = re.compile(
        rf"(?i:\b(?:by|with|when|from|against|and|,|defendant,?|arresting|that|while)\s+"
        rf"(?:police\s+)?(?:Officers?|Ofc\.?|Office))\s*,?\s*({name_word})\b"
    )
    # Pattern F: "<agency> [employee|vehicle driven by|vehicle operated by] First Last"
    pat_agency_employee = re.compile(
        rf"\b{agency_i}\s+(?i:employee,?\s*|vehicle\s+(?:driven|operated)\s+by\s+)"
        rf"({name_word})\s+({name_word})"
    )
    # Pattern G: "First Last (<agency>)" — agency in parens after name
    pat_paren_agency = re.compile(
        rf"\b({name_word})\s+({name_word})\s*\(\s*{agency_i}\s*\)"
    )
    # Pattern H: "[police]? officer/office/ofc[,] First Last" — comma variant
    pat_officer_comma = re.compile(
        rf"(?i:\b(?:police\s+)?(?:Officers?|Office|Ofc\.?))\s*,\s*({name_word})\s+({name_word})"
    )
    # Pattern I: Officer/Ofc at sentence start — "Officer Thomas was..." / "Ofc. Dave Davis"
    # Only catches when followed by capitalized word; NOT preceded by letters (must be start of text or after . or ;)
    pat_officer_sentence_start = re.compile(
        rf"(?:^|(?<=[.;\n]))\s*(?i:Officers?|Ofc\.?)\s+({name_word})(?:\s+({name_word}))?"
    )
    # Pattern J: "First Last, who works for (Police|<agency>)"
    pat_works_for = re.compile(
        rf"\b({name_word})\s+({name_word})\s*,?\s+who\s+works\s+for\s+"
        rf"(?i:Police|{agency_i})"
    )
    # Pattern L: "[rank]. First Last" — rank with period followed by full name (e.g. "Ofc. Dave Davis")
    pat_rank_dot_name = re.compile(
        rf"(?i:\b(?:Ofc|Sgt|Lt|Cpl|Cpt|Det)\.)\s+({name_word})\s+({name_word})"
    )

    # Case-insensitive keyword-within-match test
    # Ensures the capture string itself contains an agency or rank keyword
    def has_agency_or_rank(text):
        return bool(re.search(rf"\b(?:{agency}|{rank})\b", text, re.IGNORECASE))

    def clean_name_pair(first, last):
        if not first or not last:
            return None
        # Case-insensitive noise check
        if first.lower() in NOISE or last.lower() in NOISE:
            return None
        # Names must start with capital in the original explanation
        if not (first[0].isupper() and last[0].isupper()):
            return None
        return (first, last)

    records = []
    for _, row in df.iterrows():
        explanation = str(row.get("disposition_desc") or "")
        officers = []
        consumed_spans = []

        def overlaps(start, end):
            return any(not (end <= s or start >= e) for s, e in consumed_spans)

        # Pattern A first (highest signal)
        for m in pat_first_last.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            pair = clean_name_pair(m.group(1), m.group(2))
            if pair:
                officers.append((pair[0], pair[1], m.group(5)))
                consumed_spans.append((m.start(), m.end()))
            pair2 = clean_name_pair(m.group(3), m.group(4))
            if pair2:
                officers.append((pair2[0], pair2[1], None))

        # Pattern B: rank + first + last (no agency)
        for m in pat_rank_first_last.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            pair = clean_name_pair(m.group(1), m.group(2))
            if pair:
                officers.append((pair[0], pair[1], m.group(5)))
                consumed_spans.append((m.start(), m.end()))
            pair2 = clean_name_pair(m.group(3), m.group(4))
            if pair2:
                officers.append((pair2[0], pair2[1], None))

        # Pattern C: "<agency> <rank> LastName" (last-name only)
        for m in pat_single.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            last = m.group(1)
            if last.lower() in NOISE or not last[0].isupper():
                continue
            officers.append((None, last, m.group(2)))

        # Pattern D: "First Last with/of <agency>"
        for m in pat_trailing_agency.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            pair = clean_name_pair(m.group(1), m.group(2))
            if pair:
                officers.append((pair[0], pair[1], None))
                consumed_spans.append((m.start(), m.end()))

        # Pattern F: "<agency> employee/vehicle driven by First Last"
        for m in pat_agency_employee.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            pair = clean_name_pair(m.group(1), m.group(2))
            if pair:
                officers.append((pair[0], pair[1], None))
                consumed_spans.append((m.start(), m.end()))

        # Pattern G: "First Last (<agency>)"
        for m in pat_paren_agency.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            pair = clean_name_pair(m.group(1), m.group(2))
            if pair:
                officers.append((pair[0], pair[1], None))
                consumed_spans.append((m.start(), m.end()))

        # Pattern H: "officer, First Last" or "police officer, First Last"
        for m in pat_officer_comma.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            pair = clean_name_pair(m.group(1), m.group(2))
            if pair:
                officers.append((pair[0], pair[1], None))
                consumed_spans.append((m.start(), m.end()))

        # Pattern J: "First Last, who works for Police/<agency>"
        for m in pat_works_for.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            pair = clean_name_pair(m.group(1), m.group(2))
            if pair:
                officers.append((pair[0], pair[1], None))
                consumed_spans.append((m.start(), m.end()))

        # Pattern L: "Ofc./Sgt./Lt. First Last"
        for m in pat_rank_dot_name.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            pair = clean_name_pair(m.group(1), m.group(2))
            if pair:
                officers.append((pair[0], pair[1], None))
                consumed_spans.append((m.start(), m.end()))

        # Pattern I: "Officer First Last" or "Ofc. Last" at sentence start
        for m in pat_officer_sentence_start.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            first = m.group(1)
            last = m.group(2)
            if last:
                pair = clean_name_pair(first, last)
                if pair:
                    officers.append((pair[0], pair[1], None))
                    consumed_spans.append((m.start(), m.end()))
            else:
                # Single name — treat as last name
                if first and first[0].isupper() and first.lower() not in NOISE:
                    officers.append((None, first, None))
                    consumed_spans.append((m.start(), m.end()))

        # Pattern E: single-name "[prep] officer LastName" (last, no first)
        # Run AFTER two-name patterns so we don't double-match
        for m in pat_officer_single.finditer(explanation):
            if overlaps(m.start(), m.end()):
                continue
            last = m.group(1)
            if last.lower() in NOISE or not last[0].isupper():
                continue
            officers.append((None, last, None))
            consumed_spans.append((m.start(), m.end()))

        # dedupe while preserving order
        seen = set()
        unique_officers = []
        for o in officers:
            key = (o[0].lower() if o[0] else "", o[1].lower() if o[1] else "")
            if key not in seen:
                seen.add(key)
                unique_officers.append(o)

        if not unique_officers:
            new_row = row.copy()
            new_row["first_name"] = ""
            new_row["last_name"] = ""
            new_row["badge_number"] = ""
            records.append(new_row)
        else:
            for first, last, badge in unique_officers:
                new_row = row.copy()
                new_row["first_name"] = first or ""
                new_row["last_name"] = last or ""
                new_row["badge_number"] = badge or ""
                records.append(new_row)

    return pd.DataFrame(records).reset_index(drop=True)


def extract_case_disposition(df: pd.DataFrame) -> pd.DataFrame:
    dispositions = []
    descriptions = []

    col = df["reason_explanation"]

    for entry in col:
        entry = str(entry).strip().strip("'")

        reason_match = re.search(r"Reason:\s*(.+?)\s+Explanation:", entry)
        disposition = reason_match.group(1).strip() if reason_match else None
        dispositions.append(disposition)

        explanation_match = re.search(r"Explanation:\s*(.*)", entry)
        description = explanation_match.group(1).strip() if explanation_match else None
        descriptions.append(description)

    df = df.copy()
    df["case_disposition"] = dispositions
    df["disposition_desc"] = descriptions

    return df.drop(columns=["reason_explanation"])

def fix_first_name(df: pd.DataFrame) -> pd.DataFrame:
    def fix(name):
        if name is None or (isinstance(name, float) and pd.isna(name)):
            return ""
        name = str(name).strip().lower()
        if name in ("", "none", "nan"):
            return ""
        if name == "officer nathan etal":
            return "nathan"
        elif name == "ltd":
            return ""
        elif name == "c urtis":
            return "curtis"
        elif name == "officer isaac":
            return "isaac"
        else:
            return name.title()

    df = df.copy()
    df["first_name"] = df["first_name"].apply(fix)
    return df

def strip_double_quotes(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: x.strip('"') if isinstance(x, str) else x)

def remove_commas_from_strings(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: x.replace(",", "") if isinstance(x, str) else x)


def blank_uid_for_missing_officers(df: pd.DataFrame) -> pd.DataFrame:
    """Blank out `uid` on rows with no officer name so they don't collapse
    onto a single phantom officer in downstream fuse."""
    df = df.copy()
    missing = (df["first_name"].fillna("") == "") & (df["last_name"].fillna("") == "")
    df.loc[missing, "uid"] = ""
    return df


def clean_16():
    df16 = (
        pd.read_csv(deba.data("raw/baton_rouge_pd/baton_rouge_pd_settlements_2016_2023.csv"))
        .pipe(clean_column_names)
        .drop_duplicates()
        .pipe(extract_fields)
        .pipe(extract_attorney_and_dates)
        .pipe(extract_settlement_fields)
        .pipe(extract_case_disposition)
        .pipe(extract_officers_from_explanation)
        .pipe(fix_first_name)
        .pipe(remove_commas_from_strings)
        .pipe(strip_double_quotes)
        .pipe(clean_names, ["first_name", "last_name", "attorney_first_name", "attorney_middle_name", "attorney_last_name"])
        .pipe(standardize_desc_cols, ["case_name", "case_number", "risk_number", "settlement_amount", "settlement_amount_desc", "case_disposition", "disposition_desc"])
        .pipe(clean_dates, ["claim_receive_date", "claim_occur_date", "claim_close_date"])
        .pipe(set_values, {"agency": "baton-rouge-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(blank_uid_for_missing_officers)
        .pipe(
            gen_uid,
            ["case_name", "case_number", "risk_number", "settlement_amount", "uid"],
            "settlement_uid",
        )
        .drop_duplicates(subset=["settlement_uid"], keep="first")
        .reset_index(drop=True)
    )
    return df16


if __name__ == "__main__":
    # df = clean()
    df16 = clean_16()
    # df.to_csv(deba.data("clean/settlements_baton_rouge_pd_2020.csv"), index=False)
    df16.to_csv(deba.data("clean/settlements_baton_rouge_pd_2016_2023.csv"), index=False, quoting=csv.QUOTE_MINIMAL)

