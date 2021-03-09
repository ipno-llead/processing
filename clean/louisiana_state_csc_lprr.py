from lib.clean import clean_dates, clean_names, standardize_desc_cols
from lib.columns import clean_column_names
from lib.path import data_file_path, ensure_data_dir
from lib.ref import is_lastname
from lib.uid import gen_uid
import pandas as pd
import re
import sys
sys.path.append("../")


def standardize_appealed(df):
    df.loc[:, "appealed"] = df.appealed.str.strip()
    return df


def split_appellant_name(df):
    def process_name(s):
        # skip processing names with forward slash
        if "/" in s:
            return s

        # if comma is found in name then assume order is last name then first name
        if "," in s:
            s = re.sub(r",\s*", " ", s)
            m = re.match(r"^(\w+(?: (?:jr|sr|i|ii|iii|iv|v))?) (.+)$", s)
            last_name = m.group(1)
            first_name = m.group(2)
            return "%s@@%s" % (last_name, first_name)

        # if dash is found then we can identify compound last name
        if "-" in s:
            m = re.match(r"^(\w+-\w+)(?: (.+))?$", s)
            last_name = m.group(1)
            first_name = m.group(2)
            return "@@".join(filter(None, [last_name, first_name]))

        # if last name prefix can be found in the middle of the name then last name is first
        m = re.match(r"^(\w+(?: (?:jr|sr|i|ii|iii|iv|v))) (.+)$", s)
        if m is not None:
            last_name = m.group(1)
            first_name = m.group(2)
            return "%s@@%s" % (last_name, first_name)

        # if middle initial can be found in the middle then last name is last
        m = re.match(r"^(\w+(?: \w)) (.+)$", s)
        if m is not None:
            last_name = m.group(2)
            first_name = m.group(1)
            return "%s@@%s" % (last_name, first_name)

        parts = s.split(" ")
        # no whitespace so assume to be last name
        if len(parts) == 1:
            return parts[0]

        # found last name at the beginning
        if is_lastname(parts[0]):
            return "@@".join([parts[0], " ".join(parts[1:])])

        # found last name at the second word
        if is_lastname(parts[1]):
            return "@@".join([" ".join(parts[1:]), parts[0]])

        return "@@".join([parts[0], " ".join(parts[1:])])

    names = df.appellant.str.strip().str.lower().fillna("").str.replace(r"[\.\s\"]+", " ")\
        .map(process_name).str.split("@@", expand=True)
    df.loc[:, "last_name"] = names.iloc[:, 0].str.strip().fillna("")
    names = names.iloc[:, 1].str.strip().str.replace(r" (\w+)$", r"@@\1")\
        .str.split("@@", expand=True)
    df.loc[:, "first_name"] = names.iloc[:, 0].fillna("")
    df.loc[:, "middle_name"] = names.iloc[:, 1].map(
        lambda x: x if x and len(x) > 1 else "")
    df.loc[:, "middle_initial"] = names.iloc[:, 1].map(
        lambda x: "" if x is None or x == "" else x[0])

    df = df.drop(columns=["appellant"])
    return df


def standardize_delay(df):
    df.loc[:, "delay"] = df.delay.str.replace(r"[\.]", "").str.replace("Smos", "5 months", regex=False)\
        .str.replace(r"\b(I|l)\s*(mos|mo|man)\b", "1 months")\
        .str.replace(r"(\d)([A-Za-z])", r"\1 \2").str.replace(r"([a-z])(\d)", r"\1 \2")\
        .str.strip().str.lower().str.replace(r"\byr\b", "years")\
        .str.replace(r"\b(mos|man|imos|mon|nos|mas|mo|mes|mor)\b", "months")\
        .str.replace(r"^[^\d]+", "").str.replace(r"\b1 (\w+)s\b", r"1 \1").fillna("")
    return df


def standardize_rendered_date(df):
    df.loc[:, "rendered_date"] = df.rendered_date.str.replace(r"[\.-]", "/")
    return df


def clean_docket_no(df):
    df.loc[:, "docket_no"] = df.docket_no.str.strip().str.replace(r"1(\d)\/(\d+)\/05", r"9\1-\2-S")\
        .str.replace(r"1(\d)\/(\d+)\/00", r"9\1-\2-)")\
        .str.replace(r"[\. ]", "-").str.replace(r"-(\d{3})(\w)", r"-\1-\2")\
        .str.replace(r"-(015|0\/5)$", "-O/S").str.replace(r"^D", "0")\
        .str.replace(r"O-", "0-").str.replace(r"^0A", "04")\
        .str.replace(r"-(0|\))$", "-O").str.replace(r"-3$", "-S")\
        .str.replace(r"-5$", "-S").str.replace(r"^04-148-1$", "04-148-S")\
        .str.replace(r"^5-156$", "05-156-S").str.replace(r"^19-244-$", "19-244-T").fillna("")
    return df


def drop_empty_docket_no(df):
    return df[df.docket_no != ""].reset_index(drop=True)


def clean():
    df = pd.read_csv(data_file_path(
        "louisiana_state_csc/louisianastate_csc_lprr_1991-2020.csv"))
    df = clean_column_names(df)
    df = df.rename(columns={
        'docket': 'docket_no',
        'apellant': 'appellant',
        'colonel': 'charging_supervisor',
        'filed': 'filed_date',
        'rendered': 'rendered_date'
    })
    # df.columns = [
    #     'docket_no', 'appellant', 'counsel', 'filed_date', 'rendered_date', 'resolution', 'delay', 'appealed']
    # df = df\
    #     .pipe(standardize_appealed)\
    #     .pipe(split_appellant_name)\
    #     .pipe(clean_names, ["counsel"])\
    #     .pipe(standardize_desc_cols, ["resolution"])\
    #     .pipe(standardize_delay)\
    #     .pipe(standardize_rendered_date)\
    #     .pipe(clean_dates, ["filed_date", "rendered_date"])\
    #     .pipe(gen_uid, ["first_name", "middle_initial", "last_name"])\
    #     .pipe(clean_docket_no)\
    #     .pipe(drop_empty_docket_no)
    return df\
        .pipe(standardize_appealed)


if __name__ == "__main__":
    df = clean()
    ensure_data_dir("clean")
    df.to_csv(data_file_path(
        "clean/lprr_louisiana_state_csc_1991_2020.csv"), index=False)
