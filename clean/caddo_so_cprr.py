import deba
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates
from lib.uid import gen_uid
import pandas as pd


def strip_leading_commas(df):
    for col in df.columns:
        df = df.apply(lambda col: col.replace(r"^\'", "", regex=True))
    return df

def extract_date_received(df):
    dates = df.tracking_id.str.extract(r"(\w+/\w+/\w+)")

    df.loc[:, "receive_date"] = dates[0]
    return df 

def clean_tracking_id(df):
    df.loc[:, "tracking_id"] = df.tracking_id.str.lower().str.strip().str.replace(r" (\w+/\w+/\w+)", "", regex=True)
    return df 

def clean_allegation(df):
    df.loc[:, "allegation"] = df.allegation.str.lower().str.strip().str.replace(r"^complaint: ", "", regex=True)
    return df 

def clean_action(df):
    df.loc[:, "action"] = (df.action.str.lower().str.strip().str.replace(r"action: ?", "", regex=True)
                           .str.replace(r"oral counseling", "oral reprimand", regex=False)
    )
    return df

def clean_disposition(df):
    df.loc[:, "disposition"]  = (df.final_disposition.str.lower().str.strip().str.replace(r"^result\/disposition: ?", "", regex=True)
                                 .str.replace(r"^closed (-|as) ?", "", regex=True)
                                 .str.replace(r"^(disciplinary|corrective|closed)(.+)", "sustained", regex=True)
                                 .str.replace(r"no action taken", "", regex=False)
    )
    return df.drop(columns=["final_disposition"])

def split_rows_with_multiple_officers(df):
    df.loc[:, "officer_name"] = (df.officer_name.str.lower().str.strip().str.replace(
        r"accused deputies: ", "", regex=True
    ).str.replace(r"Daniel Clayton #2666 \(OPS\) Christopher Davis #1444 \(OPS\)", "Daniel Clayton #2666 (OPS), Christopher Davis #1444 (OPS)", regex=True)
     .str.replace(r"Joseph Fourcade #1891 \(OPS\) Brian Johnson #1970 \(OPS\)", "Joseph Fourcade #1891 (OPS), Brian Johnson #1970 (OPS)", regex=True)
    )

    df = (
        df.drop("officer_name", axis=1)
        .join(
            df["officer_name"]
            .str.split(",", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("officer_name"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df[~((df.officer_name.fillna("") == ""))]

def extract_badge_num(df):
    badges = df.officer_name.str.extract(r"#(\w+)")

    df.loc[:, "badge_no"] = badges[0]
    return df

def extract_rank_desc(df):
    ranks = df.officer_name.str.extract(r"(\(\w+\))")
    df.loc[:, "rank_desc"] = ranks[0].str.replace(r"(\(|\))", "", regex=True).str.replace(r"det", "detective", regex=False).str.replace(r"(ops|adm|na)", "", regex=True)
    return df 

def split_name(df):
    names = df.officer_name.str.extract(r"^(\w+) (\w+)")
    df.loc[:, "first_name"]  = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns=["officer_name"])

def split_supervisor_name(df):
    df.loc[:, "assigned_to"] = df.assigned_to.str.lower().str.strip().str.replace(r"preliminary assigned to: ?", "", regex=True).str.replace(r"#", "", regex=False)

    names = df.assigned_to.str.extract(r"(\w+) (\w+) (\w+)")
    df.loc[:, "supervisor_first_name"] = names[0]
    df.loc[:, "supervisor_last_name"] = names[1]
    df.loc[:, "supervisor_badge_no"] = names[2]
    return df.drop(columns=["assigned_to"])


def clean():
    df = (pd.read_csv(deba.data("raw/caddo_so/cprr_caddo_so_2022-2023.csv"))
          .pipe(clean_column_names)
          .drop(columns=["disposition"])
          .pipe(strip_leading_commas)
          .pipe(extract_date_received)
          .pipe(clean_tracking_id)
          .pipe(clean_allegation)
          .pipe(clean_action)
          .pipe(clean_disposition)
          .pipe(split_rows_with_multiple_officers)
          .pipe(extract_badge_num)
          .pipe(extract_rank_desc)
          .pipe(split_name)
          .pipe(split_supervisor_name)
          .pipe(clean_dates, ["receive_date"])
          .pipe(set_values, {"agency": "caddo-so"})
          .pipe(gen_uid, ["first_name", "last_name", "agency"])
          .pipe(gen_uid, ["tracking_id", "allegation", "receive_year", "receive_month", "receive_day"], "allegation_uid")
          .pipe(gen_uid, ["supervisor_first_name", "supervisor_last_name", "agency"], "supervisor_uid")
          .drop_duplicates(subset=["allegation_uid"])
    )
    return df 

def split_case_number_and_date(df):
    split_df = df["tracking_id"].str.extract(r"(?P<tracking_id>IA[0-9\-]+)\s+(?P<receive_date>\d{1,2}/\d{1,2}/\d{4})")
    df["tracking_id"] = split_df["tracking_id"]
    df["receive_date"] = split_df["receive_date"]
    return df

def clean_disposition_25(df):
    df.loc[:, "disposition"] = (
        df.final_disposition.str.lower().str.strip()
        .str.replace(r"^result\/disposition: ?", "", regex=True)
        .str.replace(r"^closed (-|as) ?", "", regex=True)
        .str.replace(r"^(disciplinary|corrective|closed)(.+)", "sustained", regex=True)
        .str.replace(r"no action taken", "", regex=False)
        .str.replace(r"^; closed -\s*$", "closed", regex=True)
        .str.replace(r"^; disciplinary action taken\s*$", "disciplinary action taken", regex=True)
    )
    return df.drop(columns=["final_disposition"])

def extract_rank_desc_25(df):
    ranks = df.officer_name.str.extract(r"(\(\w+\))")
    df.loc[:, "rank_desc"] = (
        ranks[0]
        .fillna("")
        .str.replace(r"(\(|\))", "", regex=True)
        .str.replace(r"det", "detective", regex=False)
        .str.replace(r"(ops|adm|na)", "", regex=True)
    )
    return df



def clean_25():
    df = (
        pd.read_csv(deba.data("raw/caddo_so/cprr_caddo_so_2015_2019.csv"))
            .drop(columns=["number"])
            .pipe(clean_column_names)
            .pipe(split_case_number_and_date)
            .pipe(clean_tracking_id)
            .pipe(clean_allegation)
            .pipe(clean_action)
            .pipe(clean_disposition_25)
            .pipe(split_rows_with_multiple_officers)
            .pipe(extract_badge_num)
            .pipe(extract_rank_desc_25)
            .pipe(split_name)
            .pipe(split_supervisor_name)
            .pipe(clean_dates, ["receive_date"])
            .pipe(set_values, {"agency": "caddo-so"})
            .pipe(gen_uid, ["first_name", "last_name", "agency"])
            .pipe(gen_uid, ["tracking_id",'first_name',"receive_year", "receive_month", "receive_day", "allegation"], "allegation_uid")
            .pipe(gen_uid, ["supervisor_first_name", "supervisor_last_name", "agency"], "supervisor_uid")
            .drop_duplicates(subset=["allegation_uid"])
    )
    return df

if __name__ == "__main__":
    df = clean()
    df25 = clean_25()
    df.to_csv(deba.data("clean/cprr_caddo_so_2022_2023.csv"), index=False)
    df25.to_csv(deba.data("clean/cprr_caddo_so_2015_2019.csv"), index=False)
