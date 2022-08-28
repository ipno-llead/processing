from posixpath import split
import deba
import pandas as pd
from lib.clean import float_to_int_str, names_to_title_case, standardize_desc_cols, clean_dates
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid

def drop_rows_missing_names(df):
    df.loc[:, "accused_name"] = df.accused_name.str.lower().str.strip()\
        .str.replace(r"appellant\,? ?", "", regex=True)
    return df[~(df.accused_name.fillna("") == "")]

def extract_doc_num(df):
    docs = df.docket_number.str.lower().str.strip()\
        .str.extract(r"(\w{4})$")
    
    df.loc[:, "docket_no"] = docs[0]
    return df.drop(columns=["docket_number"])[~(df.docket_no.fillna("") == "")]
    

def split_name(df):
    names = df.accused_name.str.extract(r"(\w+) (\w+)")

    df.loc[:, "first_name"] = names[0]
    df.loc[:, "last_name"] = names[1]
    return df.drop(columns=["accused_name"])
    
def clean():
    df = pd.read_csv(deba.data("ner/nopd_appeals_pdfs.csv"))\
        .pipe(clean_column_names)\
        .drop(columns=['docket_number_1',
       'docket_number_2', 'docket_number_3', 'accused_name_1',  'accused_name_2',
       'docket_number_4', 'accused_name_3', 'appeal_hearing_date_1', 'appeal_hearing_date_2', 
       "appeal_hearing_date", 'appeal_hearing_disposition_1', 'appeal_hearing_disposition',
       'appeal_hearing_disposition_2'
       ])\
        .pipe(drop_rows_missing_names)\
        .pipe(extract_doc_num)\
        .pipe(split_name)\
        .pipe(set_values, {"agency": "New Orleans PD"})\
        .pipe(clean_dates, ["decision_notification_date"])\
        .pipe(gen_uid, ["first_name","last_name", "agency"])
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/app_new_orleans_pd_transcripts.csv"), index=False)

