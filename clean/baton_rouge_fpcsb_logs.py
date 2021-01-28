from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import io
import re
import sys
sys.path.append("../")


def realign():
    with open(data_file_path("baton_rouge_fpcsb/baton_rouge_fpcsb_logs_1992-2012.csv"), "r", encoding="latin-1") as f:
        # remove extraneous column names
        s = re.sub(
            r"(Date of Hearing|Hearing (Scheduled|Date)).?,(Docket|Case) #.+", "", f.read())
        s = re.sub(r",\s+$", ",", s)
        # add original column names
        s = "hearing_date,docket_no,name,disciplinary_action,resolution_reached,spill\n" + s
        return pd.read_csv(io.StringIO(s))


def clean():
    df = realign()
    return df
