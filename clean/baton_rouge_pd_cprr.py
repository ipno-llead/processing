from lib.columns import clean_column_names
from lib.clean import clean_names, parse_dates, clean_salaries, standardize_desc_cols
from lib.path import data_file_path, ensure_data_dir
import pandas as pd
import re
import sys
sys.path.append("../")


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

    names = dep["name"].str.replace(
        r"^(\w+(?: (?:Iii?|Iv|V|Jr|Sr))?) (\w+)(?: (\w+|N\/A))?$", r"\1 # \2 # \3").str.split(" # ", expand=True)
    names.columns = ["last_name", "first_name", "middle_initial"]
    names.loc[:, "middle_initial"] = names["middle_initial"].str.replace(
        "N/A", "", regex=False).fillna("")
    names.loc[:, "last_name"] = names["last_name"]\
        .str.replace(r"^(.+) (Iii?|Iv)$", lambda m: "%s %s" % (m.group(1), m.group(2).upper())).fillna("")
    names.loc[:, "first_name"] = names["first_name"].fillna("")

    df = pd.concat([df, dep, names], axis=1)
    df.drop(columns=["officer_name", "name"], inplace=True)
    return df


def assign_tracking_num_18(df):
    df.loc[:, "tracking_number"] = df.ia_year + "-" + df["1a_seq"]
    df = df.drop(columns=["1a_seq", "ia_year"])
    return df


def clean_18():
    df = realign_18()
    df = df\
        .pipe(clean_column_names)\
        .pipe(parse_officer_name_18)\
        .pipe(standardize_desc_cols, ["department_desc"])\
        .pipe(parse_complaint_18)\
        .pipe(assign_tracking_num_18)
    df = df.rename(columns={
        "status": "investigation_status",
        "received": "receive_date",
    })
    return df
