import sys

sys.path.append("../")
import pandas as pd
import os
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.path import data_file_path
from lib.clean import clean_names
import glob


#  match appeal_uids with event_uids
def match_appeals_and_personnel(cprr, pprr):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 1

    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    # value is added to "extracted_uid" column if match score is 1
    cprr.loc[:, "extracted_uid"] = cprr.uid.map(lambda x: match_dict.get(x))
    return cprr


# personnel table is the product of a merge of both the events and personnel table
personnel = pd.read_csv(data_file_path("raw/wrgl/personnels.csv"))
personnel = personnel.astype(str).pipe(clean_names, ["first_name", "last_name"])

directory = os.listdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))
os.chdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))


for file in glob.glob("*.csv"):
    cprr = pd.read_csv(file)
    cprr = cprr.astype(str)
    if "uid" in cprr.columns:
        cprr = match_appeals_and_personnel(cprr, personnel)
        file_name = file
        cprr.to_csv(file_name, index=False)

# replace spacy generated uid with uid extracted from personnel table, if score = 1

for file in directory:
    if file.endswith(".csv"):
        cprr = pd.read_csv(file)
        cprr = cprr.astype(str)
        if "uid" in cprr.columns:
            if len(cprr["uid"]) == len(cprr["extracted_uid"]):
                cprr = cprr.drop(columns=["uid"]).rename(
                    columns={"extracted_uid": "uid"}
                )
                file_name = file
                cprr.to_csv(file_name, index=False)
