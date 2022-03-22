import sys
sys.path.append("../")
import pandas as pd
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.clean import clean_names
import os
import deba



# match uids produced from output of Spacy model with personnel table 
def match_extracted_with_personnel(cprr, pprr):
    dfa = cprr[["uid", "first_name", "last_name"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna('').map(lambda x: x[:1])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:1])

    dfb = pprr[["uid", "first_name", "last_name"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.fillna('').map(lambda x: x[:1])
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
    cprr.loc[:, "matched_uid"] = cprr.uid.map(lambda x: match_dict.get(x))
    return cprr

directory = os.listdir(deba.data("/raw/post/spacy"))
os.chdir(deba.data("raw/post/spacy"))

per = pd.read_csv(deba.data("raw/fused/personnel.csv"))
per = per.astype(str).pipe(clean_names, ['first_name', 'last_name'])

for file in directory: 
    if file.endswith(".csv"):
        cprr = pd.read_csv(file)
        cprr = cprr.astype(str)
        if "uid" in cprr.columns:
            cprr = match_extracted_with_personnel(cprr, per)
            file_name = file
            cprr.to_csv(file_name, index=False)


if __name__ == '__main__':
    match_extracted_with_personnel()





    