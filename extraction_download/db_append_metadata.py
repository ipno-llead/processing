import pandas as pd
import sys
sys.path.append('../')
import os
import deba

directory = os.listdir(deba.data("raw/post/spacy"))
os.chdir(deba.data("raw/post/spacy"))

meta = pd.read_csv(deba.data("raw/post/metadata/clean_post_metadata.csv"))

for file in directory: 
    if file.endswith(".csv"):
        cprr = pd.read_csv(file)
        cprr = cprr.astype(str)
        if "uid" in cprr.columns:
            cprr = pd.merge(cprr, meta, on='file_name', how='inner')
            file_name = file
            cprr.to_csv(file_name, index=False)
