import sys

sys.path.append("../")
import pandas as pd
import os
from lib.uid import gen_uid
from lib.path import data_file_path
import re

directory = os.listdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))
os.chdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))

for hearing in directory:
    if hearing.endswith(".csv"):
        df = pd.read_csv(hearing)

        for columns in df.columns:
            if "accused_name" in df.columns:
                names = df.accused_name.str.extract(r"(\w+) (\w+)")
                df.loc[:, "first_name"] = names[0]
                df.loc[:, "last_name"] = names[1]
                df = df.drop(columns=["accused_name"]).pipe(
                    gen_uid, ["first_name", "last_name", "agency"]
                )

        file_name = hearing
        df.to_csv(file_name, index=False)


# #  rename all files names to their original state,
# i.e., "extracted_Neyrey,-James-8055.pdf.txt.csv" to ""extracted_Neyrey,-James-8055.csv""
for file in directory:
    if file.endswith(".csv"):
        os.rename(file, re.sub(r".pdf.txt", "", file))
