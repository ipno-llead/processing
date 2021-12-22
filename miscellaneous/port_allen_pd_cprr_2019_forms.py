# noinspection PyUnresolvedReferences
import pandas as pd

# noinspection PyUnresolvedReferences
import re

# noinspection PyUnresolvedReferences
import numpy as np

f = open("Port_Allen_PD_CPRR_Forms_2019.txt", "r")
lines = f.readlines()

spec_chars = [
    r"Date/Time of Incident: ",
    r"hours",
    r"Case Number",
    r"hrs",
    "!",
    '"',
    "%",
    "&",
    "'",
    "(",
    ")",
    "*",
    "+",
    ",",
    "-",
    ".",
    ":",
    ";",
    "<",
    "=",
    ">",
    "?",
    "[",
    "\\",
    "]",
    "^",
    "_",
    "`",
    "{",
    "|",
    "}",
    "~",
    "–",
    "\n",
    r"Date",
    r"Filed",
]

datefiled = []
for line in lines:
    if re.search("(Date Filed)", line):
        line.strip()
        datefiled.append(line)
df1 = pd.DataFrame(np.array(datefiled))
df1.columns = ["date_filed"]
for char in spec_chars:
    df1["date_filed"] = (
        df1["date_filed"].str.replace(char, " ").str.split().str.join(" ")
    )
df1["date_filed"] = pd.to_datetime(df1["date_filed"])

officer = []
for line in lines:
    if re.match("(1.)", line):
        line.strip()
        officer.append(line)
df2 = pd.DataFrame(np.array(officer))
df2.columns = ["officer"]
df2["officer"] = df2["officer"].map(lambda x: x.lstrip("1.").rstrip("\n"))
df2[["title", "first_name", "last_name"]] = df2["officer"].str.split(expand=True)
del df2["officer"]

badge = []
for line in lines:
    if re.match("(PA-)", line):
        line.strip()
        badge.append(line)
df3 = pd.DataFrame(np.array(badge))
df3.columns = ["badge"]

order = []
for line in lines:
    if re.search("(Order)", line):
        line.strip()
        order.append(line)
df4 = pd.DataFrame(np.array(order))
df4.columns = ["charge"]
df4["charge"] = df4["charge"].map(lambda x: x.rstrip("\n")).astype(str)

disposition = []
for line in lines:
    if re.match("(5.)", line):
        line.strip()
        disposition.append(line)
df5 = pd.DataFrame(np.array(disposition))
df5.columns = ["disposition"]
df5 = df5[df5.disposition != "5.\n"].reset_index(drop=True)
df5["disposition"] = (
    df5["disposition"]
    .map(lambda x: x.lstrip("5.").rstrip("\n"))
    .str.split()
    .str.join(" ")
    .str.capitalize()
)

dateofincident = []
for line in lines:
    if re.search("(Date/Time of Incident)", line):
        line.strip()
        dateofincident.append(line)
df6 = pd.DataFrame(np.array(dateofincident))
df6.columns = ["occur_date"]
for char in spec_chars:
    df6["occur_date"] = (
        df6["occur_date"].str.replace(char, " ").str.split().str.join(" ")
    )
df6 = pd.DataFrame(
    df6["occur_date"].str.strip().str.split("@", 1).tolist(),
    columns=["occur_date", "occur_time"],
)
df6["occur_hour"] = df6["occur_time"]
df6["occur_hour"] = df6["occur_hour"].str.strip().str.replace(" ", "-")
del df6["occur_time"]

case = []
for line in lines:
    if re.search("(Case Number)", line):
        line.strip()
        case.append(line)
df7 = pd.DataFrame(np.array(case))
df7.columns = ["case_number"]
for char in spec_chars:
    df7["case_number"] = (
        df7["case_number"].str.replace(char, " ").str.split().str.join(" ")
    )
df7["tracking_number"] = df7["case_number"]
df7["tracking_number"] = df7["tracking_number"].str.replace(" ", "-")
del df7["case_number"]


df = pd.concat([df1, df2, df3, df4, df5, df6, df7], axis=1)

df.to_csv(r"port_allen_pd_cprr_2019_forms.csv", index=None)
