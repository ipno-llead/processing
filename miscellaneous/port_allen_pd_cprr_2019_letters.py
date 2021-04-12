import pandas as pd
# noinspection PyUnresolvedReferences
import re
# noinspection PyUnresolvedReferences
import numpy as np

f = open('port_allen_pd_cprr_letters_2019.txt', 'r')
lines = f.readlines()

spec_chars = [r'Date/Time of Incident: ', r'hours', r'hrs', "!",'"',"%","&","'","(",")",
              "*","+",",","-",".","/",":",";","<",
              "=",">","?","[","\\","]","^","_",
              "`","{","|","}","~","–", '\n', r'Date', r'Filed', "To", ":"]

officer = []
for line in lines:
    if re.search("(To:)", line):
        line.strip()
        officer.append(line)
df1 = pd.DataFrame(np.array(officer))
df1.columns = ['Officer']
for char in spec_chars:
    df1['Officer' ] = df1['Officer'].str.replace(char, ' ').str.split().str.join(" ")

date = []
for line in lines:
    if re.match("(Date)", line):
        line.strip()
        date.append(line)
df2 = pd.DataFrame(np.array(date))
df2.columns = ['Date of Notification']
for char in spec_chars:
    df2['Date of Notification' ] = df2['Date of Notification'].str.replace(char, ' ').str.split().str.join(" ")

df2['Date of Notification'] = pd.to_datetime(df2['Date of Notification'])

case = []
for line in lines:
    if re.match("(I.A.)", line):
        line.strip()
        case.append(line)
df3 = pd.DataFrame(np.array(case))
df3.columns = ['Case #']


charge = []
for line in lines:
    if re.search("(Order)", line):
        line.strip()
        charge.append(line)
df4 = pd.DataFrame(np.array(charge))
df4.columns = ['Charge']


df = pd.concat([df1, df2, df3, df4], axis=1)


df.to_csv (r'port_allen_pd_cprr_2019_letters.csv', index=None)
