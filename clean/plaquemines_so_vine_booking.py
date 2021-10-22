import requests
from bs4 import BeautifulSoup
import pandas as pd
from lib.columns import clean_column_names
from lib.path import data_file_path


url = 'http://www.plaquemines.lavns.org/roster.aspx'
r = requests.get(url)
soup = BeautifulSoup(r.text, 'html.parser')

inmates_dict = []
for table_row in soup.select("tr"):
    cells = table_row.findAll('td', {'align': 'left', 'valign': 'middle'})
    if len(cells) > 0:
        name = cells[0].text
        dob = cells[1].text
        race = cells[2].text
        gender = cells[3].text
        arrest_date = cells[4].text
        inmate = {'name': name, 'birth_date': dob, 'race': race, 'gender': gender, 'arrest_date': arrest_date}
        inmates_dict.append(inmate)


def clean():
    df = pd.DataFrame.from_dict(inmates_dict)\
        .pipe(clean_column_names)
    return df


if __name__ == '__main__':
    df = clean()
    df.to_csv(data_file_path('clean/plaquemines_so_booking_log_10_21_2021.csv'), index=False)
