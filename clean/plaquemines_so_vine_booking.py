import requests
from bs4 import BeautifulSoup
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import standardize_desc_cols


def scrape(url):
    try:
        inmates = []
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        for table_row in soup.select("tr"):
            cells = table_row.findAll('td', {'align': 'left', 'valign': 'middle'})
            if len(cells) > 0:
                name = cells[0].text
                dob = cells[1].text
                race = cells[2].text
                sex = cells[3].text
                arrest_date = cells[4].text
                inmate = {'name': name, 'birth_date': dob, 'race': race, 'sex': sex, 'arrest_date': arrest_date}
                inmates.append(inmate)
    except Exception as e:
        return e

    df = pd.DataFrame.from_records(inmates)\
        .pipe(clean_column_names)\
        .pipe(standardize_desc_cols, ['name', 'race', 'sex', 'arrest_date'])
    return df


if __name__ == '__main__':
    url = 'http://www.plaquemines.lavns.org/roster.aspx'
    df = scrape(url)
