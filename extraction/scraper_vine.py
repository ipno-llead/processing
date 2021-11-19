import sys
sys.path.append('../')
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import standardize_desc_cols, strip_birth_date
from lib.uid import gen_uid
from lib.path import data_file_path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def scrape(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--log-level=3')
    browser = webdriver.Chrome(chrome_options=chrome_options)
    browser.get(url)
    login_button = WebDriverWait(browser, 2).until(EC.presence_of_element_located((By.ID, 'lbShowAll')))
    login_button.click()

    try:
        inmates = []
        soup = BeautifulSoup(browser.page_source, 'lxml')
        for table_row in soup.select('tr'):
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

    df = pd.DataFrame.from_dict(inmates)\
        .pipe(clean_column_names)\
        .pipe(standardize_desc_cols, ['birth_date', 'race', 'sex', 'arrest_date'])\
        .pipe(gen_uid, ['name', 'birth_date', 'race', 'sex'], 'citizen_uid')\
        .pipe(strip_birth_date, ['birth_date'])\
        .rename(columns={
            'birth_date': 'birth_year'
        })\
        .drop(columns='name')
    return df


if __name__ == '__main__':

    #  sheriff's offices:
    arcadia = 'http://www.acadia.lavns.org/roster.aspx'
    arcadia = scrape(arcadia).to_csv(data_file_path('scraped/vine/arcadia_so_booking.csv'), index=False)
    iberia = 'http://www.iberia.lavns.org/roster.aspx'
    iberia = scrape(iberia).to_csv(data_file_path('scraped/vine/iberia_so_booking.csv'), index=False)
    jeffersondavis = 'http://www.jeffersondavis.lavns.org/roster.aspx'
    jeffersondavis = scrape(jeffersondavis).to_csv(data_file_path('scraped/vine/jeffersondavis_so_booking.csv'), index=False)
    plaquemines = 'http://www.plaquemines.lavns.org/roster.aspx'
    plaquemines = scrape(plaquemines).to_csv(data_file_path('scraped/vine/plaqumines_so_booking.csv'), index=False)
    tangipahoa = 'http://www.tangipahoa.lavns.org/roster.aspx'
    tangipahoa = scrape(tangipahoa).to_csv(data_file_path('scraped/vine/tangipahoa_so_booking.csv'), index=False)
    ascension = 'http://www.ascension.lavns.org/roster.aspx'
    ascension = scrape(ascension).to_csv(data_file_path('scraped/vine/ascension_so_booking.csv'), index=False)
    allen = 'http://www.allen.lavns.org/roster.aspx'
    allen = scrape(allen).to_csv(data_file_path('scraped/vine/allen_so_booking.csv'), index=False)
    assumption = 'http://www.assumption.lavns.org/roster.aspx'
    assumption = scrape(assumption).to_csv(data_file_path('scraped/vine/assumption_so_booking.csv'), index=False)
    avoyelles = 'http://www.avoyelles.lavns.org/roster.aspx'
    avoyelles = scrape(avoyelles).to_csv(data_file_path('scraped/vine/avoyelles_so_booking.csv'), index=False)
    beauregard = 'http://www.beauregard.lavns.org/roster.aspx'
    beauregard = scrape(beauregard).to_csv(data_file_path('scraped/vine/beauregard_so_booking.csv'), index=False)
    bossier = 'http://www.bossier.lavns.org/roster.aspx'
    bossier = scrape(bossier).to_csv(data_file_path('scraped/vine/bossier_so_booking.csv'), index=False)
    bienville = 'http://www.bienville.lavns.org/roster.aspx'
    bienville = scrape(bienville).to_csv(data_file_path('scraped/vine/bienville_so_booking.csv'), index=False)
    calcasieu = 'http://www.calcasieu.lavns.org/roster.aspx'
    calcasieu = scrape(calcasieu).to_csv(data_file_path('scraped/vine/calcasieu_so_booking.csv'), index=False)
    caddo = 'http://www.caddo.lavns.org/roster.aspx'
    caddo = scrape(caddo).to_csv(data_file_path('scraped/vine/caddo_so_booking.csv'), index=False)
    caldwell = 'http://www.caldwell.lavns.org/roster.aspx'
    caldwell = scrape(caldwell).to_csv(data_file_path('scraped/vine/caldwell_so_booking.csv'), index=False)
    cameron = 'http://www.cameron.lavns.org/roster.aspx'
    cameron = scrape(cameron).to_csv(data_file_path('scraped/vine/cameron_so_booking.csv'), index=False)
    catahoula = 'http://www.catahoula.lavns.org/roster.aspx'
    catahoula = scrape(catahoula).to_csv(data_file_path('scraped/vine/catahoula_so_booking.csv'), index=False)
    claiborne = 'http://www.claiborne.lavns.org/roster.aspx'
    claiborne = scrape(claiborne).to_csv(data_file_path('scraped/vine/claiborne_so_booking.csv'), index=False)
    concordia = 'http://www.concordia.lavns.org/roster.aspx'
    concordia = scrape(concordia).to_csv(data_file_path('scraped/vine/concordia_so_booking.csv'), index=False)
    desoto = 'http://www.desoto.lavns.org/roster.aspx'
    desoto = scrape(desoto).to_csv(data_file_path('scraped/vine/desoto_so_booking.csv'), index=False)
    ebatonrouge = 'http://www.eastbatonrouge.lavns.org/roster.aspx'
    ebatonrouge = scrape(ebatonrouge).to_csv(data_file_path('scraped/vine/eastbatonrouge_so_booking.csv'), index=False)
    efeliciana = 'http://www.eastfeliciana.lavns.org/roster.aspx'
    efeliciana = scrape(efeliciana).to_csv(data_file_path('scraped/vine/eastfeliciana_so_booking.csv'), index=False)
    franklin = 'http://www.franklin.lavns.org/roster.aspx'
    franklin = scrape(franklin).to_csv(data_file_path('scraped/vine/franklin_so_booking.csv'), index=False)
    iberville = 'http://www.iberville.lavns.org/roster.aspx'
    iberville = scrape(iberville).to_csv(data_file_path('scraped/vine/iberville_so_booking.csv'), index=False)
    jefferson = 'http://www.jefferson.lavns.org/roster.aspx'
    jefferson = scrape(jefferson).to_csv(data_file_path('scraped/vine/jefferson_so_booking.csv'), index=False)
    lasalle = 'http://www.lasalle.lavns.org/roster.aspx'
    lasalle = scrape(lasalle).to_csv(data_file_path('scraped/vine/lasalle_so_booking.csv'), index=False)
    lafayette = 'http://www.lafayette.lavns.org/roster.aspx'
    lafayette = scrape(lafayette).to_csv(data_file_path('scraped/vine/lafayette_so_booking.csv'), index=False)
    madison = 'http://www.madison.lavns.org/roster.aspx'
    madison = scrape(madison).to_csv(data_file_path('scraped/vine/madison_so_booking.csv'), index=False)
    morehouse = 'http://www.morehouse.lavns.org/roster.aspx'
    morehouse = scrape(morehouse).to_csv(data_file_path('scraped/vine/morehouse_so_booking.csv'), index=False)
    natchitoches = 'http://www.natchitoches.lavns.org/roster.aspx'
    natchitoches = scrape(natchitoches).to_csv(data_file_path('scraped/vine/natchitoches_so_booking.csv'), index=False)
    orleans = 'http://www.orleans.lavns.org/roster.aspx'
    orleans = scrape(orleans).to_csv(data_file_path('scraped/vine/orleans_so_booking.csv'), index=False)
    ouachita = 'http://www.ouachita.lavns.org/roster.aspx'
    ouachita = scrape(ouachita).to_csv(data_file_path('scraped/vine/ouachita_so_booking.csv'), index=False)
    pointecoupee = 'http://www.pointecoupee.lavns.org/roster.aspx'
    pointecoupee = scrape(pointecoupee).to_csv(data_file_path('scraped/vine/pointecoupee_so_booking.csv'), index=False)
    rapides = 'http://www.rapides.lavns.org/roster.aspx'
    rapides = scrape(rapides).to_csv(data_file_path('scraped/vine/rapides_so_booking.csv'), index=False)
    redriver = 'http://www.redriver.lavns.org/roster.aspx'
    redriver = scrape(redriver).to_csv(data_file_path('scraped/vine/redriver_so_booking.csv'), index=False)
    richland = 'http://www.richland.lavns.org/roster.aspx'
    richland = scrape(richland).to_csv(data_file_path('scraped/vine/richland_so_booking.csv'), index=False)
    sabine = 'http://www.sabine.lavns.org/roster.aspx'
    sabine = scrape(sabine).to_csv(data_file_path('scraped/vine/sabine_so_booking.csv'), index=False)
    stbernard = 'http://www.stbernard.lavns.org/roster.aspx'
    stbernard = scrape(stbernard).to_csv(data_file_path('scraped/vine/stbernard_so_booking.csv'), index=False)
    stcharles  = 'http://www.stcharles.lavns.org/roster.aspx'
    stcharles = scrape(stcharles).to_csv(data_file_path('scraped/vine/stcharles_so_booking.csv'), index=False)
    sthelena = 'http://www.sthelena.lavns.org/roster.aspx'
    sthelena = scrape(sthelena).to_csv(data_file_path('scraped/vine/sthelena_so_booking.csv'), index=False)
    stjames = 'http://www.stjames.lavns.org/roster.aspx'
    stjames = scrape(stjames).to_csv(data_file_path('scraped/vine/stjames_so_booking.csv'), index=False)
    stjohn = 'http://www.stjohn.lavns.org/roster.aspx'
    stjohn = scrape(stjohn).to_csv(data_file_path('scraped/vine/stjohn_so_booking.csv'), index=False)
    stlandry = 'http://www.stlandry.lavns.org/roster.aspx'
    stlandry = scrape(stlandry).to_csv(data_file_path('scraped/vine/stlandry_so_booking.csv'), index=False)
    stmartin = 'http://www.stmartin.lavns.org/roster.aspx'
    stmartin = scrape(stmartin).to_csv(data_file_path('scraped/vine/stmartin_so_booking.csv'), index=False)
    stmary = 'http://www.stmary.lavns.org/roster.aspx'
    stmary = scrape(stmary).to_csv(data_file_path('scraped/vine/stmary_so_booking.csv'), index=False)
    sttammany = 'http://www.sttammany.lavns.org/roster.aspx'
    sttammany = scrape(sttammany).to_csv(data_file_path('scraped/vine/sttammany_so_booking.csv'), index=False)
    tensas = 'http://www.tensas.lavns.org/roster.aspx'
    tensas = scrape(tensas).to_csv(data_file_path('scraped/vine/tensas_so_booking.csv'), index=False)
    vernon = 'http://www.vernon.lavns.org/roster.aspx'
    vernon = scrape(vernon).to_csv(data_file_path('scraped/vine/vernon_so_booking.csv'), index=False)
    terrebonne = 'http://www.terrebonne.lavns.org/roster.aspx'
    terrebonne = scrape(terrebonne).to_csv(data_file_path('scraped/vine/terrebonne_so_booking.csv'), index=False)
    washington = 'http://www.washington.lavns.org/roster.aspx'
    washington = scrape(washington).to_csv(data_file_path('scraped/vine/washington_so_booking.csv'), index=False)
    webster = 'http://www.webster.lavns.org/roster.aspx'
    webster = scrape(webster).to_csv(data_file_path('scraped/vine/webster_so_booking.csv'), index=False)
    wbatonrouge = 'http://www.westbatonrouge.lavns.org/roster.aspx'
    wbatonrouge = scrape(wbatonrouge).to_csv(data_file_path('scraped/vine/wbatonrouge_so_booking.csv'), index=False)
    westcarroll = 'http://www.westcarroll.lavns.org/roster.aspx'
    westcarroll = scrape(westcarroll).to_csv(data_file_path('scraped/vine/westcarroll_so_booking.csv'), index=False)
    wfeliciana = 'http://www.westfeliciana.lavns.org/roster.aspx'
    wfeliciana = scrape(wfeliciana).to_csv(data_file_path('scraped/vine/westfeliciana_so_booking.csv'), index=False)
    winn = 'http://www.winn.lavns.org/roster.aspx'
    winn = scrape(winn).to_csv(data_file_path('scraped/vine/winn_pd_booking.csv'), index=False)

    #  police departments:
    bogalusa = 'http://www.bogalusa.lavns.org/roster.aspx'
    bogalusa = scrape(bogalusa).to_csv(data_file_path('scraped/vine/bogalusa_pd_booking.csv'), index=False)
    bossiercity = 'http://www.bossierpd.lavns.org/roster.aspx'
    bossiercity = scrape(bossiercity).to_csv(data_file_path('scraped/vine/bossiercity_pd_booking.csv'), index=False)
    hammond = 'http://www.hammond.lavns.org/roster.aspx'
    hammond = scrape(hammond).to_csv(data_file_path('scraped/vine/hammond_pd_booking.csv'), index=False)
    kinder = 'http://www.kinder.lavns.org/roster.aspx'
    kinder = scrape(kinder).to_csv(data_file_path('scraped/vine/kinder_pd_booking.csv'), index=False)
    leesville = 'http://www.leesville.lavns.org/roster.aspx'
    leesville = scrape(leesville).to_csv(data_file_path('scraped/vine/leesville_pd_booking.csv'), index=False)
    oakdale = 'http://www.oakdale.lavns.org/roster.aspx'
    oakdale = scrape(oakdale).to_csv(data_file_path('scraped/vine/oakdale_pd_booking.csv'), index=False)
    shreveport = 'http://www.shreveport.lavns.org/roster.aspx'
    shreveport = scrape(shreveport).to_csv(data_file_path('scraped/vine/shreveport_pd_booking.csv'), index=False)
    sulphur = 'http://www.sulphur.lavns.org/roster.aspx'
    sulphur = scrape(sulphur).to_csv(data_file_path('scraped/vine/sulphur_pd_booking.csv'), index=False)
    villeplatte = 'http://www.villeplatte.lavns.org/roster.aspx'
    villeplatte = scrape(villeplatte).to_csv(data_file_path('scraped/vine/villeplatte_pd_booking.csv'), index=False)
    winnfield = 'http://www.winnfield.lavns.org/roster.aspx'
    winnfield = scrape(winnfield).to_csv(data_file_path('scraped/vine/winnfield_pd_booking.csv'), index=False)
