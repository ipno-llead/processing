import pandas as pd
import deba

def add_morehouse_da(df):
    dfa = pd.DataFrame({"agency_slug": "morehouse-da", "agency_name": "Morehouse Parish District Attorney's Office", "location": "32.77851924127957, -91.91389555397885"}, index=[590])
    df = df.append(dfa)
    return df


def add_mississippi_river_bridge_pd(df):
    dfa = pd.DataFrame({"agency_slug": "mississippi-river-bridge-pd", "agency_name": "Mississippi River Bridge Police Department", "location": "29.934485523906382, -90.03563957064394"}, index=[591])
    df = df.append(dfa)
    return df 


def clean():
    df = pd.read_csv(deba.data("raw/agency_reference_list/agency-reference-list.csv"))\
        .pipe(add_morehouse_da)\
        .pipe(add_mississippi_river_bridge_pd)
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/agency_reference_list.csv"), index=False)
