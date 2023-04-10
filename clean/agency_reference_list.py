import pandas as pd
import deba

def add_morehouse_da(df):
    dfa = pd.DataFrame({"agency_slug": "morehouse-da", "agency_name": "Morehouse Parish District Attorney's Office", "location": "32.77851924127957, -91.91389555397885"}, index=[590])
    df = pd.concat([df, dfa])
    return df


def add_mississippi_river_bridge_pd(df):
    dfa = pd.DataFrame({"agency_slug": "mississippi-river-bridge-pd", "agency_name": "Mississippi River Bridge Police Department", "location": "29.934485523906382, -90.03563957064394"}, index=[591])
    df = pd.concat([df, dfa])
    return df 


def add_13th_da(df):
    dfa = pd.DataFrame({"agency_slug": "13th-da", "agency_name": "13th District Attorney's Office", "location": "30.68895087250337, -92.27833746251149"}, index=[592])
    df = pd.concat([df, dfa])
    return df 


def add_probation_parola(df):
    dfa = pd.DataFrame({"agency_slug": "probation-parole", "agency_name": "Probation and Parole", "location": "30.9842977, -91.9623327"}, index=[593])
    df = pd.concat([df, dfa])
    return df 


def add_hammond_marshal(df):
    dfa = pd.DataFrame({"agency_slug": "hammond-city-marshal", "agency_name": "Hammond City Marshal", "location": "30.50567007627712, -90.45503598015175"}, index=[594])
    df = pd.concat([df, dfa])
    return df 

def add_federal_dept_of_justice(df):
    dfa = pd.DataFrame({"agency_slug": "federal-department-of-justice", "agency_name": "Federal Department of Justice", "location": "32.51672720900096, -93.74585120801952"}, index=[595])
    df = pd.concat([df, dfa])
    return df 


def clean():
    df = (pd.read_csv(deba.data("raw/agency_reference_list/agency-reference-list.csv"))\
        .pipe(add_morehouse_da)
        .pipe(add_mississippi_river_bridge_pd)
        .pipe(add_13th_da)
        .pipe(add_probation_parola)
        .pipe(add_hammond_marshal)
        .pipe(add_federal_dept_of_justice)
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/agency_reference_list.csv"), index=False)
