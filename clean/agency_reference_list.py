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

def add_marksville(df):
    dfa = pd.DataFrame({"agency_slug": "marksville-city-marshal", "agency_name": "Marksville City Marshal", "location": "31.127949248698542, -92.065875748721"}, index=[596])
    df = pd.concat([df, dfa])
    return df 

def add_1st_court(df):
    dfa = pd.DataFrame({"agency_slug": "1st-circuit-court-of-appeal", "agency_name": "1st Court of Appeal", "location": "30.4618125190435, -91.18597064709638"}, index=[597])
    df = pd.concat([df, dfa])
    return df 

def add_morehouse_da(df):
    dfa = pd.DataFrame({"agency_slug": "morehouse-da", "agency_name": "Morehouse District Attorney's Office", "location": "32.7786274869667, -91.91213602488001"}, index=[598])
    df = pd.concat([df, dfa])
    return df

def add_chitimacha(df):
    dfa = pd.DataFrame({"agency_slug": "chitimacha-tribal-court", "agency_name": "Chitimacha Tribal Court", "location": "29.887303795934262, -91.52659410106546"}, index=[599])
    df = pd.concat([df, dfa])
    return df 

def add_csx_railroad(df):
    dfa = pd.DataFrame({"agency_slug": "csx-railroad-pd", "agency_name": "CSX Railroad Police Department", "location": "30.46271325576703, -88.86959776044269"}, index=[600])
    df = pd.concat([df, dfa])
    return df 

def add_fort_polk(df):
    dfa = pd.DataFrame({"agency_slug": "fort-polk-military-pd", "agency_name": "Fort Polk Millitary Police Department", "location": "31.033983117259957, -93.21213886931271"}, index=[601])
    df = pd.concat([df, dfa])
    return df 

def add_hammond_marshal(df):
    dfa = pd.DataFrame({"agency_slug": "hammond-city-marshal", "agency_name": "Hammond City Marshal", "location": "30.505597126332045, -90.45688133969092"}, index=[602])
    df = pd.concat([df, dfa])
    return df 

def add_louisiana_licensing_board(df):
    dfa = pd.DataFrame({"agency_slug": "louisiana-state-licensing-board", "agency_name": "Louisiana State Licensing Board", "location": "30.452442921107746, -91.18327926671857"}, index=[603])
    df = pd.concat([df, dfa])
    return df 

def add_morgan_city_marshal(df):
    dfa = pd.DataFrame({"agency_slug": "morgan-city-6th-ward-marshals-office", "agency_name": "Morgan City 6th Ward Marshal", "location": "29.694667789582077, -91.1878035081093"}, index=[604])
    df = pd.concat([df, dfa])
    return df 

def add_newellton_pd(df):
    dfa = pd.DataFrame({"agency_slug": "newellton-pd", "agency_name": "Newellton Police Department", "location": "32.07302810479684, -91.2418079867873"}, index=[605])
    df = pd.concat([df, dfa])
    return df 

def add_st_tam_con(df):
    dfa = pd.DataFrame({"agency_slug": "st-tammany-constable-5th-ward", "agency_name": "St. Tammany 5th Ward Constable", "location": "30.401123740088057, -90.03775876894412"}, index=[606])
    df = pd.concat([df, dfa])
    return df 

def add_gov_sec(df):
    dfa = pd.DataFrame({"agency_slug": "us-government-security", "agency_name": "United States Government Security", "location": "30.467131655272322, -91.18744606839024"}, index=[607])
    df = pd.concat([df, dfa])
    return df 

def add_gov_imm(df):
    dfa = pd.DataFrame({"agency_slug": "us-immigration-naturalization-service", "agency_name": "United States Immigration and Naturalization Service", "location": "30.467131655272322, -91.18744606839024"}, index=[608])
    df = pd.concat([df, dfa])
    return df 

def add_washington_constable(df):
    dfa = pd.DataFrame({"agency_slug": "washington-constable", "agency_name": "United States Immigration and Naturalization Service", "location": "30.81930055392451, -89.8421300467456"}, index=[609])
    df = pd.concat([df, dfa])
    return df 

def fix_agency_name(df):
    df.loc[:, "agency_name"] = df.agency_name.str.replace(r"^6th Judicial Court$", "6th Judicial District Court", regex=True)
    return df 

def clean():
    df = (pd.read_csv(deba.data("raw/agency_reference_list/agency-reference-list.csv"))\
        .pipe(add_morehouse_da)
        .pipe(add_mississippi_river_bridge_pd)
        .pipe(add_13th_da)
        .pipe(add_probation_parola)
        .pipe(add_hammond_marshal)
        .pipe(add_federal_dept_of_justice)
        .pipe(add_1st_court)
        .pipe(add_marksville)
        .pipe(add_morehouse_da)
        .pipe(add_chitimacha)
        .pipe(add_csx_railroad)
        .pipe(add_fort_polk)
        .pipe(add_gov_imm)
        .pipe(add_gov_sec)
        .pipe(add_newellton_pd)
        .pipe(add_st_tam_con)
        .pipe(add_hammond_marshal)
        .pipe(add_louisiana_licensing_board)
        .pipe(add_morgan_city_marshal)
        .pipe(add_washington_constable)
        .pipe(fix_agency_name)
    )
    return df

if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/agency_reference_list.csv"), index=False)
