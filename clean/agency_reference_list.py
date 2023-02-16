import pandas as pd
import deba

def add_morehouse_da(df):
    # dfa = pd.DataFrame(columns=["agency_name"])
    # dfa["agency_slug"] = "morehouse-da"
    # dfa["agency_name"] = 21
    # dfa["agency_name"] = "Morehouse Parish District Attorney's Office"
    # dfa["location"] = "32.77851924127957, -91.91389555397885"

    # df = pd.concat([dfa, df], axis=0)

    dfa = pd.DataFrame({"agency_slug": "morehouse-da", "agency_name": "Morehouse Parish District Attorney's Office", "location": "32.77851924127957, -91.91389555397885"}, index=[590])
    df = df.append(dfa)


    return df

def clean():
    df = pd.read_csv(deba.data("raw/agency_reference_list/agency-reference-list.csv"))\
        .pipe(add_morehouse_da)
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/agency_reference_list.csv"), index=False)
