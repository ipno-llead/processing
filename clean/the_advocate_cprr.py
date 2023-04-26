import pandas as pd
from lib.columns import clean_column_names
from lib.clean import standardize_desc_cols
from lib.uid import gen_uid
import deba


def extract_names_and_source(df):
    data = (df.name
            .str.lower()
            .str.strip()
            .str.extract(r"^\[?(\w{2}?\.? ?\w+\,? ?\w{2}?\.?\,?) (\w+) ?(\w+\.?)?\]?\(?(.+)?$")
    )

    df.loc[:, "last_name"] = data[0].str.replace(r"(\.|\,)", "", regex=True)
    df.loc[:, "first_name"] = data[1]
    df.loc[:, "middle_name"] = data[2].fillna("").str.replace(r"\.", "", regex=True)
    df.loc[:, "source"] = data[3].fillna("").str.replace(r"\)", "", regex=True)
    return df


def extract_parish_and_agency(df):
    data = df.agency.str.extract(r"^(.+:)? ?(.+)")
    df.loc[:, "parish"] = data[0].str.replace(r":", "", regex=False)
    df.loc[:, "agency"] = data[1]

    df.loc[:, "agency"] = (df.agency
                           .str.lower()
                           .str.strip()
                           .str.replace(r" parish", "", regex=False)
                           .str.replace(r"\.", "", regex=True)
                           .str.replace(r"(.+) police department", r"\1-pd", regex=True)
                           .str.replace(r"(.+) sheriff\'s office", r"\1-so", regex=True)
                           .str.replace(r"\s+", "-", regex=True)
                           .str.replace(r"jackson-town-marshal", "jackson-marshals-office", regex=False)
                           .str.replace(r"police$", "pd", regex=True)
                           .str.replace(r"^orleans-so$", "new-orleans-so", regex=True)
                           .str.replace(r"^louisiana-state-university-pd$", "lsu-university-pd", regex=True)
                           .str.replace(r"marksville-marshals", "marksville-city-marshal", regex=True)
                           .str.replace(r"^not-available$", "", regex=True)
    )
    return df 


def generate_disposition_col(df):
    df["disposition"] = "convicted"
    df.loc[:, "investigation_complete_date"] = df.conviction_date
    return df 


def clean_conviction_dates(df):
    df.loc[:, "conviction_date"] = (df.conviction_year
                                    .str.replace(r"Not available", "", regex=True)
                                    .str.replace(r"^(\w{4})$", r"12/31/\1", regex=True)
    )         
    return df.drop(columns=["conviction_year"])


def assign_names(df):
    df.loc[df.name == "[Wald, Kevin](https://www.theadvocate.com/baton_rouge/news/article_789c9ab0-bdb8-11e6-b27f-bf5dc9d868ff.html)", "first_name"] = "kevin"
    df.loc[df.name == "[Wald, Kevin](https://www.theadvocate.com/baton_rouge/news/article_789c9ab0-bdb8-11e6-b27f-bf5dc9d868ff.html)", "last_name"] = "wald"
    
    df.loc[df.name == "[Hill, Dennis](https://www.theadvocate.com/baton_rouge/news/article_1f96e08c-fe58-11e8-aa56-b39b05a03569.html)", "first_name"] = "dennis"
    df.loc[df.name == "[Hill, Dennis](https://www.theadvocate.com/baton_rouge/news/article_1f96e08c-fe58-11e8-aa56-b39b05a03569.html)", "last_name"] = "hill"

    df.loc[df.name == "[Pope, Brian](https://www.theadvocate.com/acadiana/news/article_43f6d472-cd50-11eb-85a3-0b5678e393bd.html)", "first_name"] = "brian"
    df.loc[df.name == "[Pope, Brian](https://www.theadvocate.com/acadiana/news/article_43f6d472-cd50-11eb-85a3-0b5678e393bd.html)", "last_name"] = "pope"

    df.loc[df.name == "Iles, Raymond Jamal", "first_name"] = "raymond"
    df.loc[df.name == "Iles, Raymond Jamal", "middle_name"] = "jamal"
    df.loc[df.name == "Iles, Raymond Jamal", "last_name"] = "iles"

    df.loc[df.name == "[Hart, Tianna](https://www.justice.gov/usao-edla/pr/former-deputy-sheriff-pleads-guilty-making-false-statement)", "first_name"] = "tianna"
    df.loc[df.name == "[Hart, Tianna](https://www.justice.gov/usao-edla/pr/former-deputy-sheriff-pleads-guilty-making-false-statement)", "last_name"] = "hart"

    df.loc[df.name == "[Core, Brad](https://www.wafb.com/2021/06/23/former-hammond-police-officer-pleads-guilty-child-sex-crimes/)", "first_name"] = "brad"
    df.loc[df.name == "[Core, Brad](https://www.wafb.com/2021/06/23/former-hammond-police-officer-pleads-guilty-child-sex-crimes/)", "last_name"] = "core"

    df.loc[df.name == "Boyd, David Tyler", "first_name"] = "david"
    df.loc[df.name == "Boyd, David Tyler", "middle_name"] = "tyler"
    df.loc[df.name == "Boyd, David Tyler", "last_name"] = "boyd"

    df.loc[df.name == "[Bahm, Brett](https://www.documentcloud.org/documents/23783741-st-tammany-washington-parish-police-convictions?responsive=1&title=1)", "first_name"] = "brett"
    df.loc[df.name == "[Bahm, Brett](https://www.documentcloud.org/documents/23783741-st-tammany-washington-parish-police-convictions?responsive=1&title=1)", "last_name"] = "bahm"
    
    df.loc[df.name == "[Gary, Elijah](https://www.documentcloud.org/documents/23783718-plaquemines-parish-police-convictions?responsive=1&title=1)", "first_name"] = "gary"
    df.loc[df.name == "[Gary, Elijah](https://www.documentcloud.org/documents/23783718-plaquemines-parish-police-convictions?responsive=1&title=1)", "last_name"] = "elijah"

    df = df[~((df.agency == "marksville-city-marshal"))]
    return df.drop(columns=["name"])[~((df.agency == ""))]


def clean():
    df = (pd.read_csv(deba.data("raw/the_advocate/criminal_convictions_data_joseph_cranney_the_advocate.csv"))
          .pipe(clean_column_names)
          .rename(columns={"case_type": "allegation"})
          .pipe(standardize_desc_cols, ["allegation", "felony"])
          .pipe(extract_names_and_source)
          .pipe(extract_parish_and_agency)
          .pipe(clean_conviction_dates)
          .pipe(generate_disposition_col)
          .pipe(assign_names)
          .pipe(gen_uid, ["first_name", "last_name", "agency"])
          .pipe(gen_uid, ["allegation", "uid", "felony", "source"], "allegation_uid")
          )
    return df 


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/the_advocate_convictions.csv"), index=False)