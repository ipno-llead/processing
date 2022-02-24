import pandas as pd

from lib.columns import clean_column_names, set_values
import deba
from lib.clean import clean_names
from lib.uid import gen_uid


def stack_disposition_rows(df):
    disp_pattern = r"(%s)" % "|".join(
        [
            "NOT SUSTAINED",
            "SUSTAINED",
            "UNFOUNDED",
            "EXONERATED",
            "NO FURTHER",
            "EXCEPTIONAL",
            "POLICY FAILURE",
            "FALSE COMPLETION",
            "NO POLICY VIOLATION",
        ]
    )

    # combine complaint type and disposition columns
    combined = (
        df.iloc[:, 2]
        .fillna("")
        .str.strip()
        .str.cat(
            [s.fillna("").str.strip() for _, s in df.iloc[:, 3:].items()],
            sep=" ",
        )
        .str.strip()
        .str.replace(r"unfoun(ded?)?", "UNFOUNDED", regex=True, case=False)
        .str.replace(r"not sustain(ed?)?", "NOT SUSTAINED", regex=True, case=False)
        .str.replace(r"sus(ainted|tain(ed?))?", "SUSTAINED", regex=True, case=False)
        .str.replace("exonerated", "EXONERATED", regex=False)
        .str.replace("exceptional", "EXCEPTIONAL", regex=False)
        .str.replace(r"policy failu(re)?", "POLICY FAILURE", regex=True, case=False)
        .str.replace(r"false compl(etion)?", "FALSE COMPLETION", regex=True, case=False)
        .str.replace(
            r"no policy vi(olation)?", "NO POLICY VIOLATION", regex=True, case=False
        )
        .str.replace(disp_pattern + r" ?([^ ])", r"\1; \2", regex=True)
        .str.replace(r"(\d+),(\d+)", r"\1.\2", regex=True)
        .str.replace(r"([a-z]) ([a-zA-Z]{3,4} \d+\.\d+)", r"\1; \2", regex=True)
    )

    # split rows with multiple charges (separated by ;)
    df = (
        pd.DataFrame(
            combined.str.split("; ").tolist(),
            index=pd.MultiIndex.from_frame(df.iloc[:, :2]),
        )
        .stack()
        .reset_index()
        .iloc[:, [0, 1, 3]]
        .rename(columns={0: "charges"})
    )

    # split "charges" into "allegation" and "disposition"
    df = (
        pd.concat(
            [df, df.charges.str.extract(r"(.*?)(?:%s)?$" % disp_pattern)],
            axis=1,
        )
        .drop(columns=["charges"])
        .rename(columns={0: "allegation", 1: "disposition"})
    )

    return df


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.strip()
        .str.lower()
        .str.replace(r"(\d+),(\d+)", r"\1.\2", regex=True)
        .str.replace(r"chain of( co?)?$", "chain of command", regex=True)
        .str.replace(r"sick le$", "sick leave", regex=True)
        .str.replace(r"courtes$", "courtesy", regex=True)
        .str.replace(r"use of( for?)?$", "use of force", regex=True)
        .str.replace(r"jail secur$", "jail security", regex=True)
        .str.replace(r"rules & regula$", "rules & regulation", regex=True)
        .str.replace(r"disobedie$", "disobedience", regex=True)
        .str.replace(r"insubordi$", "insubordination", regex=True)
        .str.replace(r"misdem$", "misdemeanor", regex=True)
        .str.replace(r"evidenc$", "evidencence", regex=True)
        .str.replace(r"felony/misd$", "felony/misdemeanor", regex=True)
        .str.replace(r"unbeco$", "unbecoming", regex=True)
        .str.replace(r"^(spd|scj|sfd|scja) ?", "", regex=True)
    )
    return df


def clean_receive_date(df, year):
    df.loc[:, "receive_date"] = (
        df.receive_date.str.strip()
        .str.replace(r"\b(\d)/", r"0\1/", regex=True)
        .str.replace(r"-$", "", regex=True)
        .str.replace(r"(\d+)/(\d+)/\d+", r"\1/\2/" + year, regex=True)
    )
    return df


def clean_tracking_number(df):
    df.loc[:, "tracking_number"] = (
        df.tracking_number.str.strip().str.upper().str.replace(r"^0 ", "O ", regex=True)
    )
    return df


def clean_cprr_disposition(disp_df, year):
    return (
        disp_df.pipe(clean_column_names)
        .rename(
            columns={
                "iab_num": "tracking_number",
                "date": "receive_date",
            }
        )
        .pipe(stack_disposition_rows)
        .pipe(clean_receive_date, year)
        .pipe(
            set_values,
            {
                "agency": "Shreveport PD",
                "data_production_year": year,
            },
        )
        .pipe(clean_tracking_number)
    )


def stack_name_rows(df):
    return (
        pd.DataFrame(df.iloc[:, 4:])
        .set_index(pd.MultiIndex.from_frame(df.iloc[:, :4]))
        .stack()
        .reset_index()
        .drop(columns=["level_4"])
        .rename(columns={0: "allegation"})
    )


def split_names(df):
    df = (
        pd.concat(
            [
                df,
                df.officer_name.str.strip()
                .str.lower()
                .str.extract(r"([^ ]+),? (.+?)(?: (jr|sr)\.)?$"),
            ],
            axis=1,
        )
        .drop(columns=["officer_name"])
        .rename(columns={0: "last_name", 1: "first_name", 2: "suffix"})
    )
    df.loc[:, "last_name"] = df.last_name.str.cat(
        df.suffix.fillna(""), sep=" "
    ).str.strip()
    names = df.first_name.str.extract(r"(\w{2,}) (\w+)")
    df.loc[names[0].notna(), "first_name"] = names.loc[names[0].notna(), 0]
    df.loc[:, "middle_name"] = names.loc[:, 1]
    return df.drop(columns=["suffix"])


def clean_cprr_names(cprr_df, year):
    return (
        cprr_df.pipe(clean_column_names)
        .rename(
            columns={
                "iab_number": "tracking_number",
                "complainant_s_name": "complainant_name",
                "date": "receive_date",
                "officer_s_name": "officer_name",
            }
        )
        .pipe(clean_receive_date, year)
        .pipe(stack_name_rows)
        .pipe(split_names)
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(
            set_values,
            {
                "agency": "Shreveport PD",
                "data_production_year": year,
            },
        )
        .pipe(clean_tracking_number)
        .pipe(gen_uid, ["agency", "first_name", "last_name", "middle_name"])
    )


def make_disposition_categorical(df):
    df.loc[:, "disposition"] = df.disposition.astype(
        pd.CategoricalDtype(
            categories=[
                "SUSTAINED",
                "NOT SUSTAINED",
                "UNFOUNDED",
                "EXONERATED",
                "NO FURTHER",
                "EXCEPTIONAL",
                "POLICY FAILURE",
                "FALSE COMPLETION",
                "NO POLICY VIOLATION",
            ],
            ordered=True,
        )
    )
    return df


def clean_cprr(disposition_file, name_file, year):
    df = clean_cprr_disposition(disposition_file, year)
    name_df = clean_cprr_names(name_file, year)

    # merge 2 frames
    df = df.merge(
        name_df[
            [
                "tracking_number",
                "uid",
                "first_name",
                "last_name",
                "middle_name",
                "complainant_name",
            ]
        ].drop_duplicates(),
        on="tracking_number",
        how="left",
    )
    df = pd.concat(
        [df, name_df[~name_df.tracking_number.isin(df.tracking_number.unique())]]
    )

    return (
        df.pipe(clean_allegation)
        .drop_duplicates()
        .pipe(gen_uid, ["agency", "tracking_number", "allegation"], "allegation_uid")
        .pipe(make_disposition_categorical)
        .sort_values(
            ["receive_date", "tracking_number", "disposition"],
            ascending=[True, True, False],
            na_position="first",
        )
        .drop_duplicates(["allegation_uid"], keep="last")
        .reset_index(drop=True)
    )


def clean_codebook():
    df = pd.read_csv(
        deba.data("raw/shreveport_pd/shreveport_codebook.csv"),
        names=["name", "code"],
    )
    df.loc[:, "name"] = df.name.str.strip().str.lower()
    df.loc[:, "code"] = df.code.str.strip().str.replace(
        r"(\d+),(\d+)", r"\1\.\2", regex=True
    )
    return df


if __name__ == "__main__":
    df = pd.concat(
        [
            clean_cprr(
                pd.read_csv(
                    deba.data(
                        "raw/shreveport_pd/shreveport_pd_cprr_dispositions_2018.csv"
                    )
                ),
                pd.read_csv(
                    deba.data("raw/shreveport_pd/shreveport_pd_cprr_names_2018.csv")
                ),
                "2018",
            ),
            clean_cprr(
                pd.read_csv(
                    deba.data(
                        "raw/shreveport_pd/shreveport_pd_cprr_dispositions_2019.csv"
                    )
                ),
                pd.read_csv(
                    deba.data("raw/shreveport_pd/shreveport_pd_cprr_names_2019.csv")
                ),
                "2019",
            ),
        ]
    )
    cb_df = clean_codebook()
    df.to_csv(deba.data("clean/cprr_shreveport_pd_2018_2019.csv"), index=False)
    cb_df.to_csv(deba.data("clean/cprr_codebook_shreveport_pd.csv"), index=False)
