from .columns import rearrange_personel_columns


def remove_unnamed_cols(df):
    return df[[col for col in df.columns if not col.startswith("Unnamed:")]]


def clean_pprr(orig_df):
    df = orig_df[["Birthdate - Personal Dta", "Department ID - Job Dta", "Empl ID",
                  "Badge Nbr", "Job Code - Job Dta", "Description - Job Code Info"]]
    name_df = orig_df["Name - Personal Dta"].str.split(",", expand=True)
    df.loc[:, "Last Name"] = name_df.iloc[:, 0]
    name_df = name_df.iloc[:, 1].str.split(" ", expand=True)
    df.loc[:, "First Name"] = name_df.iloc[:, 0]
    df.loc[:, "Middle Name"] = name_df.iloc[:, 1].fillna("")
    df.rename(columns={
        "Birthdate - Personal Dta": "Birth Year",
        "Department ID - Job Dta": "Department #",
        "Badge Nbr": "Badge #",
        "Job Code - Job Dta": "Rank Number #",
        "Description - Job Code Info": "Rank",
        "Empl ID": "Employee ID #"
    }, inplace=True)
    return rearrange_personel_columns(df)
