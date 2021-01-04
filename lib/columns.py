PERSONEL_COLUMNS = ["Last Name", "Middle Name", "First Name", "Badge #", "Employee ID #", "Department #", "Department",
                    "Rank Number #", "Rank", "Birth Date", "Birth Year", "Hire Date", "Termination Date", "Pay Prog Start Date", "Annual Salary", "Hourly Salary"]


def rearrange_personel_columns(df):
    existing_cols = set(df.columns)
    return df[[col for col in PERSONEL_COLUMNS if col in existing_cols]]
