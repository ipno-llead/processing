PERSONEL_COLUMNS = [
    "Last Name", "Middle Name", "First Name", "Employee ID #",
    "Birth Date", "Birth Year",
]

PERSONEL_HISTORY_COLUMNS = [
    "Badge #", "Department #", "Department", "Rank Number #", "Rank",
    "Hire Date", "Termination Date", "Pay Prog Start Date",
    "Annual Salary", "Hourly Salary", "Year", "Police Department"
]

COMPLAINT_COLUMNS = [
    "Last Name", "First Name", "Middle Name", "Badge #", "Employee ID #",
    "Incident Type", "Complaint Tracking Number", "Date Complaint Occurred",
    "Date Complaint Received", "Date Complaint Investigation Complete",
    "Investigation Status", "Disposition", "Complaint Classification",
    "Bureau of Complainant", "Division of Complainant", "Unit of Complainant",
    "Unit Additional Details of Complainant", "Working Status of Complainant",
    "Shift of Complainant", "Rule Violation", "Paragraph Violation",
    "Unique Officer Allegation ID", "Officer Race Ethnicity", "Officer Gender",
    "Officer Age", "Officer Years of Service", "Complainant Gender",
    "Complainant Ethnicity", "Complainant Age"
]


def rearrange_personel_columns(df):
    existing_cols = set(df.columns)
    return df[[col for col in PERSONEL_COLUMNS if col in existing_cols]]


def rearrange_personel_history_columns(df):
    existing_cols = set(df.columns)
    return df[[col for col in PERSONEL_HISTORY_COLUMNS if col in existing_cols]]


def rearrange_complaint_columns(df):
    existing_cols = set(df.columns)
    return df[[col for col in COMPLAINT_COLUMNS if col in existing_cols]]
