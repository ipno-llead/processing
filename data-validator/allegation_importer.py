import os
import pandas as pd
from slack_sdk import WebClient
# from wrgl import Repository


# def __retrieve_complaint_frm_wrgl_data():
#     repo = Repository("https://wrgl.llead.co/", None)

#     original_commit = repo.get_branch("allegation")

#     columns = original_commit.table.columns
#     if not set(COMPLAINT_COLS).issubset(set(columns)):
#         raise Exception('BE complaint columns are not recognized in the current commit')

#     all_rows = list(repo.get_blocks("heads/allegation"))
#     df = pd.DataFrame(all_rows)
#     df.columns = df.iloc[0]
#     df = df.iloc[1:].reset_index(drop=True)

#     df = df.loc[:, COMPLAINT_COLS]
#     df.to_csv('complaint.csv', index=False)


def __build_complaints_relationship(db_con):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Building complaints_officers relationship')
    complaints_df = pd.read_sql(
        'SELECT id, allegation_uid, uid, agency FROM complaints_complaint',
        con=db_con
    )
    complaints_df.columns = ['complaint_id', 'allegation_uid', 'uid', 'complaint_agency']

    officers_df = pd.read_sql(
        'SELECT id, uid, agency FROM officers_officer',
        con=db_con
    )
    officers_df.columns = ['officer_id', 'uid', 'officer_agency_slug']

    cor_df = pd.merge(complaints_df, officers_df, how='left', on='uid')

    print('Check officer id after merged')
    null_officers_data = cor_df[cor_df['officer_id'].isnull()]
    if len(null_officers_data) > 0:
        null_officers_data.drop_duplicates(subset=['uid'], inplace=True)
        null_officers_data.to_csv(
            'null_officers_of_complaints.csv',
            columns=['uid'],
            index=False,
            header=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null officers of complaints",
            file="./null_officers_of_complaints.csv",
            initial_comment="The following file provides a list of officers in complaints that cannot map to personnel:",
        )

        raise Exception('Cannot map officer to complaint')

    cod_df = cor_df.copy()

    cor_df = cor_df.loc[:, ["complaint_id", "officer_id"]]
    cor_df.to_csv('complaints_officers_rel.csv', index=False)

    print('Building complaints_departments relationship')
    cod_df['agency_slug'] = cod_df.apply(
        lambda x: x['officer_agency_slug'] if pd.isnull(x['complaint_agency']) \
                    else x['complaint_agency'],
        axis=1
    )

    null_inferred_agency_data = cod_df[cod_df['agency_slug'].isnull()]
    if len(null_inferred_agency_data) > 0:
        raise Exception('Cannot find agency for complaint')

    diff_officer_agency = cod_df[cod_df['officer_agency_slug'] != cod_df['agency_slug']]
    if len(diff_officer_agency) > 0:
        diff_officer_agency.to_csv(
            'diff_officer_agency_of_complaints.csv',
            columns=['allegation_uid', 'uid', 'complaint_agency', 'officer_agency_slug'],
            index=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Diff officer agency of complaints",
            file="./diff_officer_agency_of_complaints.csv",
            initial_comment="The following file provides a list of officers that have conflicting agency in complaints:",
        )

        raise Exception('There are discrepancy between officer and agency data')

    agency_df = pd.read_sql(
        'SELECT id, agency_slug FROM departments_department',
        con=db_con
    )
    agency_df.columns = ['department_id', 'agency_slug']

    cdr_df = pd.merge(cod_df, agency_df, how='left', on='agency_slug')

    print('Check agency id after merged')
    null_department_data = cdr_df[cdr_df['department_id'].isnull()]
    if len(null_department_data) > 0:
        null_department_data.drop_duplicates(subset=['complaint_agency'], inplace=True)
        null_department_data.to_csv(
            'null_agency_of_complaints.csv',
            columns=['complaint_agency'],
            index=False,
            header=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null agency of complaints",
            file="./null_agency_of_complaints.csv",
            initial_comment="The following file provides a list of agency in complaints that cannot map to departments:",
        )

        raise Exception('Cannot map agency to complaint')

    cdr_df = cdr_df.astype({
        'complaint_id': pd.Int64Dtype(),
        'department_id': pd.Int64Dtype()
    })

    cdr_df = cdr_df.loc[:, ["complaint_id", "department_id"]]
    cdr_df.to_csv('complaints_departments_rel.csv', index=False)


def run(db_con, allegation_df, allegation_cols):
    allegation_df = allegation_df.loc[:, allegation_cols]
    allegation_df.to_csv('complaints.csv', index=False)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY complaints_complaint({', '.join(allegation_cols)})
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('complaints.csv','r'),
    )

    db_con.commit()
    cursor.close()

    __build_complaints_relationship(db_con)

    # Importing complaints and officers relationship'
    cursor = db_con.cursor()
    cursor.copy_expert(
        sql="""
            COPY complaints_complaint_officers(
                complaint_id, officer_id
            ) FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('complaints_officers_rel.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM complaints_complaint_officers',
        con=db_con
    )
    print('Number of records in complaints_officers rel', count.iloc[0][0])

    # Importing complaints and agency relationship
    cursor = db_con.cursor()
    cursor.copy_expert(
        sql="""
            COPY complaints_complaint_departments(
                complaint_id, department_id
            ) FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('complaints_departments_rel.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM complaints_complaint_departments',
        con=db_con
    )
    print('Number of records in complaints_departments rel', count.iloc[0][0])
