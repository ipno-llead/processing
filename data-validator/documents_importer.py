import os
import pandas as pd
from slack_sdk import WebClient
from wrgl import Repository

def __build_document_rel(db_con, documents_df):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Building documents_officers relationship')
    officers_df = pd.read_sql(
        'SELECT id, uid FROM officers_officer',
        con=db_con
    )
    officers_df.columns = ['officer_id', 'uid']

    documents_df = documents_df[["docid", "matched_uid", "agency"]]

    dor_df = pd.merge(documents_df, officers_df, how='left', left_on='matched_uid', right_on='uid')

    no_officers_in_documents = documents_df['matched_uid'].dropna().unique()
    print('Number of officers in WRGL documents', len(no_officers_in_documents))
    diff_officers = set(no_officers_in_documents) - set(officers_df['uid'])
    print('Number of differences in officers', len(diff_officers))

    if len(diff_officers) > 0:
        with open('diff_officers_of_documents.csv', 'w') as fwriter:
            fwriter.write('\n'.join(list(diff_officers)))

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Diff of officers of documents",
            file="./diff_officers_of_documents.csv",
            initial_comment="The following file provides a list of matched_uid in documents that cannot map to officers:",
        )

        raise Exception('There is anomaly in the number of officers in documents')

    dor_df.dropna(subset=['officer_id'], inplace=True)

    dor_df = dor_df.loc[:, ['docid', 'officer_id']]
    dor_df = dor_df.astype({
        'docid': str,
        'officer_id': pd.Int64Dtype(),
    })
    dor_df.to_csv('documents_officers_rel.csv', index=False)

    print('Building documents_agency relationship')
    agency_df = pd.read_sql(
        'SELECT id, agency_slug FROM departments_department',
        con=db_con
    )
    agency_df.columns = ['department_id', 'agency_slug']

    ddr_df = pd.merge(documents_df, agency_df, how='left', left_on='agency', right_on='agency_slug')

    no_agency_in_documents = documents_df['agency'].dropna().unique()
    print('Number of agency in WRGL documents', len(no_agency_in_documents))
    diff_agency = set(no_agency_in_documents) - set(agency_df['agency_slug'])
    print('Number of differences in agency', len(diff_agency))

    if len(diff_agency) > 0:
        with open('diff_agency_of_documents.csv', 'w') as fwriter:
            fwriter.write('\n'.join(list(diff_agency)))

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Diff of agency of documents",
            file="./diff_agency_of_documents.csv",
            initial_comment="The following file provides a list of agency in documents that cannot map to department:",
        )

        raise Exception('There is anomaly in the number of agency in documents')

    ddr_df.dropna(subset=['department_id'], inplace=True)

    ddr_df = ddr_df.loc[:, ['docid', 'department_id']]
    ddr_df = ddr_df.astype({
        'docid': str,
        'department_id': pd.Int64Dtype(),
    })
    ddr_df.to_csv('documents_departments_rel.csv', index=False)


def run(db_con, documents_df, documents_cols):
    print("Starting the document relationships build.")
    __build_document_rel(db_con, documents_df)

    print("Starting to copy documents data to the database.")
    cursor = db_con.cursor()
    try:
        cursor.copy_expert(
            sql=f"""
                COPY documents_document({', '.join(documents_cols)})
                FROM stdin WITH CSV HEADER
                DELIMITER as ','
            """,
            file=open('documents.csv', 'r'),
        )
        db_con.commit()
        print("Documents data successfully copied to the database.")
    except Exception as e:
        db_con.rollback()
        print("Failed to copy documents data to the database:", e)
    finally:
        cursor.close()

    print("Starting to copy documents and officers relationship data to the database.")
    cursor = db_con.cursor()
    try:
        cursor.copy_expert(
            sql="""
                COPY documents_document_officers(
                    docid, officer_id
                ) FROM stdin WITH CSV HEADER
                DELIMITER as ','
            """,
            file=open('documents_officers_rel.csv', 'r'),
        )
        db_con.commit()
        print("Documents and officers relationship data successfully copied to the database.")
    except Exception as e:
        db_con.rollback()
        print("Failed to copy documents and officers relationship data to the database:", e)
    finally:
        cursor.close()

    try:
        count = pd.read_sql(
            'SELECT COUNT(*) FROM documents_document_officers',
            con=db_con
        )
        print('Number of records in documents_officers relationship:', count.iloc[0][0])
    except Exception as e:
        print("Failed to count records in documents_document_officers:", e)

    print("Starting to copy documents and agency relationship data to the database.")
    cursor = db_con.cursor()
    try:
        cursor.copy_expert(
            sql="""
                COPY documents_document_departments(
                    docid, department_id
                ) FROM stdin WITH CSV HEADER
                DELIMITER as ','
            """,
            file=open('documents_departments_rel.csv', 'r'),
        )
        db_con.commit()
        print("Documents and agency relationship data successfully copied to the database.")
    except Exception as e:
        db_con.rollback()
        print("Failed to copy documents and agency relationship data to the database:", e)
    finally:
        cursor.close()

    try:
        count = pd.read_sql(
            'SELECT COUNT(*) FROM documents_document_departments',
            con=db_con
        )
        print('Number of records in documents_departments relationship:', count.iloc[0][0])
    except Exception as e:
        print("Failed to count records in documents_document_departments:", e)
