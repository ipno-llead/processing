import os
import pandas as pd
from slack_sdk import WebClient


def __build_citizen_rel(db_con, citizens_df, citizens_cols):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Building citizens_agency relationship')
    agency_df = pd.read_sql(
        'SELECT id, agency_slug FROM departments_department',
        con=db_con
    )
    agency_df.columns = ['department_id', 'agency']

    result = pd.merge(citizens_df, agency_df, how='left', on='agency')

    print('Check agency id after merged')
    null_department_data = result[result['department_id'].isnull()]
    if len(null_department_data) > 0:
        null_department_data.drop_duplicates(subset=['agency'], inplace=True)
        null_department_data.to_csv(
            'null_agency_of_citizen.csv',
            columns=['agency'],
            index=False,
            header=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null agency of citizen",
            file="./null_agency_of_citizen.csv",
            initial_comment="The following file provides a list of agency in citizens that cannot map to departments:",
        )

        raise Exception('Cannot map agency to citizen')

    print('Building citizens_complaints relationship')
    complaints_df = pd.read_sql(
        'SELECT id, allegation_uid FROM complaints_complaint',
        con=db_con
    )
    complaints_df.columns = ['complaint_id', 'allegation_uid']

    result = pd.merge(result, complaints_df, how='left', on='allegation_uid')

    print('Building citizens_uof relationship')
    uof_df = pd.read_sql(
        'SELECT id, uof_uid FROM use_of_forces_useofforce',
        con=db_con
    )
    uof_df.columns = ['use_of_force_id', 'uof_uid']

    result = pd.merge(result, uof_df, how='left', on='uof_uid')

    result = result.loc[:, citizens_cols + ['department_id', 'complaint_id', 'use_of_force_id']]
    result = result.astype({
        'department_id': int,
        'complaint_id': pd.Int64Dtype(),
        'use_of_force_id': pd.Int64Dtype(),
        'citizen_age': pd.Int64Dtype()
    })
    result.to_csv('citizens.csv', index=False)


def run(db_con, citizens_df, citizens_cols):
    __build_citizen_rel(db_con, citizens_df, citizens_cols)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY citizens_citizen(
                {', '.join(citizens_cols + ['department_id', 'complaint_id', 'use_of_force_id'])}
            )
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('citizens.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM citizens_citizen',
        con=db_con
    )
    print('Number of records in citizen', count.iloc[0][0])
