import os
import pandas as pd
from slack_sdk import WebClient


def __build__officer_rel(db_con, officer_df, officer_cols):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Building officers_agency relationship')
    agency_df = pd.read_sql(
        'SELECT id, agency_slug FROM departments_department',
        db_con
    )
    agency_df.columns = ['department_id', 'agency']

    print('Check for duplicated records')
    check_dup_officers = officer_df[officer_df.duplicated(['uid', 'agency'])]
    if len(check_dup_officers) > 0:
        check_dup_officers.to_csv(
            'duplicated_officers.csv',
            index=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title='Duplicated officers',
            file="./duplicated_officers.csv",
            initial_comment='The following file provides a list of duplicated personnels:',
        )

        raise Exception('There are duplicated records in personnel')

    result = pd.merge(officer_df, agency_df, how='left', on='agency')

    print('Check agency id after merged')
    null_data = result[result['department_id'].isnull()]
    if len(null_data) > 0:
        null_data.drop_duplicates(subset=['agency'], inplace=True)
        null_data.to_csv(
            'null_agency_of_officers.csv',
            columns=['agency'],
            index=False,
            header=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title='Null agency of officers',
            file="./null_agency_of_officers.csv",
            initial_comment='The following file provides a list of agency in personnels that cannot map to departments:',
        )

        raise Exception('Cannot map officer to agency')

    result = result.astype({
        'department_id': int,
        'birth_year': pd.Int64Dtype(),
        'birth_month': pd.Int64Dtype(),
        'birth_day': pd.Int64Dtype()
    })
    result = result.loc[:, officer_cols]
    result.to_csv('officers.csv', index=False)


def run(db_con, officer_df, officer_cols):
    __build__officer_rel(db_con, officer_df, officer_cols)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY officers_officer({', '.join(officer_cols)})
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('officers.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM officers_officer',
        con=db_con
    )
    print('Number of records in officer', count.iloc[0][0])
