import os
import pandas as pd
from slack_sdk import WebClient


def __build_brady_rel(db_con, brady_df, brady_cols):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Building brady_officers relationship')
    officers_df = pd.read_sql(
        'SELECT id, uid FROM officers_officer',
        con=db_con
    )
    officers_df.columns = ['officer_id', 'uid']

    no_officers_in_brady = brady_df['uid'].dropna().unique()
    print('Number of officers in WRGL brady', len(no_officers_in_brady))
    diff_officers = set(no_officers_in_brady) - set(officers_df['uid'])
    print('Number of differences in officers', len(diff_officers))

    if len(diff_officers) > 0:
        with open('diff_officers_in_brady.csv', 'w') as fwriter:
            fwriter.write('\n'.join(list(diff_officers)))

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Diff of officers in brady",
            file="./diff_officers_in_brady.csv",
            initial_comment="The following file provides a list of uid in brady that cannot map to officers:",
        )
        raise Exception('There is anomaly in the number of officers in brady')

    print('Building brady_agency relationship')
    agency_df = pd.read_sql(
        'SELECT id, agency_slug FROM departments_department',
        con=db_con
    )
    agency_df.columns = ['department_id', 'agency']

    no_agency_in_brady = brady_df['agency'].dropna().unique()
    print('Number of agency in WRGL brady', len(no_agency_in_brady))
    diff_agency = set(no_agency_in_brady) - set(agency_df['agency'])
    print('Number of differences in agency', len(diff_agency))

    if len(diff_agency) > 0:
        with open('diff_agency_in_brady.csv', 'w') as fwriter:
            fwriter.write('\n'.join(list(diff_agency)))

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="diff of agency in brady",
            file="./diff_agency_in_brady.csv",
            initial_comment="The following file provides a list of agency in brady that cannot map to department:",
        )

        raise Exception('There is anomaly in the number of agency in brady')

    result = pd.merge(brady_df, officers_df, how='left', on='uid')
    result = pd.merge(result, agency_df, how='left', on='agency')

    result = result.loc[:, brady_cols + ['officer_id', 'department_id']]
    result.to_csv('brady.csv', index=False)


def run(db_con, brady_df, brady_cols):
    __build_brady_rel(db_con, brady_df, brady_cols)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY brady_brady({', '.join(brady_cols + ['officer_id', 'department_id'])})
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('brady.csv','r'),
    )

    db_con.commit()
    cursor.close()
