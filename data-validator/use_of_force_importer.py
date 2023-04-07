import os
import pandas as pd
from slack_sdk import WebClient


def __build_uof_rel(db_con, uof_df, uof_cols):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Check integrity between officers\' agency and uof agency')
    officers_df = pd.read_sql(
        'SELECT id, uid, agency FROM officers_officer',
        con=db_con
    )
    officers_df.columns = ['officer_id', 'uid', 'officer_agency_slug']

    uofor_df = pd.merge(uof_df, officers_df, how='left', on='uid')

    print('Check officer id after merged')
    null_officers_data = uofor_df[uofor_df['officer_id'].isnull()]
    if len(null_officers_data) > 0:
        null_officers_data.to_csv('null_officers_of_uof.csv', index=False)

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null officers of uof",
            file="./null_officers_of_uof.csv",
            initial_comment="The following file provides a list of personnels that cannot map to uof:",
        )

        raise Exception('Cannot map officer to uof')

    uofor_df['agency_slug'] = uofor_df.apply(
        lambda x: x['officer_agency_slug'] if pd.isnull(x['agency']) \
                    else x['agency'],
        axis=1
    )

    print('Check agency discrepancy')
    diff_agency_data = uofor_df[uofor_df['officer_agency_slug'] != uofor_df['agency_slug']]
    if len(diff_agency_data) > 0:
        raise Exception('There are discrepancy between officer agency and uof agency')

    agency_df = pd.read_sql(
        'SELECT id, agency_slug FROM departments_department',
        con=db_con
    )
    agency_df.columns = ['department_id', 'agency_slug']

    result = pd.merge(uofor_df, agency_df, how='left', on='agency_slug')

    print('Check agency id after merged')
    null_department_data = result[result['department_id'].isnull()]
    if len(null_department_data) > 0:
        null_department_data.to_csv('null_agency_of_uof.csv', index=False)

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null agency of uof",
            file="./null_agency_of_uof.csv",
            initial_comment="The following file provides a list of agency that cannot map to uof:",
        )

        raise Exception('Cannot map agency to uof')

    result = result.loc[:, uof_cols + ['officer_id', 'department_id']]
    result.to_csv('uof.csv', index=False)


def run(db_con, uof_df, uof_cols):
    __build_uof_rel(db_con, uof_df, uof_cols)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY use_of_forces_useofforce(
                {', '.join(uof_cols + ['officer_id', 'department_id'])}
            )
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('uof.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM use_of_forces_useofforce',
        con=db_con
    )
    print('Number of records in useofforce', count.iloc[0][0])
