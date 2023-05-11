import os
import pandas as pd
from slack_sdk import WebClient


def __build_event_rel(db_con, event_df, event_cols):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Building events_officers relationship')
    officers_df = pd.read_sql(
        'SELECT id, uid, agency FROM officers_officer',
        con=db_con
    )
    officers_df.columns = ['officer_id', 'uid', 'officer_agency']

    no_officers_in_events = event_df['uid'].dropna().unique()
    print('Number of officers in WRGL event', len(no_officers_in_events))
    diff_officers = set(no_officers_in_events) - set(officers_df['uid'])
    print('Number of differences in officers', len(diff_officers))

    if len(diff_officers) > 0:
        with open('diff_officers_in_event.csv', 'w') as fwriter:
            fwriter.write('\n'.join(list(diff_officers)))

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Diff of officers in event",
            file="./diff_officers_in_event.csv",
            initial_comment="The following file provides a list of uid in event that cannot map to officers:",
        )
        raise Exception('There is anomaly in the number of officers in events')

    print('Building events_agency relationship')
    agency_df = pd.read_sql(
        'SELECT id, agency_slug FROM departments_department',
        con=db_con
    )
    agency_df.columns = ['department_id', 'agency']

    no_agency_in_events = event_df['agency'].dropna().unique()
    print('Number of agency in WRGL events', len(no_agency_in_events))
    diff_agency = set(no_agency_in_events) - set(agency_df['agency'])
    print('Number of differences in agency', len(diff_agency))

    if len(diff_agency) > 0:
        with open('diff_agency_in_events.csv', 'w') as fwriter:
            fwriter.write('\n'.join(list(diff_agency)))

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="diff of agency in event",
            file="./diff_agency_in_events.csv",
            initial_comment="The following file provides a list of agency in event that cannot map to department:",
        )

        raise Exception('There is anomaly in the number of agency in event')

    print('Building events_appeal relationship')
    appeal_df = pd.read_sql(
        'SELECT id, appeal_uid FROM appeals_appeal',
        con=db_con
    )
    appeal_df.columns = ['appeal_id', 'appeal_uid']

    no_appeal_in_events = event_df['appeal_uid'].dropna().unique()
    print('Number of appeal in WRGL events', len(no_appeal_in_events))
    diff_appeal = set(no_appeal_in_events) - set(appeal_df['appeal_uid'])
    print('Number of differences in appeal', len(diff_appeal))

    if len(diff_appeal) > 0:
        with open('diff_appeal_in_events.csv', 'w') as fwriter:
            fwriter.write('\n'.join(list(diff_appeal)))

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="diff of appeal in event",
            file="./diff_appeal_in_events.csv",
            initial_comment="The following file provides a list of appeal in event that cannot map to appeal:",
        )

        raise Exception('There is anomaly in the number of appeal in event')

    print('Building events_uof relationship')
    uof_df = pd.read_sql(
        'SELECT id, uof_uid FROM use_of_forces_useofforce',
        con=db_con
    )
    uof_df.columns = ['use_of_force_id', 'uof_uid']

    no_uof_in_events = event_df['uof_uid'].dropna().unique()
    print('Number of uof in WRGL events', len(no_uof_in_events))
    diff_uof = set(no_uof_in_events) - set(uof_df['uof_uid'])
    print('Number of differences in uof', len(diff_uof))

    if len(diff_uof) > 0:
        with open('diff_uof_in_events.csv', 'w') as fwriter:
            fwriter.write('\n'.join(list(diff_uof)))

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Diff use-of-force in event",
            file="./diff_uof_in_events.csv",
            initial_comment="The following file provides a list of use-of-force in event that cannot map to use-of-force:",
        )

        raise Exception('There is anomaly in the number of use-of-force in event')

    print('Check for integrity of officer agency and agency')
    check_agency_df = event_df.loc[:, ['event_uid', 'uid', 'agency']]
    check_agency_df.dropna(subset=['uid', 'agency'], inplace=True)
    check_agency_df = pd.merge(check_agency_df, officers_df, how='left', on='uid')

    diff_check_agency = check_agency_df[check_agency_df['officer_agency'] != check_agency_df['agency']]
    if len(diff_check_agency) > 0:
        diff_check_agency.drop_duplicates(inplace=True)
        diff_check_agency.to_csv(
            'diff_officers_agency_of_event.csv',
            columns=['event_uid', 'agency', 'uid', 'officer_agency'],
            index=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Diff of officer agency of event",
            file="./diff_officers_agency_of_event.csv",
            initial_comment="The following file provides a list of officers that have conflicting agency in event:",
        )

        raise Exception('There are discrepancy between agency of officers and events')

    print('Building events_brady relationship')
    brady_df = pd.read_sql(
        'SELECT id, brady_uid FROM brady_brady',
        con=db_con
    )
    brady_df.columns = ['brady_id', 'brady_uid']

    result = pd.merge(event_df, officers_df, how='left', on='uid')
    result = pd.merge(result, agency_df, how='left', on='agency')
    result = pd.merge(result, appeal_df, how='left', on='appeal_uid')
    result = pd.merge(result, uof_df, how='left', on='uof_uid')
    result = pd.merge(result, brady_df, how='left', on='brady_uid')

    result = result.loc[:,
        event_cols + [
            'officer_id', 'department_id', 'appeal_id', 'use_of_force_id', 'brady_id'
        ]
    ]

    result = result.astype({
        'year': pd.Int64Dtype(),
        'month': pd.Int64Dtype(),
        'day': pd.Int64Dtype(),
        'officer_id': pd.Int64Dtype(),
        'department_id': pd.Int64Dtype(),
        'appeal_id': pd.Int64Dtype(),
        'use_of_force_id': pd.Int64Dtype(),
        'brady_id': pd.Int64Dtype()
    })
    result.to_csv('events.csv', index=False)


def run(db_con, event_df, event_cols):
    __build_event_rel(db_con, event_df, event_cols)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY officers_event(
                {', '.join(
                    event_cols + [
                        'officer_id', 'department_id', 'appeal_id',
                        'use_of_force_id', 'brady_id'
                    ]
                )}
            ) FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('events.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM officers_event',
        con=db_con
    )
    print('Number of records in event', count.iloc[0][0])
