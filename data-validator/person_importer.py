import os
import pandas as pd
from slack_sdk import WebClient


def __build_person_rel(db_con, person_df, person_cols):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Building canonical_officer relationship')
    officer_df = pd.read_sql('SELECT id, uid FROM officers_officer', con=db_con)
    officer_df.columns = ['canonical_officer_id', 'canonical_uid']

    result = pd.merge(person_df, officer_df, how='left', on='canonical_uid')

    print('Check officer canonical id after merged')
    null_canonical_data = result[result['canonical_officer_id'].isnull()]
    if len(null_canonical_data) > 0:
        null_canonical_data.drop_duplicates(subset=['canonical_uid'], inplace=True)
        null_canonical_data.to_csv(
            'null_canonical_uid_of_person.csv',
            columns=['canonical_uid'],
            index=False,
            header=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null canonical uid of person",
            file="./null_canonical_uid_of_person.csv",
            initial_comment="The following file provides a list of canonical uids in person that cannot map to personnel:",
        )

        raise Exception('Cannot map canonical uid in person to personnel')

    result = result.loc[:, person_cols + ['canonical_officer_id']]
    result.to_csv('person.csv', index=False)


def __update_person_id_in_officer(db_con):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    person_df = pd.read_sql('SELECT id, uids FROM people_person', con=db_con)
    person_df = (
        person_df.drop("uids", axis=1)
        .join(
            person_df["uids"]
            .str.split(",", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("uid"),
            how="outer",
        )
        .reset_index(drop=True)
    )

    officer_df = pd.read_sql('SELECT id, uid FROM officers_officer', con=db_con)
    officer_df.columns = ['officer_id', 'uid']

    uid_df = pd.merge(person_df, officer_df, how='left', on='uid')

    print('Check officer id after merged')
    null_officer_data = uid_df[uid_df['officer_id'].isnull()]
    if len(null_officer_data) > 0:
        null_officer_data.drop_duplicates(subset=['uid'], inplace=True)
        null_officer_data.to_csv(
            'null_officers_of_person.csv',
            columns=['uid'],
            index=False,
            header=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null officers of person",
            file="./null_officers_of_person.csv",
            initial_comment="The following file provides a list of uids in person that cannot map to personnel:",
        )

        raise Exception('Cannot map uid in person to personnel')

    print('Updating person_id in officers')
    cursor = db_con.cursor()
    for p_id, p_uid in person_df.values.tolist():
        cursor.execute(
            f'''
            UPDATE officers_officer
            SET person_id={p_id}
            WHERE uid='{p_uid}'
            '''
        )
    db_con.commit()
    cursor.close()

    check_null_person_officer = pd.read_sql(
        'SELECT id, uid, person_id FROM officers_officer WHERE person_id IS NULL',
        con=db_con
    )
    print('Check person_id in officers')
    if len(check_null_person_officer) > 0:
        check_null_person_officer.to_csv(
            'null_person_id_in_officers.csv',
            columns=['uid'],
            index=False,
            header=False
        )

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null person id in officers",
            file="./null_person_id_in_officers.csv",
            initial_comment="The following file provides a list of uids in officers that cannot find person_id:",
        )

        raise Exception('There are officers that have no person_id')


def run(db_con, person_df, person_cols):
    __build_person_rel(db_con, person_df, person_cols)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY people_person({', '.join(person_cols + ['canonical_officer_id'])})
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('person.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    __update_person_id_in_officer(db_con)

    count = pd.read_sql(
        'SELECT COUNT(*) FROM people_person',
        con=db_con
    )
    print('Number of records in person', count.iloc[0][0])
