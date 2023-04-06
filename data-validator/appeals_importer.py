import os
import pandas as pd
from slack_sdk import WebClient
# from wrgl import Repository


# def __retrieve_appeal_frm_wrgl_data(branch=None):
#     repo = Repository("https://wrgl.llead.co/", None)

#     original_commit = repo.get_branch("appeal-hearing")

#     columns = original_commit.table.columns
#     if not set(APPEAL_COLS).issubset(set(columns)):
#         raise Exception('BE appeal columns are not recognized in the current commit')

#     all_rows = list(repo.get_blocks("heads/appeal-hearing"))
#     df = pd.DataFrame(all_rows)
#     df.columns = df.iloc[0]
#     df = df.iloc[1:].reset_index(drop=True)

#     df.to_csv('appeals.csv', index=False)


def __build_appeal_rel(db_con, appeals_df, appeals_cols):
    client = WebClient(os.environ.get('SLACK_BOT_TOKEN'))

    print('Check integrity between officers\' agency and appeal agency')
    officers_df = pd.read_sql(
        'SELECT id, uid, agency FROM officers_officer',
        con=db_con
    )
    officers_df.columns = ['officer_id', 'uid', 'officer_agency_slug']

    aor_df = pd.merge(appeals_df, officers_df, how='left', on='uid')

    print('Check officer id after merged')
    null_officers_data = aor_df[aor_df['officer_id'].isnull()]
    if len(null_officers_data) > 0:
        null_officers_data.to_csv('null_officers_of_appeals.csv', index=False)

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null officers of appeals",
            file="./null_officers_of_appeals.csv",
            initial_comment="The following file provides a list of personnels that cannot map to appeal:",
        )

        raise Exception('Cannot map officer to appeal')

    aor_df['agency_slug'] = aor_df.apply(
        lambda x: x['officer_agency_slug'] if pd.isnull(x['agency']) \
                    else x['agency'],
        axis=1
    )

    print('Check agency discrepancy')
    diff_agency_data = aor_df[aor_df['officer_agency_slug'] != aor_df['agency_slug']]
    if len(diff_agency_data) > 0:
        raise Exception('There are discrepancy between officer agency and appeal agency')

    agency_df = pd.read_sql(
        'SELECT id, agency_slug FROM departments_department',
        con=db_con
    )
    agency_df.columns = ['department_id', 'agency_slug']

    result = pd.merge(aor_df, agency_df, how='left', on='agency_slug')

    print('Check agency id after merged')
    null_department_data = result[result['department_id'].isnull()]
    if len(null_department_data) > 0:
        null_department_data.to_csv('null_agency_of_appeals.csv', index=False)

        client.files_upload(
            channels=os.environ.get('SLACK_CHANNEL'),
            title="Null agency of appeals",
            file="./null_agency_of_appeals.csv",
            initial_comment="The following file provides a list of agency that cannot map to appeal:",
        )

        raise Exception('Cannot map agency to appeal')

    result = result.loc[:, appeals_cols + ['officer_id', 'department_id']]
    result.to_csv('appeals.csv', index=False)


def run(db_con, appeals_df, appeals_cols):
    __build_appeal_rel(db_con, appeals_df, appeals_cols)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY appeals_appeal(
                {', '.join(appeals_cols + ['officer_id', 'department_id'])}
            )
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('appeals.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM appeals_appeal',
        con=db_con
    )
    print('Number of records in appeal', count.iloc[0][0])
