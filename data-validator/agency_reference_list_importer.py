import pandas as pd
# from wrgl import Repository


# def __retrieve_wrgl_data(branch=None):
#     repo = Repository("https://wrgl.llead.co/", None)

#     original_commit = repo.get_branch("agency-reference-list")

#     columns = original_commit.table.columns
#     if not set(AGENCY_COLS).issubset(set(columns)):
#         raise Exception('BE agency columns are not recognized in the current commit')

#     all_rows = list(repo.get_blocks("heads/agency-reference-list"))
#     df = pd.DataFrame(all_rows)
#     df.columns = df.iloc[0]
#     df = df.iloc[1:].reset_index(drop=True)

#     df.to_csv('agency.csv', index=False)


def run(db_con, agency_df, agency_cols):
    agency_df = agency_df.loc[:, agency_cols]
    agency_df.to_csv('agency.csv', index=False)

    cursor = db_con.cursor()
    cursor.copy_expert(
        sql=f"""
            COPY departments_department({', '.join(agency_cols)})
            FROM stdin WITH CSV HEADER
            DELIMITER as ','
        """,
        file=open('agency.csv', 'r'),
    )
    db_con.commit()
    cursor.close()

    count = pd.read_sql(
        'SELECT COUNT(*) FROM departments_department',
        con=db_con
    )
    print('Number of records in department', count.iloc[0][0])
