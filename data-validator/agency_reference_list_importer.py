import pandas as pd


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
