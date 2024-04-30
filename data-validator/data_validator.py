import os
import psycopg2
import pandas as pd
import importlib


BE_SCHEMA = {
   'agency_reference_list': [
      'agency_slug', 'agency_name', 'location'
   ],
   'personnel': [
      'uid', 'last_name', 'middle_name', 'first_name', 'birth_year',
      'birth_month', 'birth_day', 'race', 'sex', 'agency'
   ],
   'allegation': [
      'uid', 'tracking_id', 'allegation_uid', 'allegation',
      'disposition', 'action', 'agency', 'allegation_desc', 'coaccusal'
   ],
   'appeals': [
      'appeal_uid', 'uid', 'charging_supervisor', 'appeal_disposition',
      'action_appealed', 'agency', 'motions'
   ],
   'use_of_force': [
      'uid', 'uof_uid', 'tracking_id', 'service_type', 'disposition',
      'use_of_force_description', 'officer_injured', 'agency', 'use_of_force_reason'
   ],
   'citizens': [
      'citizen_uid', 'allegation_uid', 'uof_uid', 'citizen_influencing_factors',
      'citizen_arrested', 'citizen_hospitalized', 'citizen_injured', 'citizen_age',
      'citizen_race', 'citizen_sex', 'agency'
   ],
   'brady': [
      'brady_uid', 'uid', 'disposition', 'action', 'allegation_desc',
      'tracking_id_og', 'source_agency', 'charging_agency', 'agency'
   ],
   'event': [
      'event_uid', 'kind', 'year', 'month', 'day', 'time', 'raw_date', 'uid',
      'allegation_uid', 'appeal_uid', 'uof_uid', 'agency', 'badge_no',
      'department_code', 'department_desc', 'division_desc', 'rank_code',
      'rank_desc', 'salary', 'overtime_annual_total', 'salary_freq', 'left_reason',
      'brady_uid'
   ],
   'person': [
      'person_id', 'canonical_uid', 'uids'
   ],
   'documents': [
      'docid', 'pdf_db_path', 'pdf_db_id', 'pdf_db_content_hash',
      'txt_db_path', 'txt_db_id', 'txt_db_content_hash', 'hrg_type',
      'year', 'month', 'day', 'dt_source', 'hrg_no', 'accused', 'matched_uid',
      'hrg_text', 'title', 'agency'
   ]
}


def run_validator():
   # Establishing the connection
   print("Connecting to postgres...")
   conn = psycopg2.connect(
      database=os.environ.get('POSTGRES_DB'),
      user=os.environ.get('POSTGRES_USER'),
      password=os.environ.get('POSTGRES_PASSWORD'),
      host=os.environ.get('POSTGRES_HOST', 'localhost'),
      port=os.environ.get('POSTGRES_PORT', '5432')
   )

   print('Building schema of BE Database')
   cursor = conn.cursor()
   cursor.execute(open("be_schema.sql", "r").read())

   conn.commit()
   cursor.close()

   for data, be_cols in BE_SCHEMA.items():
      print(f'======== Importing {data} ========')
      df = pd.read_csv(os.path.join(os.environ.get('DATA_DIR'), data + '.csv'))

      columns = df.columns
      if not set(be_cols).issubset(set(columns)):
         raise Exception(f'BE {data} columns are not recognized in the current processed file')

      module = importlib.import_module(f'{data}_importer')
      module.run(conn, df, be_cols)

   conn.close()


if __name__ == "__main__":
   run_validator()
