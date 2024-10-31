import psycopg2
import os
import json
import re
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_slug_from_filename(filename):
    # Extract agency name from filenames like:
    # per_new_orleans_pd.csv
    # com_new_orleans_pd.csv
    # uof_new_orleans_pd.csv
    # etc.
    match = re.match(r'[a-z]+_(.+)\.csv', filename)
    if match:
        return match.group(1).replace('_', '-')
    return None

def get_file_type(filename):
    # Extract type from filename (per, com, uof, etc.)
    match = re.match(r'([a-z]+)_', filename)
    if match:
        return match.group(1)
    return None

def update_wrgl_urls(json_path='csv_urls.json'):
    success_count = 0
    error_count = 0
    skipped_count = 0
    type_stats = {}
    
    logger.info("Starting database update process...")
    
    try:
        conn = psycopg2.connect(
            dbname=os.environ['POSTGRES_DB'],
            user=os.environ['POSTGRES_USER'],
            password=os.environ['POSTGRES_PASSWORD'],
            host='localhost',
            port=5432
        )
        logger.info("Successfully connected to database")
        
        try:
            with open(json_path, 'r') as f:
                urls = json.load(f)
            logger.info(f"Loaded {len(urls)} URLs from {json_path}")
            
            with conn.cursor() as cur:
                # First, verify all slugs exist
                all_slugs = set()
                cur.execute("SELECT slug FROM departments_wrglfile")
                for row in cur.fetchall():
                    all_slugs.add(row[0])
                
                # Map file type to column names
                column_mapping = {
                    'per': ('url', 'download_url'),  # personnel files
                    'com': ('complaint_url', 'complaint_download_url'),  # complaints
                    'uof': ('uof_url', 'uof_download_url'),  # use of force
                    'sas': ('sas_url', 'sas_download_url'),  # stop and search
                    'app': ('appeals_url', 'appeals_download_url'),  # appeals
                    'bra': ('brady_url', 'brady_download_url'),  # brady
                    'doc': ('documents_url', 'documents_download_url')  # documents
                }
                
                for filename, url in urls.items():
                    logger.info(f"\nProcessing {filename}...")
                    slug = get_slug_from_filename(filename)
                    file_type = get_file_type(filename)
                    
                    # Update type statistics
                    if file_type:
                        type_stats[file_type] = type_stats.get(file_type, 0) + 1
                    
                    if not slug or not file_type:
                        logger.error(f"Could not determine slug or type from filename {filename}")
                        error_count += 1
                        continue
                    
                    if slug not in all_slugs:
                        logger.warning(f"No matching record found for slug '{slug}'")
                        skipped_count += 1
                        continue
                    
                    if file_type not in column_mapping:
                        logger.warning(f"Unknown file type '{file_type}' for {filename}")
                        skipped_count += 1
                        continue
                    
                    try:
                        url_column, download_url_column = column_mapping[file_type]
                        
                        cur.execute(f"""
                            UPDATE departments_wrglfile 
                            SET {url_column} = %s, {download_url_column} = %s 
                            WHERE slug = %s
                            RETURNING id
                        """, (url, url, slug))
                        
                        updated = cur.fetchone()
                        if updated:
                            logger.info(f"Successfully updated URLs for {slug}")
                            success_count += 1
                        else:
                            logger.warning(f"No update performed for {slug}")
                            skipped_count += 1
                            
                    except Exception as e:
                        logger.error(f"Failed to update {slug}: {str(e)}")
                        error_count += 1
                        continue
                
                conn.commit()
                logger.info("\nUpdate process completed:")
                logger.info(f"- Successfully updated: {success_count}")
                logger.info(f"- Skipped: {skipped_count}")
                logger.info(f"- Errors: {error_count}")
                logger.info("\nFiles processed by type:")
                for file_type, count in type_stats.items():
                    logger.info(f"- {file_type}: {count} files")
                
                if error_count > 0:
                    raise Exception(f"Encountered {error_count} errors during update")
                    
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse {json_path}: {str(e)}")
            
    except Exception as e:
        logger.error(f"ERROR: {str(e)}")
        raise e
        
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == '__main__':
    update_wrgl_urls()
