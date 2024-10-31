import psycopg2
import os
import json
import re
from typing import Dict, Set, Tuple

def get_db_connection():
    return psycopg2.connect(
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        host='localhost',
        port=5432
    )

def get_column_mapping(file_type: str) -> Tuple[str, str]:
    mappings = {
        'per': ('url', 'download_url'),
        'com': ('complaint_url', 'complaint_download_url'),
        'uof': ('uof_url', 'uof_download_url'),
        'sas': ('sas_url', 'sas_download_url'),
        'app': ('appeals_url', 'appeals_download_url'),
        'bra': ('brady_url', 'brady_download_url'),
        'doc': ('documents_url', 'documents_download_url')
    }
    return mappings.get(file_type)

def process_file(cur, filename: str, url: str, all_slugs: Set[str]) -> Tuple[bool, str]:
    slug = re.match(r'[a-z]+_(.+)\.csv', filename)
    file_type = re.match(r'([a-z]+)_', filename)
    
    if not (slug and file_type):
        return False, "Invalid filename format"
    
    slug = slug.group(1).replace('_', '-')
    file_type = file_type.group(1)
    
    if slug not in all_slugs:
        return False, f"No matching record for slug '{slug}'"
        
    columns = get_column_mapping(file_type)
    if not columns:
        return False, f"Unknown file type '{file_type}'"
    
    url_column, download_url_column = columns
    cur.execute(f"""
        UPDATE departments_wrglfile 
        SET {url_column} = %s, {download_url_column} = %s 
        WHERE slug = %s
        RETURNING id
    """, (url, url, slug))
    
    return bool(cur.fetchone()), None

def update_wrgl_urls():
    stats = {'success': 0, 'error': 0, 'skipped': 0}
    type_stats: Dict[str, int] = {}
    
    try:
        # Load URLs
        with open('csv_urls.json', 'r') as f:
            urls = json.load(f)
        
        # Process database updates
        with get_db_connection() as conn, conn.cursor() as cur:
            # Get existing slugs
            cur.execute("SELECT slug FROM departments_wrglfile")
            all_slugs = {row[0] for row in cur.fetchall()}
            
            # Process each file
            for filename, url in urls.items():
                success, error_msg = process_file(cur, filename, url, all_slugs)
                
                if success:
                    stats['success'] += 1
                    file_type = re.match(r'([a-z]+)_', filename).group(1)
                    type_stats[file_type] = type_stats.get(file_type, 0) + 1
                else:
                    if "No matching record" in error_msg:
                        stats['skipped'] += 1
                    else:
                        stats['error'] += 1
                    print(f"Error processing {filename}: {error_msg}")
            
            conn.commit()
        
        # Print summary
        print(f"\nUpdate completed:")
        print(f"- Success: {stats['success']}")
        print(f"- Skipped: {stats['skipped']}")
        print(f"- Errors: {stats['error']}")
        print("\nFiles by type:")
        for file_type, count in type_stats.items():
            print(f"- {file_type}: {count}")
        
        if stats['error'] > 0:
            raise Exception(f"Encountered {stats['error']} errors during update")
            
    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise

if __name__ == '__main__':
    update_wrgl_urls()
