import os
import logging
import yaml
import pandas as pd
import pyodbc
import paramiko
from datetime import datetime
from dotenv import load_dotenv

# --- INITIALIZATION ---
load_dotenv()
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"etl_master_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)

def get_db_connection(server, port, database):
    """Creates a connection for a specific database using Kerberos."""
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server},{port};"
        f"Database={database};"
        f"Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def process_extraction(task, sql_conf, sftp_client, remote_dir):
    """Handles the extraction and upload for a single table."""
    db_name = task['database']
    table_name = task['table']
    file_name = f"{task['file_prefix']}_{datetime.now().strftime('%Y%m%d')}.csv"
    
    logging.info(f"--- Starting Task: {db_name}.{table_name} ---")
    
    try:
        # Extraction
        with get_db_connection(sql_conf['host'], sql_conf['port'], db_name) as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
            
            # Clean data (standard SQL Server string trimming)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            
            df.to_csv(file_name, index=False)
            logging.info(f"Successfully extracted {len(df)} rows to {file_name}")

        # Upload
        remote_path = f"{remote_dir}/{file_name}"
        sftp_client.put(file_name, remote_path)
        logging.info(f"Successfully uploaded to SFTP: {remote_path}")
        
        # Cleanup local file
        os.remove(file_name)
        return True

    except Exception as e:
        logging.error(f"Failed task {db_name}.{table_name}: {str(e)}")
        if os.path.exists(file_name):
            os.remove(file_name)
        return False

def main():
    # 1. Load Config
    try:
        with open("config/settings.yaml", "r") as f:
            conf = yaml.safe_load(f)
    except Exception as e:
        logging.critical(f"Config load failed: {e}")
        return

    # 2. Setup SFTP Connection once
    try:
        transport = paramiko.Transport((conf['sftp']['host'], conf['sftp']['port']))
        transport.connect(
            username=os.getenv('SFTP_USER'), 
            password=os.getenv('SFTP_PASS')
        )
        sftp = paramiko.SFTPClient.from_transport(transport)
    except Exception as e:
        logging.critical(f"SFTP Connection failed: {e}")
        return

    # 3. Iterate through all extractions
    success_count = 0
    total_tasks = len(conf['extractions'])
    
    for task in conf['extractions']:
        if process_extraction(task, conf['sql_connection'], sftp, conf['sftp']['remote_dir']):
            success_count += 1

    # 4. Final Wrap-up
    logging.info(f"ETL Run Finished. Success: {success_count}/{total_tasks}")
    sftp.close()
    transport.close()

if __name__ == "__main__":
    main()
