import os
import sys
import logging
import yaml
import pandas as pd
import pyodbc
import paramiko
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- LOGGING ---
log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"etl_master_{datetime.now().strftime('%Y%m%d')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
)

# --- DB CONNECTION via SQLAlchemy (fixes pandas warning) ---
def get_db_engine(server, port, database):
    """Creates a SQLAlchemy engine using Kerberos (Trusted_Connection)."""
    conn_str = (
        f"mssql+pyodbc://@{server},{port}/{database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
        f"&Trusted_Connection=yes"
        f"&Encrypt=yes"
        f"&TrustServerCertificate=yes"
    )
    return create_engine(conn_str, fast_executemany=True)

# --- EXTRACTION ---
def process_extraction(task, sql_conf, sftp_client, remote_dir):
    """Handles extraction and SFTP upload for a single table."""
    db_name    = task['database']
    table_name = task['table']
    file_name  = f"{task['file_prefix']}_{datetime.now().strftime('%Y%m%d')}.csv"
    local_path = os.path.join('/tmp', file_name)          # ✅ local temp file
    remote_path = f"{remote_dir}/{file_name}"             # ✅ full remote path

    logging.info(f"--- Starting Task: {db_name}.{table_name} ---")

    try:
        # Extract using SQLAlchemy engine
        engine = get_db_engine(sql_conf['host'], sql_conf['port'], db_name)
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)

        # Clean strings
        df = df.apply(lambda col: col.map(
            lambda x: x.strip() if isinstance(x, str) else x
        ))

        # Save locally to /tmp
        df.to_csv(local_path, index=False, encoding='utf-8-sig')
        logging.info(f"Extracted {len(df)} rows → {local_path}")

        # Upload to SFTP
        sftp_client.put(local_path, remote_path)          # ✅ local_path → remote_path
        logging.info(f"Uploaded → SFTP: {remote_path}")

        return True

    except Exception as e:
        logging.error(f"Failed task {db_name}.{table_name}: {e}", exc_info=True)
        return False

    finally:
        if os.path.exists(local_path):                    # ✅ always clean up
            os.remove(local_path)

# --- MAIN ---
def main():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'settings.yaml')
    try:
        with open(config_path) as f:
            conf = yaml.safe_load(f)
    except Exception as e:
        logging.critical(f"Config load failed: {e}")
        sys.exit(1)

    # Guard: ensure Kerberos ticket exists before attempting SQL
    if os.system("klist -s") != 0:
        logging.critical("No valid Kerberos ticket. Run kinit first.")
        sys.exit(1)

    # Setup SFTP
    sftp, transport = None, None
    try:
        transport = paramiko.Transport((conf['sftp']['host'], conf['sftp']['port']))
        transport.connect(
            username=os.getenv('SFTP_USER'),
            password=os.getenv('SFTP_PASS')
        )
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info("SFTP connection established.")
    except Exception as e:
        logging.critical(f"SFTP connection failed: {e}")
        sys.exit(1)

    # Run all extractions
    tasks = conf.get('extractions', [])
    success_count = sum(
        process_extraction(task, conf['sql_connection'], sftp, conf['sftp']['remote_dir'])
        for task in tasks
    )

    logging.info(f"ETL complete: {success_count}/{len(tasks)} succeeded.")
    sftp.close()
    transport.close()

    if success_count < len(tasks):
        sys.exit(1)

if __name__ == "__main__":
    main()
