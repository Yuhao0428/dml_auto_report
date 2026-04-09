A professional README is the most important part of a hand-over. It ensures that even a year from now, someone can step in and fix a bug or add a database without needing you in the room.

Here is the finalized, high-standard `README.md` for your project.

---

# **Project: MediaFacts Multi-Database ETL Pipeline**

## **1. Overview**
This project provides a robust, automated ETL (Extract, Transform, Load) pipeline designed to extract data from multiple Microsoft SQL Server databases and transfer them to a remote SFTP server. 

### **Key Features:**
* **Multi-Database Support:** Dynamically iterates through multiple databases and tables defined in a configuration file.
* **Kerberos Authentication:** Uses Active Directory (Kerberos) for secure, passwordless SQL Server access.
* **Automation-Ready:** Utilizes a Keytab for 100% autonomous operation (no manual password entry).
* **Detailed Logging:** Maintains daily logs for audit trails and troubleshooting.
* **Memory Efficient:** Processes tables locally and cleans up temporary files after successful transmission.

---

## **2. Architecture Flow**
1.  **Trigger:** A Linux Cron job executes the `run_etl.sh` wrapper.
2.  **Auth:** The wrapper uses a **Keytab** to initialize a Kerberos ticket (`kinit`).
3.  **Process:** The Python script reads `settings.yaml` and iterates through the defined tasks.
4.  **Extract:** Data is pulled from SQL Server via `pyodbc` and saved as a CSV.
5.  **Load:** CSV files are uploaded to the SFTP server via `paramiko`.
6.  **Cleanup:** Local CSV files and Kerberos tickets are destroyed to maintain security and disk space.

---

## **3. Prerequisites**
The following must be installed on the Ubuntu server:
* **System Packages:** `krb5-user`, `unixodbc-dev`, `python3-venv`.
* **SQL Driver:** [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server).
* **Network:** Access to Port `1437` (SQL) and Port `22` (SFTP).

---

## **4. Installation & Setup**

### **Step 1: Clone and Environment Setup**
```bash
# Navigate to the project root
cd /home/administrator/mediafacts_etl

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### **Step 2: Generate the Kerberos Keytab**
To allow the script to run without a human typing a password, you must generate a Keytab file for your service account/user.
```bash
ktutil
addent -password -p wangyu@SERVICEPLAN.DE -k 1 -e aes256-cts
# [Enter Password when prompted]
wkt wangyu.keytab
quit

# Set strict permissions
chmod 600 wangyu.keytab
```

### **Step 3: Configure Credentials**
Create a `.env` file in the root directory:
```bash
SFTP_USER=your_sftp_username
SFTP_PASS=your_sftp_password
```

---

## **5. Configuration Management**
To update the tables being extracted or change server details, edit `config/settings.yaml`.

| Field | Description |
| :--- | :--- |
| `sql_connection` | Global host and port for the SQL instance. |
| `extractions` | A list of tasks. Add a new entry for every table needed. |
| `database` | The specific SQL database name. |
| `table` | Full table name (e.g., `dbo.my_table`). |
| `file_prefix` | The name the file will have on the SFTP (e.g., `tv_data`). |

---

## **6. Operation & Maintenance**

### **Manual Execution**
To test the pipeline manually:
```bash
./run_etl.sh
```

### **Automation (Deployment)**
The project is designed to run via Cron. Add the following entry to `crontab -e`:
```text
# Run at 2:00 AM every day
00 02 * * * /bin/bash /home/administrator/mediafacts_etl/run_etl.sh >> /home/administrator/mediafacts_etl/logs/cron_error.log 2>&1
```

### **Monitoring**
* **Success/Failure logs:** Located in `logs/etl_YYYYMMDD.log`.
* **Cron errors:** Check `logs/cron_error.log`.

---

## **7. Troubleshooting**

* **Login Timeout / Error 0x102:** The firewall is blocking Port 1437. Check connectivity with `nc -zv spm1ml1.serviceplan.de 1437`.
* **Kinit Failures:** Ensure the Keytab file exists and the password hasn't changed.
* **Permission Denied (SQL):** Ensure the `wangyu` AD account has `SELECT` permissions on the target database/table.
* **Disk Full:** Check `logs/` or check if `temp_data` files were not deleted due to a crash.

---

## **8. Project Structure**
```text
mediafacts_etl/
├── config/
│   └── settings.yaml    # Task metadata (Add/Remove tables here)
├── src/
│   └── main.py          # Core ETL logic
├── logs/                # Daily log files
├── .env                 # SFTP credentials (ignored by Git)
├── wangyu.keytab        # Encryption key for Kerberos
├── run_etl.sh           # Main execution wrapper
├── requirements.txt     # Python library dependencies
└── README.md            # You are here
```

---
**Owner:** Yuhao Wang 
**Last Updated:** 2026-04-09
