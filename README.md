# **Project: DML  Multi-Database ETL Pipeline**

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

Before running anything, make sure the following are set up on your Ubuntu server.

### **3.1 System Packages**

These are OS-level dependencies. Install them with `apt`:

```bash
sudo apt-get update
sudo apt-get install -y \
    krb5-user \        # Kerberos client (for kinit, ktutil, kdestroy)
    unixodbc-dev \     # ODBC header files needed to compile pyodbc
    python3-venv \     # Lets you create isolated Python environments
    python3-pip        # Python package installer
```

> **Note:** During `krb5-user` installation, Ubuntu will ask for your default Kerberos realm. Enter your Active Directory domain in uppercase (e.g. `SERVICEPLAN.DE`). You can also leave it blank and configure `/etc/krb5.conf` manually later.

---

### **3.2 Microsoft ODBC Driver 18 for SQL Server**

This driver is **not** in the standard Ubuntu repositories — install it from Microsoft's package feed:

```bash
# Import Microsoft's signing key and package source
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list \
    | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Install the driver (ACCEPT_EULA is required)
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Verify the driver is registered
odbcinst -q -d -n "ODBC Driver 18 for SQL Server"
```

Full reference: [Microsoft ODBC Driver 18 for SQL Server (Linux)](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)

---

### **3.3 Network Access**

Ensure the server can reach the following ports — ask your network/firewall team if unsure:

| Port | Protocol | Purpose |
|------|----------|---------|
| `1437` | TCP | MS SQL Server |
| `22` | TCP | SFTP file transfer |

You can test connectivity with:
```bash
nc -zv your-sql-server-hostname 1437
nc -zv your-sftp-server-hostname 22
```

---

---

## **4. Installation & Setup**

### **Step 1: Clone and Environment Setup**
```bash
# Navigate to the project root
cd /home/administrator/dml_auto_report

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### **Step 2: Generate the Kerberos Keytab**
To allow the script to run without a human typing a password, you must generate a Keytab file for your service account/user.
```bash

# Destroy current ticket first
kdestroy

# Remove old broken keytab
rm wangyu.keytab

# Regenerate with correct salt
ktutil
addent -password -p wangyu@SERVICEPLAN.DE -k 1 -e aes256-cts-hmac-sha1-96 -s SERVICEPLAN.DEyu.wang
# [Enter wangyu's password]
wkt wangyu.keytab
quit

chmod 600 wangyu.keytab

# Verify it works non-interactively
kinit -kt wangyu.keytab wangyu@SERVICEPLAN.DE
klist

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
00 02 * * * /bin/bash /home/administrator/dml_auto_report/run_etl.sh >> /home/administrator/dml_auto_report/logs/cron_error.log 2>&1
```

Save and verify it was added:
```bash
crontab -l
```

### **Monitoring**
* **Success/Failure logs:** Located in `logs/etl_YYYYMMDD.log`.
* **Cron errors:** Check `logs/cron_error.log`.

Log Rotation (optional but recommended)
Without this, cron.log will grow forever. Create /etc/logrotate.d/dml_auto_report:

```bash
sudo nano /etc/logrotate.d/dml_auto_report
```

```text
/home/administrator/compose/dml_auto_report/logs/*.log {
    daily
    rotate 30
    compress
    missingok
    notifempty
    dateext
}
```

#### Verify It Works
```text
# Test the script runs cleanly end-to-end right now
bash /home/administrator/compose/dml_auto_report/run_etl.sh

# Check cron is running
systemctl status cron

# After 6am tomorrow, check the log
cat /home/administrator/compose/dml_auto_report/logs/cron.log

```

---

## **7. Troubleshooting**

* **Login Timeout / Error 0x102:** The firewall is blocking Port 1437. Check connectivity with `nc -zv spm1ml1.serviceplan.de 1437`.
* **Kinit Failures:** Ensure the Keytab file exists and the password hasn't changed.
* **Permission Denied (SQL):** Ensure the `wangyu` AD account has `SELECT` permissions on the target database/table.
* **Disk Full:** Check `logs/` or check if `temp_data` files were not deleted due to a crash.

---

## **8. Project Structure**
```text
dml_auto_report/
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
