#!/bin/bash
# Configuration
PROJECT_DIR="/home/administrator/dml_auto_report"
PRINCIPAL="wangyu@SERVICEPLAN.DE"
KEYTAB="$PROJECT_DIR/wangyu.keytab"

cd $PROJECT_DIR

# 1. Automated Kerberos Login
kinit $PRINCIPAL -kt $KEYTAB

# 2. Run ETL
source venv/bin/activate
python3 src/main.py

# 3. Cleanup ticket
kdestroy
