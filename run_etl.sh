#!/bin/bash
set -euo pipefail

PROJECT_DIR="/home/wangyu/projects/dml_auto_report"
PRINCIPAL="wangyu@SERVICEPLAN.DE"
KEYTAB="$PROJECT_DIR/wangyu.keytab"

# ✅ Explicitly set PATH for cron environment
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export KRB5_CONFIG="/etc/krb5.conf"

cd "$PROJECT_DIR"

echo "=== ETL Start: $(date) ==="

# 1. Verify keytab exists
if [[ ! -f "$KEYTAB" ]]; then
    echo "ERROR: Keytab not found at $KEYTAB"
    exit 1
fi

# 2. Kerberos login
echo "Authenticating as $PRINCIPAL..."
if ! kinit -kt "$KEYTAB" "$PRINCIPAL"; then
    echo "ERROR: kinit failed."
    exit 1
fi
klist

# 3. Run ETL inside virtualenv
source "$PROJECT_DIR/venv/bin/activate"
python3 "$PROJECT_DIR/src/main.py"
ETL_EXIT=$?

# 4. Cleanup
kdestroy
echo "=== ETL End: $(date) | Exit: $ETL_EXIT ==="

exit $ETL_EXIT
