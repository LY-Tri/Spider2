#!/bin/bash
# Snowflake Environment Variables Setup Script
# Usage: source set_snowflake_env.sh
#
# Loads credentials dynamically from snowflake.key

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KEY_FILE="${SCRIPT_DIR}/snowflake.key"

if [[ ! -f "$KEY_FILE" ]]; then
    echo "Error: $KEY_FILE not found"
    return 1
fi

# Load variables from key file
while IFS='=' read -r key value; do
    # Skip empty lines and comments
    [[ -z "$key" || "$key" =~ ^# ]] && continue
    export "$key"="$value"
done < "$KEY_FILE"

echo "Snowflake environment variables set from $KEY_FILE:"
echo "  SNOWFLAKE_USER=$SNOWFLAKE_USER"
echo "  SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT"
echo "  SNOWFLAKE_ROLE=$SNOWFLAKE_ROLE"
echo "  SNOWFLAKE_WAREHOUSE=$SNOWFLAKE_WAREHOUSE"
echo "  SNOWFLAKE_PASSWORD=****"