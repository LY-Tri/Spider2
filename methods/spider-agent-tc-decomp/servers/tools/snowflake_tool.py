import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError
import pandas as pd
from typing import Dict, Any, Optional
import logging
import time
import os

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
if os.path.exists("../../snowflake.key"):
    load_dotenv("../../snowflake.key")
else:
    raise FileNotFoundError("snowflake.key not found")

TIMEOUT = 300  # 5 minutes for complex queries
MAX_CSV_CHARS = 2000

# Session connection pool
_session_connections = {}

def get_snowflake_credentials() -> Dict[str, str]:
    """Load Snowflake credentials from environment variables."""
    return {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "role": os.environ.get("SNOWFLAKE_ROLE", "PARTICIPANT"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH_PARTICIPANT"),
    }

def close_session(session_id: str = "default"):
    """Close session connection."""
    if session_id in _session_connections:
        try:
            _session_connections[session_id].close()
        except:
            pass
        del _session_connections[session_id]

def _format_sql_result(cursor: Any) -> str:
    """Helper to format SQL execution results."""
    if cursor.description:
        headers = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=headers)
            full_csv_data = df.to_csv(index=False)
            total_rows = len(df)
            
            if len(full_csv_data) > MAX_CSV_CHARS:
                truncated_csv = full_csv_data[:MAX_CSV_CHARS]
                last_newline = truncated_csv.rfind('\n')
                if last_newline > 0:
                    truncated_csv = truncated_csv[:last_newline]
                
                return f"""Query executed successfully

```csv
{truncated_csv}
```

Note: The result has been truncated to {MAX_CSV_CHARS} characters for display purposes. The complete result set contains {total_rows} rows and {len(full_csv_data)} characters."""
            else:
                return f"""Query executed successfully

```csv
{full_csv_data}
```"""
        else:
            return "Query executed successfully, but no rows returned."
    else:
        return "Query executed successfully."

def get_or_create_connection(session_id: str, database: str = None) -> Any:
    """Get existing connection or create new one for session."""
    if session_id in _session_connections:
        conn = _session_connections[session_id]
        if not conn.is_closed():
            if database:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(f"USE DATABASE {database}")
                except Exception as e:
                    logger.warning(f"Could not switch database to {database} for existing session {session_id}: {str(e)}")
            return conn

    creds = get_snowflake_credentials()
    if database:
        creds['database'] = database
    
    conn = snowflake.connector.connect(**creds)
    _session_connections[session_id] = conn
    return conn

def _execute_query(sql: str, session_id: str, database: str = None) -> str:
    """Internal helper to execute SQL and format results."""
    try:
        conn = get_or_create_connection(session_id, database)
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return _format_sql_result(cursor)
    except (ProgrammingError, DatabaseError) as e:
        logger.error(f"Snowflake SQL error (session: {session_id}): {str(e)}")
        return f"SQL Error: {str(e)}"
    except Exception as e:
        logger.error(f"Unexpected error (session: {session_id}): {str(e)}")
        return f"Unexpected error: {str(e)}"

def execute_snowflake_sql(sql: str = None, session_id: str = "default", **kwargs) -> Dict[str, Any]:
    """Execute a SQL query in Snowflake directly."""
    if not sql:
        return {"content": "ERROR: 'sql' is a required parameter."}
    
    logger.info(f"Executing Snowflake SQL (session: {session_id})")
    database = kwargs.get('database')
    content = _execute_query(sql, session_id, database)
    
    return {"content": f"EXECUTION RESULT of [execute_snowflake_sql]:\n{content}"}

def execute_sql_step(sql: str = None, step_name: Optional[str] = None, session_id: str = "default", **kwargs) -> Dict[str, Any]:
    """Execute a sub-SQL as part of iterative decomposition."""
    if not sql:
        return {"content": "ERROR: 'sql' is a required parameter."}
    
    # Auto-wrap SELECT as temp table creation if step_name provided
    if step_name and sql.strip().upper().startswith("SELECT"):
        sql = f"CREATE OR REPLACE TEMP TABLE {step_name} AS\n{sql}"
    
    logger.info(f"Executing SQL step (session: {session_id}, step: {step_name})")
    database = kwargs.get('database')
    content = _execute_query(sql, session_id, database)
    
    return {"content": f"EXECUTION RESULT of [execute_sql_step]:\n{content}"}

def register_tools(registry):
    registry.register_tool("execute_snowflake_sql", execute_snowflake_sql)
    registry.register_tool("execute_sql_step", execute_sql_step)
    registry.register_tool("close_session", close_session)
