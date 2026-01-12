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

def get_or_create_connection(session_id: str = "default", database: str = None) -> Any:
    """Get existing connection or create new one for session."""
    if session_id not in _session_connections or _session_connections[session_id].is_closed():
        creds = get_snowflake_credentials()
        if database:
            creds['database'] = database
        _session_connections[session_id] = snowflake.connector.connect(**creds)
    return _session_connections[session_id]

def close_session(session_id: str = "default"):
    """Close session connection."""
    if session_id in _session_connections:
        _session_connections[session_id].close()
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

def execute_snowflake_sql(sql: str, session_id: str = "default", **kwargs) -> Dict[str, Any]:
    """Execute a SQL query in Snowflake directly."""
    logger.info(f"Executing Snowflake SQL (session: {session_id}): {sql}")
    
    timeout = kwargs.get('timeout', TIMEOUT)
    database = kwargs.get('database', None)
    start_time = time.time()
    
    try:
        conn = get_or_create_connection(session_id, database)
        cursor = conn.cursor()
        cursor.execute(sql)
        content = _format_sql_result(cursor)
    except (ProgrammingError, DatabaseError) as e:
        content = f"SQL Error: {str(e)}"
        logger.error(f"Snowflake SQL error: {str(e)}")
    except Exception as e:
        content = f"Unexpected error: {str(e)}"
        logger.error(f"Unexpected error executing Snowflake query: {str(e)}")
    finally:
        execution_time = time.time() - start_time
        logger.info(f"Execution completed in {execution_time:.2f} seconds")
    
    return {
        "content": f"EXECUTION RESULT of [execute_snowflake_sql]:\n{content}"
    }

def execute_sql_step(sql: str, step_name: Optional[str] = None, session_id: str = "default", **kwargs) -> Dict[str, Any]:
    """Execute a sub-SQL as part of iterative decomposition."""
    logger.info(f"Executing SQL step (session: {session_id}, step: {step_name}): {sql}")
    
    # Auto-wrap SELECT as temp table creation if step_name provided
    if step_name and sql.strip().upper().startswith("SELECT"):
        sql = f"CREATE OR REPLACE TEMP TABLE {step_name} AS\n{sql}"
    
    timeout = kwargs.get('timeout', TIMEOUT)
    database = kwargs.get('database', None)
    start_time = time.time()
    
    try:
        conn = get_or_create_connection(session_id, database)
        cursor = conn.cursor()
        cursor.execute(sql)
        content = _format_sql_result(cursor)
    except (ProgrammingError, DatabaseError) as e:
        content = f"SQL Error: {str(e)}"
        logger.error(f"Snowflake SQL error: {str(e)}")
    except Exception as e:
        content = f"Unexpected error: {str(e)}"
        logger.error(f"Unexpected error executing Snowflake query: {str(e)}")
    finally:
        execution_time = time.time() - start_time
        logger.info(f"Execution completed in {execution_time:.2f} seconds")
    
    return {
        "content": f"EXECUTION RESULT of [execute_sql_step]:\n{content}"
    }

def register_tools(registry):
    registry.register_tool("execute_snowflake_sql", execute_snowflake_sql)
    registry.register_tool("execute_sql_step", execute_sql_step)
    registry.register_tool("close_session", close_session)
