import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError
import pandas as pd
from typing import Dict, Any
import logging
import time
import os

logger = logging.getLogger(__name__)

TIMEOUT = 300  # 5 minutes for complex queries
MAX_CSV_CHARS = 2000

def get_snowflake_credentials() -> Dict[str, str]:
    """Load Snowflake credentials from environment variables."""
    return {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "role": os.environ.get("SNOWFLAKE_ROLE", "PARTICIPANT"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH_PARTICIPANT"),
    }

def execute_snowflake_sql(sql: str, **kwargs) -> Dict[str, Any]:
    logger.info(f"Executing Snowflake SQL: {sql}")
    
    timeout = kwargs.get('timeout', TIMEOUT)
    database = kwargs.get('database', None)  # Optional database context
    start_time = time.time()
    
    content = ""
    
    conn = None
    try:
        # Get Snowflake credentials from file
        snowflake_credential = get_snowflake_credentials()
        
        # Add database to connection if specified
        if database:
            snowflake_credential = {**snowflake_credential, 'database': database}
        
        # Connect to Snowflake using credentials
        conn = snowflake.connector.connect(
            **snowflake_credential,
            login_timeout=timeout,
            network_timeout=timeout
        )
        cursor = conn.cursor()
        
        # Execute SQL query
        cursor.execute(sql)
        
        # First print success message
        print("Query executed successfully")
        
        # Fetch results if the query returns data
        if cursor.description:
            headers = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=headers)
                
                # Convert full dataset to CSV
                full_csv_data = df.to_csv(index=False)
                total_rows = len(df)
                
                # Check if we need to truncate by character length
                if len(full_csv_data) > MAX_CSV_CHARS:
                    # Truncate to MAX_CSV_CHARS characters
                    truncated_csv = full_csv_data[:MAX_CSV_CHARS]
                    
                    # Find the last complete line to avoid cutting in the middle
                    last_newline = truncated_csv.rfind('\n')
                    if last_newline > 0:
                        truncated_csv = truncated_csv[:last_newline]
                    
                    content = f"""Query executed successfully

```csv
{truncated_csv}
```

Note: The result has been truncated to {MAX_CSV_CHARS} characters for display purposes. The complete result set contains {total_rows} rows and {len(full_csv_data)} characters."""
                else:
                    content = f"""Query executed successfully

```csv
{full_csv_data}
```"""
            else:
                content = "Query executed successfully, but no rows returned."
        else:
            conn.commit()
            content = "Query executed successfully."
        
        
    except ProgrammingError as e:
        content = f"SQL Error: {str(e)}"
        logger.error(f"Snowflake SQL error: {str(e)}")
    except DatabaseError as e:
        content = f"Database error: {str(e)}"
        logger.error(f"Snowflake database error: {str(e)}")
    except TimeoutError:
        content = f"Execution timed out after {timeout} seconds."
        logger.error(f"Snowflake query timed out: {sql}")
    except Exception as e:
        content = f"Unexpected error: {str(e)}"
        logger.error(f"Unexpected error executing Snowflake query: {str(e)}")
    finally:
        if conn:
            conn.close()
            
        # Log execution time
        execution_time = time.time() - start_time
        logger.info(f"Execution completed in {execution_time:.2f} seconds")
    
    return {
        "content": f"EXECUTION RESULT of [execute_snowflake_sql]:\n{content}"
    }

def execute_snowflake_sql_batch(queries: list, **kwargs) -> list:
    """
    Execute multiple SQL queries in a single connection.
    Useful for creating temp tables that need to persist across queries.
    
    Args:
        queries: List of SQL strings to execute in order
        database: Optional database to use
        schema: Optional schema to use (default: same as database, or "PUBLIC")
        timeout: Connection timeout (default 300s)
    
    Returns:
        List of result dicts, one per query
    """
    timeout = kwargs.get('timeout', TIMEOUT)
    database = kwargs.get('database', None)
    schema = kwargs.get('schema', None)
    
    results = []
    conn = None
    
    try:
        snowflake_credential = get_snowflake_credentials()
        if database:
            snowflake_credential = {**snowflake_credential, 'database': database}
            # Default schema to database name if not specified (common pattern)
            if not schema:
                schema = database
        if schema:
            snowflake_credential = {**snowflake_credential, 'schema': schema}
        
        conn = snowflake.connector.connect(
            **snowflake_credential,
            login_timeout=timeout,
            network_timeout=timeout
        )
        cursor = conn.cursor()
        
        for sql in queries:
            try:
                cursor.execute(sql)
                
                if cursor.description:
                    headers = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    if rows:
                        df = pd.DataFrame(rows, columns=headers)
                        csv_data = df.to_csv(index=False)
                        if len(csv_data) > MAX_CSV_CHARS:
                            truncated = csv_data[:MAX_CSV_CHARS]
                            last_nl = truncated.rfind('\n')
                            if last_nl > 0:
                                truncated = truncated[:last_nl]
                            content = f"Query executed successfully\n\n```csv\n{truncated}\n```\n\nNote: Truncated to {MAX_CSV_CHARS} chars. Total: {len(rows)} rows."
                        else:
                            content = f"Query executed successfully\n\n```csv\n{csv_data}```"
                    else:
                        content = "Query executed successfully, but no rows returned."
                else:
                    conn.commit()
                    content = "Query executed successfully."
                
                results.append({"content": f"EXECUTION RESULT of [execute_snowflake_sql]:\n{content}", "success": True})
                
            except (ProgrammingError, DatabaseError) as e:
                results.append({"content": f"EXECUTION RESULT of [execute_snowflake_sql]:\nSQL Error: {str(e)}", "success": False})
                break  # Stop on error
                
    except Exception as e:
        results.append({"content": f"EXECUTION RESULT of [execute_snowflake_sql]:\nConnection error: {str(e)}", "success": False})
    finally:
        if conn:
            conn.close()
    
    return results


def register_tools(registry):
    registry.register_tool("execute_snowflake_sql", execute_snowflake_sql)