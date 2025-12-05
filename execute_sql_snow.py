import os
import json
import math
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import argparse
import snowflake.connector
import sqlglot
from sqlglot import exp

from decompose import SQLDecomposer


def get_snowflake_credentials():
    """Load Snowflake credentials from environment variables."""
    required = ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT"]
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    return {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "role": os.environ.get("SNOWFLAKE_ROLE", "PARTICIPANT"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH_PARTICIPANT"),
    }



def compare_pandas_table(pred, gold, condition_cols=[], ignore_order=False):
    """_summary_

    Args:
        pred (Dataframe): _description_
        gold (Dataframe): _description_
        condition_cols (list, optional): _description_. Defaults to [].
        ignore_order (bool, optional): _description_. Defaults to False.

    """
    print('condition_cols', condition_cols)
    
    tolerance = 1e-2

    def vectors_match(v1, v2, tol=tolerance, ignore_order_=False):
        if ignore_order_:
            v1, v2 = (sorted(v1, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))),
                    sorted(v2, key=lambda x: (x is None, str(x), isinstance(x, (int, float)))))
        if len(v1) != len(v2):
            return False
        for a, b in zip(v1, v2):
            if pd.isna(a) and pd.isna(b):
                continue
            elif isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if not math.isclose(float(a), float(b), abs_tol=tol):
                    return False
            elif a != b:
                return False
        return True
    
    if condition_cols != []:
        gold_cols = gold.iloc[:, condition_cols]
    else:
        gold_cols = gold
    pred_cols = pred
    
    t_gold_list = gold_cols.transpose().values.tolist()
    t_pred_list = pred_cols.transpose().values.tolist()
    score = 1
    for _, gold in enumerate(t_gold_list):
        if not any(vectors_match(gold, pred, ignore_order_=ignore_order) for pred in t_pred_list):
            score = 0
        else:
            for j, pred in enumerate(t_pred_list):
                if vectors_match(gold, pred, ignore_order_=ignore_order):
                    break

    return score


def execute_sql_to_dataframe(sql_query: str, database: str, timeout: int,  instance_id: str = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute SQL and return DataFrame or error message.
    Uses the evaluation suite's connection approach.
    """
    credentials = get_snowflake_credentials()
    connection_kwargs = {k: v for k, v in credentials.items() if k != "session_parameters"}
    session_parameters = credentials.get("session_parameters", {}).copy()
    session_parameters["STATEMENT_TIMEOUT_IN_SECONDS"] = timeout
    connection_kwargs["session_parameters"] = session_parameters
    
    conn = None
    prefix = f"[{instance_id}] " if instance_id else ""
    
    try:
        conn = snowflake.connector.connect(
            database=database,
            **connection_kwargs
        )
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results, columns=columns)
        
        if df.empty:
            return None, "No data returned"
        
        return df, None
        
    except snowflake.connector.errors.ProgrammingError as e:
        error_message = str(e)
        if "STATEMENT_TIMEOUT" in error_message or "SQL execution canceled" in error_message:
            return None, f"Query timed out after {timeout} seconds"
        return None, f"SQL Error: {error_message}"
    except Exception as e:
        return None, f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()


def execute_batch_sql_to_dataframe(queries: List[str],database: str, schema: Optional[str], timeout: int, instance_id: str = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Execute multiple SQL queries in a single connection (for temp tables).
    Returns the result of the final query.
    """
    credentials = get_snowflake_credentials()
    connection_kwargs = {k: v for k, v in credentials.items() if k != "session_parameters"}
    session_parameters = credentials.get("session_parameters", {}).copy()
    session_parameters["STATEMENT_TIMEOUT_IN_SECONDS"] = timeout
    connection_kwargs["session_parameters"] = session_parameters
    
    conn = None
    prefix = f"[{instance_id}] " if instance_id else ""
    
    try:
        # Set database and schema for temp table creation
        conn = snowflake.connector.connect(
            database=database,
            schema=schema if schema else database,  # Default schema to database name
            **connection_kwargs
        )
        cursor = conn.cursor()
        
        # Execute all queries, return result from last one
        final_df = None
        for i, sql in enumerate(queries):
            cursor.execute(sql)
            
            if cursor.description:
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                final_df = pd.DataFrame(results, columns=columns)
        
        if final_df is None or final_df.empty:
            return None, "No data returned"
        
        return final_df, None
        
    except snowflake.connector.errors.ProgrammingError as e:
        error_message = str(e)
        if "STATEMENT_TIMEOUT" in error_message or "SQL execution canceled" in error_message:
            return None, f"Query timed out after {timeout} seconds"
        return None, f"SQL Error: {error_message}"
    except Exception as e:
        return None, f"Error: {str(e)}"
    finally:
        if conn:
            conn.close()


def extract_schema_from_sql(sql: str, database: str, dialect: str = "snowflake") -> Optional[str]:
    """
    Extract the schema from fully qualified table references in SQL.
    Table references like DATABASE.SCHEMA.TABLE.
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
        
        for table in parsed.find_all(exp.Table):
            table_db = table.db  # Schema in Snowflake's 3-part naming
            table_catalog = table.catalog  # Database
            
            if table_catalog and table_db:
                catalog_clean = table_catalog.strip('"').upper()
                db_clean = database.upper()
                if catalog_clean == db_clean:
                    return table_db.strip('"')
        
        return None
    except Exception:
        return None


def execute_decomposed_sql(sql: str, database: str, timeout: int, instance_id: str = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Decompose SQL and execute each step.
    """
    try:
        decomposer = SQLDecomposer(sql, dialect="snowflake")
        queries = decomposer.queries
        
        if not queries:
            return None, "No queries to execute"
        
        # If only one query (no decomposition needed), execute directly
        if len(queries) == 1:
            return execute_sql_to_dataframe(
                queries[0].sql, 
                database, 
                timeout=timeout,
                instance_id=instance_id
            )
        
        # Extract schema for temp table creation
        schema = extract_schema_from_sql(sql, database)
        
        sql_statements = [q.sql for q in queries]
        return execute_batch_sql_to_dataframe(
            sql_statements,
            database,
            schema=schema,
            timeout=timeout,
            instance_id=instance_id
        )
        
    except Exception as e:
        return None, f"Decomposition error: {str(e)}"


def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[bool, str]:
    """
    Compare two DataFrames using the evaluation suite's comparison logic.
    Returns (match: bool, reason: str)
    """
    if df1 is None and df2 is None:
        return True, "both_empty"
    
    if df1 is None or df2 is None:
        return False, "one_empty"
    
    # Use evaluation suite's comparison (tolerant to order and small numeric differences)
    try:
        score = compare_pandas_table(df1, df2, condition_cols=[], ignore_order=True)
        if score == 1:
            return True, "match"
        else:
            return False, "value_mismatch"
    except Exception as e:
        return False, f"comparison_error: {str(e)}"


def process_single_query(query: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    """Process a single query: run original and decomposed, compare results."""
    instance_id = query.get("instance_id", "unknown")
    sql = query.get("sql", "")
    database = query.get("db_id", "")
    
    result = {
        "instance_id": instance_id,
        "database": database,
        "match": False,
        "reason": "",
        "original_error": None,
        "decomposed_error": None,
    }
    
    try:
        # Execute original SQL
        df_original, original_error = execute_sql_to_dataframe(
            sql, database, timeout=timeout, instance_id=instance_id
        )
        
        if original_error:
            result["original_error"] = original_error[:200]
        
        # Execute decomposed SQL
        df_decomposed, decomposed_error = execute_decomposed_sql(
            sql, database, timeout=timeout, instance_id=instance_id
        )
        
        if decomposed_error:
            result["decomposed_error"] = decomposed_error[:200]
        
        # Compare results
        if df_original is None and original_error:
            result["reason"] = "original_error"
            result["match"] = False
        elif df_decomposed is None and decomposed_error:
            result["reason"] = "decomposed_error"
            result["match"] = False
        else:
            match, reason = compare_dataframes(df_original, df_decomposed)
            result["match"] = match
            result["reason"] = reason
            
    except Exception as e:
        result["reason"] = f"exception: {str(e)}"
    
    return result


def run_parallel(sql_data: List[Dict], max_workers: int, timeout: int) -> List[Dict]:
    """Run all queries in parallel using ThreadPoolExecutor."""
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_query = {
            executor.submit(process_single_query, query, timeout): query 
            for query in sql_data
        }
        
        with tqdm(total=len(sql_data), desc="Processing queries") as pbar:
            for future in as_completed(future_to_query):
                result = future.result()
                results.append(result)
                pbar.update(1)
                
                status = "✓" if result["match"] else "✗"
                pbar.set_postfix_str(f"{result['instance_id']}: {status}")
    
    return results


def print_summary(results: List[Dict]):
    """Print summary of results."""
    total = len(results)
    matches = sum(1 for r in results if r["match"])
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total queries: {total}")
    print(f"Matches:       {matches} ({100*matches/total:.1f}%)")
    print(f"Mismatches:    {total - matches} ({100*(total-matches)/total:.1f}%)")
    
    # Group by reason
    reasons = {}
    for r in results:
        reason = r["reason"]
        reasons[reason] = reasons.get(reason, 0) + 1
    
    print("\nBreakdown by reason:")
    for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"  {reason}: {count}")
    
    # Show first few failures
    failures = [r for r in results if not r["match"]]
    if failures:
        print("\nFirst 5 failures:")
        for r in failures[:5]:
            print(f"  - {r['instance_id']}: {r['reason']}")
            if r.get("decomposed_error"):
                print(f"    Error: {r['decomposed_error'][:100]}...")


def main():
    parser = argparse.ArgumentParser(description="Parallel SQL decomposition verification")
    parser.add_argument("--data", default="df_sql.json",
                        help="Path to SQL data JSON file")
    parser.add_argument("--workers", type=int, default=32,
                        help="Number of parallel workers (default: 32)")
    parser.add_argument("--timeout", type=int, default=120, 
                        help="SQL execution timeout in seconds (default: 120)")
    parser.add_argument("--output", default=None,
                        help="Output file for detailed results (JSON)")
    args = parser.parse_args()
    
    # Load SQL data
    print(f"Loading SQL data from {args.data}...")
    with open(args.data, "r") as f:
        sql_data = json.load(f)
    
    print(f"Loaded {len(sql_data)} queries")
    print(f"Using {args.workers} parallel workers")
    print()
    
    # Run parallel processing
    results = run_parallel(sql_data, max_workers=args.workers, timeout=args.timeout)
    
    # Print summary
    print_summary(results)
    
    # Save detailed results if requested
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nDetailed results saved to {args.output}")


if __name__ == "__main__":
    main()

