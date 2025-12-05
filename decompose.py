import sqlglot
from sqlglot import exp
from graphlib import TopologicalSorter
from graphviz import Digraph
from dataclasses import dataclass
from typing import Optional
import re


@dataclass
class DecomposedQuery:
    """A single decomposed query step."""
    name: str
    sql: str
    dependencies: list[str]
    
    def __repr__(self):
        return f"DecomposedQuery(name='{self.name}', deps={self.dependencies})"


class SQLDecomposer:
    """
    Decompose complex SQL queries into sequential subqueries.
    
    Usage:
        decomposer = SQLDecomposer(sql, dialect="snowflake")
        decomposer.graph()           # Visualize dependency graph
        decomposer.queries           # List of decomposed queries
        decomposer.print_queries()   # Print all queries
    """
    
    def __init__(self, sql: str, dialect: str = "snowflake"):
        self.sql = sql
        self.dialect = dialect
        self._parsed = sqlglot.parse_one(sql, dialect=dialect)
        self._ctes: dict[str, str] = {}
        self._dependencies: dict[str, list[str]] = {}
        self._queries: list[DecomposedQuery] = []
        self._recursive_ctes: set[str] = set()  # Track recursive CTEs
        self._has_recursive = False  # Flag if any CTE is recursive
        self._skip_decomposition = False  # Flag to skip decomposition entirely
        self._skip_reason = ""  # Reason for skipping
        self._parse()
    
    def _check_skip_conditions(self) -> bool:
        """
        Check conditions that prevent decomposition entirely.
        Returns True if decomposition should be skipped.
        """
        # 1. Check for WITH RECURSIVE in original SQL (case-insensitive)
        if re.search(r'\bWITH\s+RECURSIVE\b', self.sql, re.IGNORECASE):
            self._skip_reason = "WITH RECURSIVE detected"
            return True
        
        # 2. Check for CTEs inside subqueries (nested WITH clauses)
        # This happens when we find more than one WITH clause, or WITH inside a subquery
        with_count = len(re.findall(r'\bWITH\b', self.sql, re.IGNORECASE))
        if with_count > 1:
            self._skip_reason = "Nested WITH clauses detected"
            return True
        
        # 3. Check if the parsed query has CTEs inside subqueries
        for subquery in self._parsed.find_all(exp.Subquery):
            inner = subquery.this
            if inner and hasattr(inner, 'ctes') and inner.ctes:
                self._skip_reason = "CTEs inside subquery"
                return True
        
        return False
    
    def _detect_recursive_ctes(self, cte_set: set) -> set[str]:
        """Detect CTEs that reference themselves (recursive CTEs)."""
        recursive = set()
        
        # Create a lowercase lookup for case-insensitive matching
        cte_lower = {name.lower(): name for name in cte_set}
        
        for cte in (self._parsed.ctes or []):
            cte_name = cte.alias
            cte_name_lower = cte_name.lower()
            
            # Check if CTE references itself (case-insensitive)
            for table in cte.this.find_all(exp.Table):
                table_name_lower = table.name.lower()
                if table_name_lower == cte_name_lower:
                    recursive.add(cte_name)
                    break
        
        return recursive
    
    def _parse(self):
        """Parse SQL and extract CTEs, dependencies, and decomposed queries."""
        # Check if we should skip decomposition entirely
        self._skip_decomposition = self._check_skip_conditions()
        if self._skip_decomposition:
            # Return original SQL as a single query
            self._queries.append(DecomposedQuery(
                name="FINAL_RESULT",
                sql=self.sql,
                dependencies=[]
            ))
            return
        
        # Extract CTEs
        cte_set = set()
        if self._parsed.ctes:
            for cte in self._parsed.ctes:
                self._ctes[cte.alias] = cte.this.sql(dialect=self.dialect, pretty=True)
                cte_set.add(cte.alias)
        
        # Detect recursive CTEs BEFORE building dependencies
        self._recursive_ctes = self._detect_recursive_ctes(cte_set)
        self._has_recursive = len(self._recursive_ctes) > 0
        
        # Build dependencies (excluding self-references for recursive CTEs)
        # Use case-insensitive matching for CTE names
        cte_lower_to_original = {name.lower(): name for name in cte_set}
        
        for cte in (self._parsed.ctes or []):
            cte_name = cte.alias
            deps = []
            for table in cte.this.find_all(exp.Table):
                table_lower = table.name.lower()
                # Don't add self-reference as dependency
                if table_lower in cte_lower_to_original and table_lower != cte_name.lower():
                    deps.append(cte_lower_to_original[table_lower])
            self._dependencies[cte_name] = list(set(deps))
        
        # Final query dependencies
        main_select = self._parsed.find(exp.Select)
        if main_select:
            final_deps = []
            for t in main_select.find_all(exp.Table):
                t_lower = t.name.lower()
                if t_lower in cte_lower_to_original:
                    final_deps.append(cte_lower_to_original[t_lower])
            self._dependencies["__FINAL__"] = list(set(final_deps))
        
        # Build execution order
        self._build_queries(cte_set)
    
    def _normalize_cte_references(self, sql: str, cte_set: set) -> str:
        """
        Replace all CTE references with uppercase unquoted names for consistency.
        This ensures temp table names match how they're created.
        """
        result = sql
        
        for cte_name in cte_set:
            upper_name = cte_name.upper()
            lower_name = cte_name.lower()
            
            # Replace various quoted forms: "cte_name", "CTE_NAME", "Cte_Name", etc.
            # Use case-insensitive pattern for quoted identifiers
            quoted_pattern = rf'"({re.escape(cte_name)})"'
            result = re.sub(quoted_pattern, upper_name, result, flags=re.IGNORECASE)
            
            # Also handle double quotes around exact case
            result = result.replace(f'"{cte_name}"', upper_name)
            result = result.replace(f'"{upper_name}"', upper_name)
            result = result.replace(f'"{lower_name}"', upper_name)
            
            # Replace unquoted references with proper case (word boundary aware)
            # Match the CTE name as a whole word, not inside quotes
            # Be careful not to match inside strings or already-processed identifiers
            pattern = rf'(?<!["\w]){re.escape(cte_name)}(?!["\w])'
            result = re.sub(pattern, upper_name, result, flags=re.IGNORECASE)
        
        return result
    
    def _build_queries(self, cte_set: set):
        """Build ordered list of executable queries."""
        # If there are recursive CTEs, don't decompose - return original SQL as single query
        if self._has_recursive:
            self._queries.append(DecomposedQuery(
                name="FINAL_RESULT",
                sql=self.sql,  # Return original SQL unchanged
                dependencies=[]
            ))
            return
        
        # Topological sort
        graph = {k: set(v) for k, v in self._dependencies.items()}
        for name in cte_set:
            if name not in graph:
                graph[name] = set()
        
        exec_order = list(TopologicalSorter(graph).static_order())
        
        # Get final query SQL and normalize CTE references
        parsed_copy = self._parsed.copy()
        with_clause = parsed_copy.find(exp.With)
        if with_clause:
            with_clause.pop()
        final_sql = parsed_copy.sql(dialect=self.dialect, pretty=True)
        final_sql = self._normalize_cte_references(final_sql, cte_set)
        
        # Build query list
        for name in exec_order:
            if name == "__FINAL__":
                self._queries.append(DecomposedQuery(
                    name="FINAL_RESULT",
                    sql=final_sql,
                    dependencies=self._dependencies.get("__FINAL__", [])
                ))
            elif name in self._ctes:
                # Always use uppercase unquoted table names
                table_name = name.upper()
                # Normalize CTE references in the body SQL too
                body_sql = self._normalize_cte_references(self._ctes[name], cte_set)
                
                self._queries.append(DecomposedQuery(
                    name=name,
                    sql=f'CREATE OR REPLACE TEMP TABLE {table_name} AS\n{body_sql}',
                    dependencies=self._dependencies.get(name, [])
                ))
    
    @property
    def queries(self) -> list[DecomposedQuery]:
        """List of decomposed queries in execution order."""
        return self._queries
    
    @property
    def dependencies(self) -> dict[str, list[str]]:
        """Dependency graph as dict."""
        return self._dependencies
    
    @property
    def cte_names(self) -> list[str]:
        """List of CTE names."""
        return list(self._ctes.keys())
    
    @property
    def has_recursive_cte(self) -> bool:
        """True if the SQL contains recursive CTEs."""
        return self._has_recursive
    
    @property
    def recursive_ctes(self) -> set[str]:
        """Set of recursive CTE names."""
        return self._recursive_ctes
    
    @property
    def skip_decomposition(self) -> bool:
        """True if decomposition was skipped."""
        return self._skip_decomposition
    
    @property
    def skip_reason(self) -> str:
        """Reason for skipping decomposition."""
        return self._skip_reason
    
    def graph(self, title: str = "SQL Dependency Graph") -> Digraph:
        """Generate and return a graphviz visualization of the dependency graph."""
        dot = Digraph(comment=title)
        dot.attr(rankdir='TB', size='10,10')
        dot.attr('node', shape='box', style='rounded,filled', fontname='Helvetica')
        
        for node, deps in self._dependencies.items():
            if node == "__FINAL__":
                dot.node(node, "FINAL QUERY", fillcolor='#ff6b6b', fontcolor='white')
            elif not deps:
                dot.node(node, node, fillcolor='#4ecdc4')  # Base (no CTE deps)
            else:
                dot.node(node, node, fillcolor='#ffe66d')  # Derived
            
            for dep in deps:
                dot.edge(dep, node)
        
        return dot
    
    def print_queries(self, max_lines: int = 10):
        """Print all decomposed queries."""
        print("=" * 70)
        print(f"DECOMPOSED QUERIES ({len(self._queries)} steps)")
        print("=" * 70)
        
        for i, q in enumerate(self._queries, 1):
            deps_str = f" ← {q.dependencies}" if q.dependencies else ""
            print(f"\n-- [{i}] {q.name}{deps_str}")
            print("-" * 50)
            lines = q.sql.split('\n')
            print('\n'.join(lines[:max_lines]))
            if len(lines) > max_lines:
                print(f"    ... ({len(lines) - max_lines} more lines)")
    
    def get_query(self, name: str) -> Optional[DecomposedQuery]:
        """Get a specific query by name."""
        for q in self._queries:
            if q.name == name:
                return q
        return None
    
    def __repr__(self):
        return f"SQLDecomposer(ctes={self.cte_names}, queries={len(self._queries)})"
