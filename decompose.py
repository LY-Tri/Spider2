import sqlglot
from sqlglot import exp
from graphlib import TopologicalSorter
from graphviz import Digraph
from dataclasses import dataclass
from typing import Optional


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
        self._parse()
    
    def _parse(self):
        """Parse SQL and extract CTEs, dependencies, and decomposed queries."""
        # Extract CTEs
        cte_set = set()
        if self._parsed.ctes:
            for cte in self._parsed.ctes:
                self._ctes[cte.alias] = cte.this.sql(dialect=self.dialect, pretty=True)
                cte_set.add(cte.alias)
        
        # Build dependencies
        for cte in (self._parsed.ctes or []):
            cte_name = cte.alias
            deps = []
            for table in cte.this.find_all(exp.Table):
                if table.name in cte_set and table.name != cte_name:
                    deps.append(table.name)
            self._dependencies[cte_name] = list(set(deps))
        
        # Final query dependencies
        main_select = self._parsed.find(exp.Select)
        if main_select:
            final_deps = [t.name for t in main_select.find_all(exp.Table) if t.name in cte_set]
            self._dependencies["__FINAL__"] = list(set(final_deps))
        
        # Build execution order
        self._build_queries(cte_set)
    
    def _build_queries(self, cte_set: set):
        """Build ordered list of executable queries."""
        # Topological sort
        graph = {k: set(v) for k, v in self._dependencies.items()}
        for name in cte_set:
            if name not in graph:
                graph[name] = set()
        
        exec_order = list(TopologicalSorter(graph).static_order())
        
        # Get final query SQL
        parsed_copy = self._parsed.copy()
        with_clause = parsed_copy.find(exp.With)
        if with_clause:
            with_clause.pop()
        final_sql = parsed_copy.sql(dialect=self.dialect, pretty=True)
        
        # Build query list
        for name in exec_order:
            if name == "__FINAL__":
                self._queries.append(DecomposedQuery(
                    name="FINAL_RESULT",
                    sql=final_sql,
                    dependencies=self._dependencies.get("__FINAL__", [])
                ))
            elif name in self._ctes:
                self._queries.append(DecomposedQuery(
                    name=name,
                    sql=f"CREATE OR REPLACE TEMP TABLE {name} AS\n{self._ctes[name]}",
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
