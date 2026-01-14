"""
Join graph builder.
Constructs FK relationship graph and computes shortest join paths.
Uses NetworkX for graph algorithms.
"""
import networkx as nx
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from observability.logger import get_logger

logger = get_logger(__name__)


@dataclass
class JoinPath:
    """Represents a join path between two tables."""
    from_table: str
    to_table: str
    path: List[str]  # List of tables in path
    # NOTE: Tuple order is (from_table, to_table, join_column_on_from_table)
    edges: List[Tuple[str, str, str]]
    depth: int


@dataclass
class FKEdge:
    """Represents a single foreign key relationship."""
    from_table: str  # Schema-qualified child table
    from_column: str  # FK column
    to_table: str  # Schema-qualified parent table
    to_column: str  # Referenced PK column
    constraint_name: str  # FK constraint name


class JoinGraphBuilder:
    """Builds FK graph and computes join paths."""

    def __init__(self, kb_schema: dict):
        self.kb_schema = kb_schema
        self.graph: Optional[nx.DiGraph] = None

    def build_fk_graph(self):
        """
        Build directed graph from foreign key relationships.
        Nodes are table names, edges are FK relationships.
        """
        if self.graph is not None:
            return  # Already built

        self.graph = nx.DiGraph()
        schema_name = self.kb_schema.get('schema_name', 'core')
        tables = self.kb_schema.get('tables', {})

        # Add all tables as nodes
        for qualified_table_name in tables.keys():
            self.graph.add_node(qualified_table_name)

        # Add FK edges (from child to parent)
        for qualified_table_name, table_meta in tables.items():
            foreign_keys = table_meta.get('foreign_keys', [])

            for fk in foreign_keys:
                # Build referenced table qualified name
                ref_schema = fk.get('referenced_schema', schema_name)
                ref_table = fk['referenced_table_name']
                ref_qualified = f"{ref_schema}.{ref_table}"

                # Edge: child -> parent with FK column info
                # Join condition meaning: child.fk_column = parent.ref_column
                self.graph.add_edge(
                    qualified_table_name,
                    ref_qualified,
                    fk_column=fk['column_name'],
                    ref_column=fk['referenced_column_name'],
                    constraint_name=fk.get('constraint_name', 'unknown')
                )

                # Also add reverse edge for bidirectional traversal
                # IMPORTANT FIX: swap columns so that edge direction always means:
                # from_table.fk_column = to_table.ref_column (column lives on "from")
                # Join condition meaning: parent.pk(ref_column) = child.fk(fk_column)
                self.graph.add_edge(
                    ref_qualified,
                    qualified_table_name,
                    fk_column=fk['referenced_column_name'],  # column on parent (from)
                    ref_column=fk['column_name'],            # column on child (to)
                    constraint_name=fk.get('constraint_name', 'unknown')
                )

        logger.info(
            "join_graph_built",
            node_count=self.graph.number_of_nodes(),
            edge_count=self.graph.number_of_edges()
        )

    def get_fk_edges(self) -> List[FKEdge]:
        """
        Get all FK edges from the schema (child -> parent only).

        Returns:
            List of FKEdge objects
        """
        if self.graph is None:
            self.build_fk_graph()

        fk_edges = []
        schema_name = self.kb_schema.get('schema_name', 'core')
        tables = self.kb_schema.get('tables', {})

        for qualified_table_name, table_meta in tables.items():
            foreign_keys = table_meta.get('foreign_keys', [])

            for fk in foreign_keys:
                ref_schema = fk.get('referenced_schema', schema_name)
                ref_table = fk['referenced_table_name']
                ref_qualified = f"{ref_schema}.{ref_table}"

                fk_edges.append(FKEdge(
                    from_table=qualified_table_name,
                    from_column=fk['column_name'],
                    to_table=ref_qualified,
                    to_column=fk['referenced_column_name'],
                    constraint_name=fk.get('constraint_name', 'unknown')
                ))

        return fk_edges

    def compute_join_paths(self, max_depth: int = 4) -> Dict[Tuple[str, str], JoinPath]:
        """
        Compute shortest join paths between all table pairs.
        Returns dict keyed by (from_table, to_table).
        """
        if self.graph is None:
            self.build_fk_graph()

        join_paths = {}
        tables = list(self.graph.nodes())

        for from_table in tables:
            try:
                paths = nx.single_source_shortest_path(
                    self.graph,
                    from_table,
                    cutoff=max_depth
                )

                for to_table, path in paths.items():
                    if from_table == to_table:
                        continue

                    depth = len(path) - 1
                    if depth > max_depth:
                        continue

                    # Extract edge information
                    edges = []
                    for i in range(len(path) - 1):
                        edge_data = self.graph.get_edge_data(path[i], path[i + 1]) or {}
                        edges.append((
                            path[i],
                            path[i + 1],
                            edge_data.get('fk_column', 'unknown')  # column on "from" table for this hop
                        ))

                    join_path = JoinPath(
                        from_table=from_table,
                        to_table=to_table,
                        path=path,
                        edges=edges,
                        depth=depth
                    )

                    join_paths[(from_table, to_table)] = join_path

            except nx.NodeNotFound:
                logger.warning("node_not_found_in_graph", table=from_table)
                continue

        logger.info(
            "join_paths_computed",
            path_count=len(join_paths),
            max_depth=max_depth
        )

        return join_paths

    def validate_join_path(self, tables: List[str]) -> bool:
        """
        Validate if a join path exists between all tables in the list.
        Returns True if all tables can be joined.
        """
        if self.graph is None:
            self.build_fk_graph()

        if len(tables) <= 1:
            return True

        # Check if there's connectivity between consecutive tables
        for i in range(len(tables) - 1):
            try:
                nx.shortest_path(self.graph, tables[i], tables[i + 1])
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                logger.warning(
                    "invalid_join_path",
                    from_table=tables[i],
                    to_table=tables[i + 1]
                )
                return False

        return True

    def get_join_depth(self, tables: List[str]) -> int:
        """
        Calculate join depth for a list of tables.
        Join depth = number of unique tables - 1
        """
        unique_tables = set(tables)
        return len(unique_tables) - 1

    def get_join_sql_hint(self, from_table: str, to_table: str) -> Optional[str]:
        """
        Generate SQL join hint for two tables based on FK relationship.
        Returns JOIN ON clause suggestion.
        """
        if self.graph is None:
            self.build_fk_graph()

        try:
            path = nx.shortest_path(self.graph, from_table, to_table)

            if len(path) == 2:
                edge_data = self.graph.get_edge_data(path[0], path[1]) or {}
                return f"{path[0]}.{edge_data['fk_column']} = {path[1]}.{edge_data['ref_column']}"
            else:
                joins = []
                for i in range(len(path) - 1):
                    edge_data = self.graph.get_edge_data(path[i], path[i + 1]) or {}
                    joins.append(
                        f"{path[i]}.{edge_data['fk_column']} = {path[i + 1]}.{edge_data['ref_column']}"
                    )
                return " AND ".join(joins)

        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return None

    def to_dict(self) -> dict:
        """
        Convert join graph to dictionary format for compiled_rules.json.
        """
        if self.graph is None:
            self.build_fk_graph()

        graph_dict = {
            "nodes": list(self.graph.nodes()),
            "edges": [
                    {
                        "from": u,
                        "to": v,
                        "from_column": data.get("fk_column"),
                        "to_column": data.get("ref_column"),
                        "constraint_name": data.get("constraint_name")
                    }
                    for u, v, data in self.graph.edges(data=True)
            ]
        }

        # Compute join paths
        join_paths = self.compute_join_paths()

        # Convert join paths to serializable format
        paths_dict = {}
        for (from_table, to_table), join_path in join_paths.items():
            key = f"{from_table}->{to_table}"
            paths_dict[key] = {
                "from_table": from_table,
                "to_table": to_table,
                "path": join_path.path,
                "edges": [
                    {"from": e[0], "to": e[1], "column": e[2]}
                    for e in join_path.edges
                ],
                "depth": join_path.depth
            }

        return {
            "graph": graph_dict,
            "join_paths": paths_dict
        }
