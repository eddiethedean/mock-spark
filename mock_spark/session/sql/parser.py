"""
SQL Parser for Mock Spark.

This module provides SQL parsing functionality for Mock Spark,
converting SQL queries into abstract syntax trees (AST) for
further processing and execution.

Key Features:
    - SQL query parsing and validation
    - AST generation for complex queries
    - Query type detection (SELECT, INSERT, CREATE, etc.)
    - Syntax error detection and reporting
    - Support for common SQL operations

Example:
    >>> from mock_spark.session.sql import SQLParser
    >>> parser = SQLParser()
    >>> ast = parser.parse("SELECT * FROM users WHERE age > 18")
    >>> print(ast.query_type)
    'SELECT'
"""

from typing import Any, Dict, List, Union
from ...core.exceptions.analysis import ParseException


class SQLAST:
    """Abstract Syntax Tree for SQL queries."""

    def __init__(self, query_type: str, components: Dict[str, Any], query: str = ""):
        """Initialize SQL AST.

        Args:
            query_type: Type of SQL query (SELECT, INSERT, CREATE, etc.).
            components: Dictionary of query components.
            query: Original SQL query string.
        """
        self.query_type = query_type
        self.components = components
        self.query = query

    def __str__(self) -> str:
        """String representation."""
        return f"SQLAST(type='{self.query_type}', components={len(self.components)})"

    def __repr__(self) -> str:
        """Representation."""
        return self.__str__()


class SQLParser:
    """SQL Parser for Mock Spark.

    Provides SQL parsing functionality that converts SQL queries
    into abstract syntax trees for further processing and execution.
    Supports common SQL operations including SELECT, INSERT, CREATE,
    DROP, and other DDL/DML operations.

    Example:
        >>> parser = SQLParser()
        >>> ast = parser.parse("SELECT name, age FROM users WHERE age > 18")
        >>> print(ast.query_type)
        'SELECT'
    """

    def __init__(self) -> None:
        """Initialize SQLParser."""
        self._keywords = {
            "SELECT",
            "FROM",
            "WHERE",
            "GROUP",
            "BY",
            "HAVING",
            "ORDER",
            "LIMIT",
            "INSERT",
            "INTO",
            "VALUES",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "TABLE",
            "DATABASE",
            "SCHEMA",
            "ALTER",
            "TRUNCATE",
            "SHOW",
            "DESCRIBE",
            "EXPLAIN",
            "WITH",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "OUTER",
            "ON",
            "AS",
            "AND",
            "OR",
            "NOT",
            "IN",
            "EXISTS",
            "BETWEEN",
            "LIKE",
            "IS",
            "NULL",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "CAST",
            "COALESCE",
            "NULLIF",
            "IF",
            "IFNULL",
            "NVL",
            "NVL2",
            "DECODE",
            "GREATEST",
            "LEAST",
            "ROUND",
            "TRUNC",
            "FLOOR",
            "CEIL",
            "ABS",
            "MOD",
            "POWER",
            "SQRT",
            "EXP",
            "LN",
            "LOG",
            "SIN",
            "COS",
            "TAN",
            "ASIN",
            "ACOS",
            "ATAN",
            "ATAN2",
            "DEGREES",
            "RADIANS",
            "PI",
            "E",
            "RAND",
            "RANDOM",
            "UUID",
            "GUID",
            "LENGTH",
            "CHAR_LENGTH",
            "CHARACTER_LENGTH",
            "UPPER",
            "LOWER",
            "INITCAP",
            "TRIM",
            "LTRIM",
            "RTRIM",
            "LPAD",
            "RPAD",
            "SUBSTRING",
            "SUBSTR",
            "INSTR",
            "POSITION",
            "REPLACE",
            "TRANSLATE",
            "CONCAT",
            "CONCAT_WS",
            "SPLIT",
            "REGEXP_REPLACE",
            "REGEXP_EXTRACT",
            "REGEXP_LIKE",
            "REGEXP_SUBSTR",
            "REGEXP_INSTR",
            "REGEXP_COUNT",
            "TO_CHAR",
            "TO_NUMBER",
            "TO_DATE",
            "TO_TIMESTAMP",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "EXTRACT",
            "YEAR",
            "MONTH",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            "QUARTER",
            "WEEK",
            "DAYOFWEEK",
            "DAYOFYEAR",
            "WEEKDAY",
            "WEEKOFYEAR",
            "YEARWEEK",
            "DATE_ADD",
            "DATE_SUB",
            "DATEDIFF",
            "TIMESTAMPDIFF",
            "TIMESTAMPADD",
            "MAKEDATE",
            "MAKETIME",
            "PERIOD_ADD",
            "PERIOD_DIFF",
            "LAST_DAY",
            "NEXT_DAY",
            "MONTHNAME",
            "DAYNAME",
            "QUARTER",
            "WEEK",
            "YEARWEEK",
            "FROM_UNIXTIME",
            "UNIX_TIMESTAMP",
            "STR_TO_DATE",
            "DATE_FORMAT",
            "TIME_FORMAT",
            "GET_FORMAT",
            "CONVERT_TZ",
            "UTC_TIMESTAMP",
            "UTC_TIME",
            "UTC_DATE",
            "SYSDATE",
            "NOW",
            "CURDATE",
            "CURTIME",
            "UNIX_TIMESTAMP",
            "FROM_UNIXTIME",
            "STR_TO_DATE",
            "DATE_FORMAT",
            "TIME_FORMAT",
            "GET_FORMAT",
            "CONVERT_TZ",
            "UTC_TIMESTAMP",
            "UTC_TIME",
            "UTC_DATE",
            "SYSDATE",
            "NOW",
            "CURDATE",
            "CURTIME",
        }

    def parse(self, query: str) -> SQLAST:
        """Parse SQL query into AST.

        Args:
            query: SQL query string.

        Returns:
            SQLAST object representing the parsed query.

        Raises:
            ParseException: If query parsing fails.
        """
        if not query or not query.strip():
            raise ParseException("Empty query provided")

        query = query.strip()
        query_type = self._detect_query_type(query)

        try:
            components = self._parse_components(query, query_type)
            return SQLAST(query_type, components, query)
        except Exception as e:
            raise ParseException(f"Failed to parse query: {str(e)}")

    def _detect_query_type(self, query: str) -> str:
        """Detect the type of SQL query.

        Args:
            query: SQL query string.

        Returns:
            Query type string.
        """
        query_upper = query.upper().strip()

        if query_upper.startswith("SELECT"):
            return "SELECT"
        elif query_upper.startswith("INSERT"):
            return "INSERT"
        elif query_upper.startswith("UPDATE"):
            return "UPDATE"
        elif query_upper.startswith("DELETE"):
            return "DELETE"
        elif query_upper.startswith("MERGE"):
            return "MERGE"
        elif query_upper.startswith("CREATE"):
            return "CREATE"
        elif query_upper.startswith("DROP"):
            return "DROP"
        elif query_upper.startswith("ALTER"):
            return "ALTER"
        elif query_upper.startswith("TRUNCATE"):
            return "TRUNCATE"
        elif query_upper.startswith("SHOW"):
            return "SHOW"
        elif query_upper.startswith("DESCRIBE") or query_upper.startswith("DESC"):
            return "DESCRIBE"
        elif query_upper.startswith("EXPLAIN"):
            return "EXPLAIN"
        else:
            return "UNKNOWN"

    def _parse_components(self, query: str, query_type: str) -> Dict[str, Any]:
        """Parse query components based on query type.

        Args:
            query: SQL query string.
            query_type: Type of SQL query.

        Returns:
            Dictionary of parsed components.
        """
        components = {
            "original_query": query,
            "query_type": query_type,
            "tokens": self._tokenize(query),
            "tables": [],
            "columns": [],
            "conditions": [],
            "joins": [],
            "group_by": [],
            "order_by": [],
            "limit": None,
            "offset": None,
        }

        if query_type == "SELECT":
            components.update(self._parse_select_query(query))
        elif query_type == "CREATE":
            components.update(self._parse_create_query(query))
        elif query_type == "DROP":
            components.update(self._parse_drop_query(query))
        elif query_type == "INSERT":
            components.update(self._parse_insert_query(query))
        elif query_type == "UPDATE":
            components.update(self._parse_update_query(query))
        elif query_type == "DELETE":
            components.update(self._parse_delete_query(query))
        elif query_type == "MERGE":
            components.update(self._parse_merge_query(query))

        return components

    def _tokenize(self, query: str) -> List[str]:
        """Tokenize SQL query.

        Args:
            query: SQL query string.

        Returns:
            List of tokens.
        """
        # Simple tokenization - split by whitespace and common delimiters
        import re

        tokens = re.findall(r"\b\w+\b|[(),;=<>!]+", query)
        return tokens

    def _parse_select_query(self, query: str) -> Dict[str, Any]:
        """Parse SELECT query components.

        Args:
            query: SELECT query string.

        Returns:
            Dictionary of SELECT components.
        """
        # Mock implementation - in real parser this would be much more sophisticated
        components: Dict[str, Union[List[str], None]] = {
            "select_columns": [],
            "from_tables": [],
            "where_conditions": [],
            "group_by_columns": [],
            "having_conditions": [],
            "order_by_columns": [],
            "limit_value": None,
        }

        # Simple regex-based parsing for demonstration
        import re

        # Extract SELECT columns
        select_match = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.IGNORECASE)
        if select_match:
            columns_str = select_match.group(1)
            if columns_str.strip() == "*":
                components["select_columns"] = ["*"]
            else:
                components["select_columns"] = [
                    col.strip() for col in columns_str.split(",")
                ]

        # Extract FROM tables
        from_match = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
        if from_match:
            components["from_tables"] = [from_match.group(1)]

        # Extract WHERE conditions
        where_match = re.search(
            r"WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)",
            query,
            re.IGNORECASE,
        )
        if where_match:
            components["where_conditions"] = [where_match.group(1).strip()]

        return components

    def _parse_create_query(self, query: str) -> Dict[str, Any]:
        """Parse CREATE query components.

        Args:
            query: CREATE query string.

        Returns:
            Dictionary of CREATE components.
        """
        import re

        # Parse CREATE DATABASE/SCHEMA [IF NOT EXISTS] <name>
        create_db_match = re.match(
            r"CREATE\s+(DATABASE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\w]+)",
            query,
            re.IGNORECASE,
        )
        if create_db_match:
            object_type = create_db_match.group(1).upper()
            object_name = create_db_match.group(2).strip("`")
            # if_not_exists should be True when "IF NOT EXISTS" is present (meaning: ignore if exists)
            if_not_exists = "IF NOT EXISTS" in query.upper()
            return {
                "object_type": object_type,
                "object_name": object_name,
                "ignore_if_exists": if_not_exists,  # Renamed for clarity
                "definition": query,
            }

        # Default for other CREATE statements
        return {
            "object_type": "TABLE",
            "object_name": "unknown",
            "definition": query,
        }

    def _parse_drop_query(self, query: str) -> Dict[str, Any]:
        """Parse DROP query components.

        Args:
            query: DROP query string.

        Returns:
            Dictionary of DROP components.
        """
        import re

        # Parse DROP DATABASE/SCHEMA [IF EXISTS] <name>
        drop_db_match = re.match(
            r"DROP\s+(DATABASE|SCHEMA)\s+(?:IF\s+EXISTS\s+)?([`\w]+)",
            query,
            re.IGNORECASE,
        )
        if drop_db_match:
            object_type = drop_db_match.group(1).upper()
            object_name = drop_db_match.group(2).strip("`")
            # ignore_if_not_exists should be True when "IF EXISTS" is present (meaning: ignore if not exists)
            ignore_if_not_exists = "IF EXISTS" in query.upper()
            return {
                "object_type": object_type,
                "object_name": object_name,
                "ignore_if_not_exists": ignore_if_not_exists,  # Renamed for clarity
            }

        # Default for other DROP statements
        return {
            "object_type": "TABLE",
            "object_name": "unknown",
        }

    def _parse_insert_query(self, query: str) -> Dict[str, Any]:
        """Parse INSERT query components.

        Args:
            query: INSERT query string.

        Returns:
            Dictionary of INSERT components.
        """
        # Mock implementation
        return {"table_name": "unknown", "columns": [], "values": []}

    def _parse_update_query(self, query: str) -> Dict[str, Any]:
        """Parse UPDATE query components.

        Args:
            query: UPDATE query string.

        Returns:
            Dictionary of UPDATE components.
        """
        # Mock implementation
        return {"table_name": "unknown", "set_clauses": [], "where_conditions": []}

    def _parse_delete_query(self, query: str) -> Dict[str, Any]:
        """Parse DELETE query components.

        Args:
            query: DELETE query string.

        Returns:
            Dictionary of DELETE components.
        """
        # Mock implementation
        return {"table_name": "unknown", "where_conditions": []}

    def _parse_merge_query(self, query: str) -> Dict[str, Any]:
        """Parse MERGE INTO query components.

        Args:
            query: MERGE query string.

        Returns:
            Dictionary of MERGE components.
        """
        import re

        components = {}

        # Extract: MERGE INTO target_table
        target_match = re.search(
            r"MERGE\s+INTO\s+(\w+(?:\.\w+)?)", query, re.IGNORECASE
        )
        if target_match:
            components["target_table"] = target_match.group(1)
            components["target_alias"] = None
            # Check for alias
            alias_match = re.search(
                r"MERGE\s+INTO\s+\w+(?:\.\w+)?\s+(?:AS\s+)?(\w+)", query, re.IGNORECASE
            )
            if alias_match:
                potential_alias = alias_match.group(1)
                if potential_alias.upper() not in ["USING"]:
                    components["target_alias"] = potential_alias

        # Extract: USING source_table
        using_match = re.search(r"USING\s+(\w+(?:\.\w+)?)", query, re.IGNORECASE)
        if using_match:
            components["source_table"] = using_match.group(1)
            components["source_alias"] = None
            # Check for alias
            alias_match = re.search(
                r"USING\s+\w+(?:\.\w+)?\s+(?:AS\s+)?(\w+)", query, re.IGNORECASE
            )
            if alias_match:
                potential_alias = alias_match.group(1)
                if potential_alias.upper() not in ["ON"]:
                    components["source_alias"] = potential_alias

        # Extract: ON condition
        on_match = re.search(r"ON\s+(.*?)\s+WHEN", query, re.IGNORECASE | re.DOTALL)
        if on_match:
            components["on_condition"] = on_match.group(1).strip()

        # Extract: WHEN MATCHED clauses
        matched_clauses = []
        for match in re.finditer(
            r"WHEN\s+MATCHED\s+THEN\s+(UPDATE|DELETE)(.*?)(?=WHEN|$)",
            query,
            re.IGNORECASE | re.DOTALL,
        ):
            action = match.group(1).upper()
            details = match.group(2).strip()

            if action == "UPDATE":
                # Parse SET clause
                set_match = re.search(
                    r"SET\s+(.*?)(?=WHEN|$)", details, re.IGNORECASE | re.DOTALL
                )
                if set_match:
                    set_clause = set_match.group(1).strip()
                    matched_clauses.append(
                        {"action": "UPDATE", "set_clause": set_clause}
                    )
            elif action == "DELETE":
                matched_clauses.append({"action": "DELETE"})

        components["when_matched"] = matched_clauses

        # Extract: WHEN NOT MATCHED clauses
        not_matched_clauses = []
        for match in re.finditer(
            r"WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s+(.*?)(?=WHEN|$)",
            query,
            re.IGNORECASE | re.DOTALL,
        ):
            insert_clause = match.group(1).strip()
            not_matched_clauses.append(
                {"action": "INSERT", "insert_clause": insert_clause}
            )

        components["when_not_matched"] = not_matched_clauses

        return components
