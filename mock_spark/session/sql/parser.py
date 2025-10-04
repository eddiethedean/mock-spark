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
    >>> from mock_spark.session.sql import MockSQLParser
    >>> parser = MockSQLParser()
    >>> ast = parser.parse("SELECT * FROM users WHERE age > 18")
    >>> print(ast.query_type)
    'SELECT'
"""

from typing import Any, Dict, List, Optional, Union
from ...core.exceptions.analysis import ParseException


class MockSQLAST:
    """Abstract Syntax Tree for SQL queries."""

    def __init__(self, query_type: str, components: Dict[str, Any]):
        """Initialize SQL AST.
        
        Args:
            query_type: Type of SQL query (SELECT, INSERT, CREATE, etc.).
            components: Dictionary of query components.
        """
        self.query_type = query_type
        self.components = components

    def __str__(self) -> str:
        """String representation."""
        return f"MockSQLAST(type='{self.query_type}', components={len(self.components)})"

    def __repr__(self) -> str:
        """Representation."""
        return self.__str__()


class MockSQLParser:
    """SQL Parser for Mock Spark.

    Provides SQL parsing functionality that converts SQL queries
    into abstract syntax trees for further processing and execution.
    Supports common SQL operations including SELECT, INSERT, CREATE,
    DROP, and other DDL/DML operations.

    Example:
        >>> parser = MockSQLParser()
        >>> ast = parser.parse("SELECT name, age FROM users WHERE age > 18")
        >>> print(ast.query_type)
        'SELECT'
    """

    def __init__(self):
        """Initialize MockSQLParser."""
        self._keywords = {
            'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT',
            'INSERT', 'INTO', 'VALUES', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
            'TABLE', 'DATABASE', 'SCHEMA', 'ALTER', 'TRUNCATE', 'SHOW',
            'DESCRIBE', 'EXPLAIN', 'WITH', 'UNION', 'INTERSECT', 'EXCEPT',
            'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'ON', 'AS', 'AND', 'OR',
            'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'CASE',
            'WHEN', 'THEN', 'ELSE', 'END', 'CAST', 'COALESCE', 'NULLIF',
            'IF', 'IFNULL', 'NVL', 'NVL2', 'DECODE', 'GREATEST', 'LEAST',
            'ROUND', 'TRUNC', 'FLOOR', 'CEIL', 'ABS', 'MOD', 'POWER',
            'SQRT', 'EXP', 'LN', 'LOG', 'SIN', 'COS', 'TAN', 'ASIN',
            'ACOS', 'ATAN', 'ATAN2', 'DEGREES', 'RADIANS', 'PI', 'E',
            'RAND', 'RANDOM', 'UUID', 'GUID', 'LENGTH', 'CHAR_LENGTH',
            'CHARACTER_LENGTH', 'UPPER', 'LOWER', 'INITCAP', 'TRIM',
            'LTRIM', 'RTRIM', 'LPAD', 'RPAD', 'SUBSTRING', 'SUBSTR',
            'INSTR', 'POSITION', 'REPLACE', 'TRANSLATE', 'CONCAT',
            'CONCAT_WS', 'SPLIT', 'REGEXP_REPLACE', 'REGEXP_EXTRACT',
            'REGEXP_LIKE', 'REGEXP_SUBSTR', 'REGEXP_INSTR', 'REGEXP_COUNT',
            'TO_CHAR', 'TO_NUMBER', 'TO_DATE', 'TO_TIMESTAMP', 'CURRENT_DATE',
            'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'LOCALTIME', 'LOCALTIMESTAMP',
            'EXTRACT', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND',
            'QUARTER', 'WEEK', 'DAYOFWEEK', 'DAYOFYEAR', 'WEEKDAY',
            'WEEKOFYEAR', 'YEARWEEK', 'DATE_ADD', 'DATE_SUB', 'DATEDIFF',
            'TIMESTAMPDIFF', 'TIMESTAMPADD', 'MAKEDATE', 'MAKETIME',
            'PERIOD_ADD', 'PERIOD_DIFF', 'LAST_DAY', 'NEXT_DAY',
            'MONTHNAME', 'DAYNAME', 'QUARTER', 'WEEK', 'YEARWEEK',
            'FROM_UNIXTIME', 'UNIX_TIMESTAMP', 'STR_TO_DATE', 'DATE_FORMAT',
            'TIME_FORMAT', 'GET_FORMAT', 'CONVERT_TZ', 'UTC_TIMESTAMP',
            'UTC_TIME', 'UTC_DATE', 'SYSDATE', 'NOW', 'CURDATE', 'CURTIME',
            'UNIX_TIMESTAMP', 'FROM_UNIXTIME', 'STR_TO_DATE', 'DATE_FORMAT',
            'TIME_FORMAT', 'GET_FORMAT', 'CONVERT_TZ', 'UTC_TIMESTAMP',
            'UTC_TIME', 'UTC_DATE', 'SYSDATE', 'NOW', 'CURDATE', 'CURTIME'
        }

    def parse(self, query: str) -> MockSQLAST:
        """Parse SQL query into AST.
        
        Args:
            query: SQL query string.
            
        Returns:
            MockSQLAST object representing the parsed query.
            
        Raises:
            ParseException: If query parsing fails.
        """
        if not query or not query.strip():
            raise ParseException("Empty query provided")

        query = query.strip()
        query_type = self._detect_query_type(query)
        
        try:
            components = self._parse_components(query, query_type)
            return MockSQLAST(query_type, components)
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
        
        if query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('INSERT'):
            return 'INSERT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE'):
            return 'DELETE'
        elif query_upper.startswith('CREATE'):
            return 'CREATE'
        elif query_upper.startswith('DROP'):
            return 'DROP'
        elif query_upper.startswith('ALTER'):
            return 'ALTER'
        elif query_upper.startswith('TRUNCATE'):
            return 'TRUNCATE'
        elif query_upper.startswith('SHOW'):
            return 'SHOW'
        elif query_upper.startswith('DESCRIBE') or query_upper.startswith('DESC'):
            return 'DESCRIBE'
        elif query_upper.startswith('EXPLAIN'):
            return 'EXPLAIN'
        else:
            return 'UNKNOWN'

    def _parse_components(self, query: str, query_type: str) -> Dict[str, Any]:
        """Parse query components based on query type.
        
        Args:
            query: SQL query string.
            query_type: Type of SQL query.
            
        Returns:
            Dictionary of parsed components.
        """
        components = {
            'original_query': query,
            'query_type': query_type,
            'tokens': self._tokenize(query),
            'tables': [],
            'columns': [],
            'conditions': [],
            'joins': [],
            'group_by': [],
            'order_by': [],
            'limit': None,
            'offset': None
        }

        if query_type == 'SELECT':
            components.update(self._parse_select_query(query))
        elif query_type == 'CREATE':
            components.update(self._parse_create_query(query))
        elif query_type == 'DROP':
            components.update(self._parse_drop_query(query))
        elif query_type == 'INSERT':
            components.update(self._parse_insert_query(query))
        elif query_type == 'UPDATE':
            components.update(self._parse_update_query(query))
        elif query_type == 'DELETE':
            components.update(self._parse_delete_query(query))

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
        tokens = re.findall(r'\b\w+\b|[(),;=<>!]+', query)
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
            'select_columns': [],
            'from_tables': [],
            'where_conditions': [],
            'group_by_columns': [],
            'having_conditions': [],
            'order_by_columns': [],
            'limit_value': None
        }

        # Simple regex-based parsing for demonstration
        import re
        
        # Extract SELECT columns
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE)
        if select_match:
            columns_str = select_match.group(1)
            if columns_str.strip() == '*':
                components['select_columns'] = ['*']
            else:
                components['select_columns'] = [col.strip() for col in columns_str.split(',')]

        # Extract FROM tables
        from_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
        if from_match:
            components['from_tables'] = [from_match.group(1)]

        # Extract WHERE conditions
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|$)', query, re.IGNORECASE)
        if where_match:
            components['where_conditions'] = [where_match.group(1).strip()]

        return components

    def _parse_create_query(self, query: str) -> Dict[str, Any]:
        """Parse CREATE query components.
        
        Args:
            query: CREATE query string.
            
        Returns:
            Dictionary of CREATE components.
        """
        # Mock implementation
        return {
            'object_type': 'TABLE',  # or DATABASE, SCHEMA, etc.
            'object_name': 'unknown',
            'definition': query
        }

    def _parse_drop_query(self, query: str) -> Dict[str, Any]:
        """Parse DROP query components.
        
        Args:
            query: DROP query string.
            
        Returns:
            Dictionary of DROP components.
        """
        # Mock implementation
        return {
            'object_type': 'TABLE',  # or DATABASE, SCHEMA, etc.
            'object_name': 'unknown'
        }

    def _parse_insert_query(self, query: str) -> Dict[str, Any]:
        """Parse INSERT query components.
        
        Args:
            query: INSERT query string.
            
        Returns:
            Dictionary of INSERT components.
        """
        # Mock implementation
        return {
            'table_name': 'unknown',
            'columns': [],
            'values': []
        }

    def _parse_update_query(self, query: str) -> Dict[str, Any]:
        """Parse UPDATE query components.
        
        Args:
            query: UPDATE query string.
            
        Returns:
            Dictionary of UPDATE components.
        """
        # Mock implementation
        return {
            'table_name': 'unknown',
            'set_clauses': [],
            'where_conditions': []
        }

    def _parse_delete_query(self, query: str) -> Dict[str, Any]:
        """Parse DELETE query components.
        
        Args:
            query: DELETE query string.
            
        Returns:
            Dictionary of DELETE components.
        """
        # Mock implementation
        return {
            'table_name': 'unknown',
            'where_conditions': []
        }
