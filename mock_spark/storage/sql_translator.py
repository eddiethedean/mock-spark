"""
SQL to SQLAlchemy Translator.

This module provides functionality to parse Spark SQL queries and convert them
to SQLAlchemy statements, eliminating the need for raw SQL execution.
"""

from typing import Any, Dict, List, Optional, Union
import sqlglot
from sqlglot import exp
from sqlalchemy import (
    Table,
    Column,
    MetaData,
    select,
    insert,
    update,
    delete,
    and_,
    or_,
    not_,
    func,
    literal,
    case,
    cast,
    desc,
    asc,
    inspect,
)
from sqlalchemy.engine import Engine
from sqlalchemy.sql import Select, Insert, Update, Delete
from sqlalchemy.sql.elements import ColumnElement
from .spark_function_mapper import get_sqlalchemy_function, is_supported_function


class SQLTranslationError(Exception):
    """Raised when SQL cannot be translated to SQLAlchemy."""
    pass


class SQLToSQLAlchemyTranslator:
    """
    Translates Spark SQL queries to SQLAlchemy statements.
    
    This enables zero raw SQL while maintaining full spark.sql() functionality.
    """
    
    def __init__(self, engine: Engine):
        """
        Initialize translator with database engine.
        
        Args:
            engine: SQLAlchemy engine for table reflection
        """
        self.engine = engine
        self.inspector = inspect(engine)
        self.metadata = MetaData()
        self._table_cache: Dict[str, Table] = {}
    
    def translate(self, sql: str) -> Union[Select, Insert, Update, Delete]:
        """
        Translate SQL string to SQLAlchemy statement.
        
        Args:
            sql: Spark SQL query string
            
        Returns:
            SQLAlchemy statement (Select, Insert, Update, or Delete)
            
        Raises:
            SQLTranslationError: If SQL cannot be translated
        """
        try:
            # Parse SQL with sqlglot using Spark dialect
            ast = sqlglot.parse_one(sql, dialect='spark')
            
            # Translate based on statement type
            if isinstance(ast, exp.Select):
                return self._translate_select(ast)
            elif isinstance(ast, exp.Insert):
                return self._translate_insert(ast)
            elif isinstance(ast, exp.Update):
                return self._translate_update(ast)
            elif isinstance(ast, exp.Delete):
                return self._translate_delete(ast)
            elif isinstance(ast, exp.Create):
                return self._translate_create(ast)
            elif isinstance(ast, exp.Drop):
                return self._translate_drop(ast)
            else:
                raise SQLTranslationError(
                    f"Unsupported SQL statement type: {type(ast).__name__}"
                )
                
        except Exception as e:
            raise SQLTranslationError(f"Failed to translate SQL: {sql}\nError: {e}") from e
    
    def _get_table(self, table_name: str) -> Table:
        """
        Get or reflect table from database.
        
        Args:
            table_name: Name of table (may include schema)
            
        Returns:
            SQLAlchemy Table object
        """
        if table_name in self._table_cache:
            return self._table_cache[table_name]
        
        # Handle schema.table format
        if '.' in table_name:
            schema, name = table_name.split('.', 1)
        else:
            schema = None
            name = table_name
        
        # Reflect table from database
        table = Table(
            name,
            self.metadata,
            schema=schema,
            autoload_with=self.engine
        )
        
        self._table_cache[table_name] = table
        return table
    
    def _translate_select(self, ast: exp.Select) -> Select:
        """Translate SELECT statement to SQLAlchemy."""
        # Get FROM table
        from_clause = ast.find(exp.From)
        if not from_clause:
            raise SQLTranslationError("SELECT must have FROM clause")
        
        table_exp = from_clause.this
        if isinstance(table_exp, exp.Table):
            table_name = str(table_exp.this)
            table = self._get_table(table_name)
        else:
            raise SQLTranslationError(f"Unsupported FROM clause: {type(table_exp).__name__}")
        
        # Build SELECT columns
        select_cols = []
        has_star = False
        for projection in ast.expressions:
            if isinstance(projection, exp.Star):
                # SELECT *
                has_star = True
                select_cols = [table]
                break
            else:
                col = self._translate_expression(projection, table)
                select_cols.append(col)
        
        # Start building statement
        if select_cols:
            stmt = select(*select_cols)
            # If not SELECT *, need to add from clause for aggregate functions
            if not has_star and select_cols:
                # Check if any columns are aggregate functions
                stmt = stmt.select_from(table)
        else:
            stmt = select(table)
        
        # Handle JOINs
        joins = ast.find_all(exp.Join)
        for join_exp in joins:
            join_table_exp = join_exp.this
            if isinstance(join_table_exp, exp.Table):
                join_table_name = str(join_table_exp.this)
                join_table = self._get_table(join_table_name)
                
                # Get join condition
                on_exp = join_exp.args.get('on')
                if on_exp:
                    join_condition = self._translate_join_condition(on_exp, table, join_table)
                    
                    # Determine join type
                    join_kind = join_exp.args.get('kind', '').upper()
                    if join_kind == 'LEFT':
                        stmt = stmt.select_from(table).outerjoin(join_table, join_condition)
                    elif join_kind == 'RIGHT':
                        # SQLAlchemy doesn't have right join, need to swap tables
                        stmt = stmt.select_from(join_table).outerjoin(table, join_condition)
                    elif join_kind == 'FULL':
                        stmt = stmt.select_from(table).outerjoin(join_table, join_condition, full=True)
                    else:
                        # INNER join (default)
                        stmt = stmt.select_from(table).join(join_table, join_condition)
        
        # Add WHERE clause
        where = ast.find(exp.Where)
        if where:
            condition = self._translate_expression(where.this, table)
            stmt = stmt.where(condition)
        
        # Add GROUP BY
        group_by = ast.find(exp.Group)
        if group_by:
            group_cols = []
            for expr in group_by.expressions:
                col = self._translate_expression(expr, table)
                group_cols.append(col)
            stmt = stmt.group_by(*group_cols)
        
        # Add HAVING
        having = ast.find(exp.Having)
        if having:
            condition = self._translate_expression(having.this, table)
            stmt = stmt.having(condition)
        
        # Add ORDER BY
        order_by = ast.find(exp.Order)
        if order_by:
            order_cols = []
            for ordered in order_by.expressions:
                col = self._translate_expression(ordered.this, table)
                if isinstance(ordered, exp.Ordered) and ordered.args.get('desc'):
                    col = desc(col)
                else:
                    col = asc(col)
                order_cols.append(col)
            stmt = stmt.order_by(*order_cols)
        
        # Add LIMIT
        limit_exp = ast.find(exp.Limit)
        if limit_exp:
            # Get limit expression value
            limit_expr = limit_exp.expression if hasattr(limit_exp, 'expression') else limit_exp.this
            if limit_expr:
                limit_val = int(str(limit_expr))
                stmt = stmt.limit(limit_val)
        
        # Add DISTINCT
        if ast.args.get('distinct'):
            stmt = stmt.distinct()
        
        return stmt
    
    def _translate_join_condition(self, on_exp: exp.Expression, left_table: Table, right_table: Table) -> ColumnElement:
        """Translate JOIN ON condition to SQLAlchemy."""
        # Simple implementation for now - assumes ON clause references columns from both tables
        # This will need enhancement for complex join conditions
        if isinstance(on_exp, exp.EQ):
            left = self._translate_expression(on_exp.this, left_table)
            right = self._translate_expression(on_exp.expression, right_table)
            return left == right
        else:
            # Try to translate as general expression using left table
            return self._translate_expression(on_exp, left_table)
    
    def _translate_insert(self, ast: exp.Insert) -> Insert:
        """Translate INSERT statement to SQLAlchemy."""
        # Get table
        table_exp = ast.this
        if isinstance(table_exp, exp.Table):
            table_name = str(table_exp.this)
            table = self._get_table(table_name)
        else:
            raise SQLTranslationError(f"Unsupported INSERT table: {type(table_exp).__name__}")
        
        # Get VALUES or SELECT
        values_exp = ast.expression
        
        if isinstance(values_exp, exp.Values):
            # INSERT INTO ... VALUES
            rows = []
            for tuple_exp in values_exp.expressions:
                row = {}
                for i, val_exp in enumerate(tuple_exp.expressions):
                    col_name = table.columns.keys()[i]
                    value = self._extract_literal(val_exp)
                    row[col_name] = value
                rows.append(row)
            return insert(table).values(rows)
        
        elif isinstance(values_exp, exp.Select):
            # INSERT INTO ... SELECT
            select_stmt = self._translate_select(values_exp)
            col_names = [col.name for col in table.columns]
            return insert(table).from_select(col_names, select_stmt)
        
        else:
            raise SQLTranslationError(f"Unsupported INSERT source: {type(values_exp).__name__}")
    
    def _translate_update(self, ast: exp.Update) -> Update:
        """Translate UPDATE statement to SQLAlchemy."""
        # Get table
        table_exp = ast.this
        if isinstance(table_exp, exp.Table):
            table_name = str(table_exp.this)
            table = self._get_table(table_name)
        else:
            raise SQLTranslationError(f"Unsupported UPDATE table: {type(table_exp).__name__}")
        
        # Build UPDATE statement
        stmt = update(table)
        
        # Add SET values
        set_values = {}
        for set_exp in ast.expressions:
            if isinstance(set_exp, exp.EQ):
                col_name = str(set_exp.this)
                value = self._extract_literal(set_exp.expression)
                set_values[col_name] = value
        
        stmt = stmt.values(**set_values)
        
        # Add WHERE clause
        where = ast.find(exp.Where)
        if where:
            condition = self._translate_expression(where.this, table)
            stmt = stmt.where(condition)
        
        return stmt
    
    def _translate_delete(self, ast: exp.Delete) -> Delete:
        """Translate DELETE statement to SQLAlchemy."""
        # Get table
        table_exp = ast.this
        if isinstance(table_exp, exp.Table):
            table_name = str(table_exp.this)
            table = self._get_table(table_name)
        else:
            raise SQLTranslationError(f"Unsupported DELETE table: {type(table_exp).__name__}")
        
        stmt = delete(table)
        
        # Add WHERE clause
        where = ast.find(exp.Where)
        if where:
            condition = self._translate_expression(where.this, table)
            stmt = stmt.where(condition)
        
        return stmt
    
    def _translate_create(self, ast: exp.Create) -> Any:
        """Translate CREATE statement (raises error - not supported via translator)."""
        raise SQLTranslationError(
            "CREATE statements should be handled by MockSparkSession.catalog, "
            "not through spark.sql() translation"
        )
    
    def _translate_drop(self, ast: exp.Drop) -> Any:
        """Translate DROP statement (raises error - not supported via translator)."""
        raise SQLTranslationError(
            "DROP statements should be handled by MockSparkSession.catalog, "
            "not through spark.sql() translation"
        )
    
    def _translate_expression(self, expr: exp.Expression, table: Table) -> ColumnElement:
        """
        Translate sqlglot expression to SQLAlchemy column element.
        
        Args:
            expr: sqlglot expression
            table: SQLAlchemy table for column references
            
        Returns:
            SQLAlchemy column element
        """
        # Column reference
        if isinstance(expr, exp.Column):
            col_name = str(expr.this)
            return table.c[col_name]
        
        # Literal values
        elif isinstance(expr, exp.Literal):
            return literal(self._extract_literal(expr))
        
        # Binary operations
        elif isinstance(expr, exp.EQ):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left == right
        
        elif isinstance(expr, exp.NEQ):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left != right
        
        elif isinstance(expr, exp.GT):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left > right
        
        elif isinstance(expr, exp.GTE):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left >= right
        
        elif isinstance(expr, exp.LT):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left < right
        
        elif isinstance(expr, exp.LTE):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left <= right
        
        # Logical operations
        elif isinstance(expr, exp.And):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return and_(left, right)
        
        elif isinstance(expr, exp.Or):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return or_(left, right)
        
        elif isinstance(expr, exp.Not):
            operand = self._translate_expression(expr.this, table)
            return not_(operand)
        
        # Functions
        elif isinstance(expr, exp.Func):
            return self._translate_function(expr, table)
        
        # Alias
        elif isinstance(expr, exp.Alias):
            col = self._translate_expression(expr.this, table)
            alias = str(expr.alias)
            return col.label(alias)
        
        # CASE WHEN
        elif isinstance(expr, exp.Case):
            return self._translate_case(expr, table)
        
        # Arithmetic operations
        elif isinstance(expr, exp.Add):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left + right
        
        elif isinstance(expr, exp.Sub):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left - right
        
        elif isinstance(expr, exp.Mul):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left * right
        
        elif isinstance(expr, exp.Div):
            left = self._translate_expression(expr.this, table)
            right = self._translate_expression(expr.expression, table)
            return left / right
        
        else:
            raise SQLTranslationError(f"Unsupported expression type: {type(expr).__name__}")
    
    def _translate_function(self, expr: exp.Func, table: Table) -> ColumnElement:
        """Translate SQL function to SQLAlchemy function."""
        func_name = expr.__class__.__name__.lower()
        
        # Aggregate functions
        if isinstance(expr, exp.Count):
            if expr.this and not isinstance(expr.this, exp.Star):
                col = self._translate_expression(expr.this, table)
                distinct = expr.args.get('distinct')
                if distinct:
                    return func.count(col.distinct())
                return func.count(col)
            else:
                return func.count()
        
        elif isinstance(expr, exp.Sum):
            col = self._translate_expression(expr.this, table)
            return func.sum(col)
        
        elif isinstance(expr, exp.Avg):
            col = self._translate_expression(expr.this, table)
            return func.avg(col)
        
        elif isinstance(expr, exp.Min):
            col = self._translate_expression(expr.this, table)
            return func.min(col)
        
        elif isinstance(expr, exp.Max):
            col = self._translate_expression(expr.this, table)
            return func.max(col)
        
        # String functions
        elif isinstance(expr, exp.Upper):
            col = self._translate_expression(expr.this, table)
            return func.upper(col)
        
        elif isinstance(expr, exp.Lower):
            col = self._translate_expression(expr.this, table)
            return func.lower(col)
        
        elif isinstance(expr, exp.Concat):
            args = [self._translate_expression(arg, table) for arg in expr.expressions]
            return func.concat(*args)
        
        elif isinstance(expr, exp.Substring):
            col = self._translate_expression(expr.this, table)
            start = self._translate_expression(expr.args.get('start'), table)
            length = expr.args.get('length')
            if length:
                length_val = self._translate_expression(length, table)
                return func.substr(col, start, length_val)
            return func.substr(col, start)
        
        elif isinstance(expr, exp.Length):
            col = self._translate_expression(expr.this, table)
            return func.length(col)
        
        elif isinstance(expr, exp.Trim):
            col = self._translate_expression(expr.this, table)
            return func.trim(col)
        
        # Math functions
        elif isinstance(expr, exp.Abs):
            col = self._translate_expression(expr.this, table)
            return func.abs(col)
        
        elif isinstance(expr, exp.Round):
            col = self._translate_expression(expr.this, table)
            decimals = expr.args.get('decimals')
            if decimals:
                dec_val = self._extract_literal(decimals)
                return func.round(col, dec_val)
            return func.round(col)
        
        elif isinstance(expr, exp.Ceil):
            col = self._translate_expression(expr.this, table)
            return func.ceil(col)
        
        elif isinstance(expr, exp.Floor):
            col = self._translate_expression(expr.this, table)
            return func.floor(col)
        
        elif isinstance(expr, exp.Sqrt):
            col = self._translate_expression(expr.this, table)
            return func.sqrt(col)
        
        # Date functions
        elif isinstance(expr, exp.CurrentDate):
            return func.current_date()
        
        elif isinstance(expr, exp.CurrentTimestamp):
            return func.current_timestamp()
        
        elif isinstance(expr, exp.Year):
            col = self._translate_expression(expr.this, table)
            return func.year(col)
        
        elif isinstance(expr, exp.Month):
            col = self._translate_expression(expr.this, table)
            return func.month(col)
        
        elif isinstance(expr, exp.Day):
            col = self._translate_expression(expr.this, table)
            return func.day(col)
        
        # COALESCE
        elif isinstance(expr, exp.Coalesce):
            args = [self._translate_expression(arg, table) for arg in expr.expressions]
            return func.coalesce(*args)
        
        # CAST
        elif isinstance(expr, exp.Cast):
            col = self._translate_expression(expr.this, table)
            target_type = self._get_cast_type(expr.to)
            return cast(col, target_type)
        
        else:
            # Generic function - try to map using function mapper
            args = []
            if expr.this:
                args.append(self._translate_expression(expr.this, table))
            if hasattr(expr, 'expressions'):
                for arg in expr.expressions:
                    args.append(self._translate_expression(arg, table))
            
            # Try to get function from mapper
            try:
                sql_func = get_sqlalchemy_function(func_name)
                return sql_func(*args)
            except ValueError:
                # Fall back to generic func attribute access
                if hasattr(func, func_name):
                    return getattr(func, func_name)(*args)
                raise SQLTranslationError(f"Unsupported function: {func_name}")
    
    def _get_cast_type(self, type_exp: exp.Expression) -> Any:
        """Get SQLAlchemy type for CAST operation."""
        from sqlalchemy import Integer, String, Float, Boolean, Date, DateTime
        
        type_str = str(type_exp).upper()
        
        if 'INT' in type_str:
            return Integer
        elif 'STRING' in type_str or 'VARCHAR' in type_str:
            return String
        elif 'FLOAT' in type_str or 'DOUBLE' in type_str:
            return Float
        elif 'BOOL' in type_str:
            return Boolean
        elif 'DATE' in type_str and 'TIME' not in type_str:
            return Date
        elif 'TIMESTAMP' in type_str:
            return DateTime
        else:
            return String  # Default fallback
    
    def _translate_case(self, expr: exp.Case, table: Table) -> ColumnElement:
        """Translate CASE WHEN to SQLAlchemy case()."""
        whens = []
        
        for if_exp in expr.args.get('ifs', []):
            condition = self._translate_expression(if_exp.this, table)
            value = self._translate_expression(if_exp.args.get('true'), table)
            whens.append((condition, value))
        
        else_value = None
        if expr.args.get('default'):
            else_value = self._translate_expression(expr.args['default'], table)
        
        return case(*whens, else_=else_value)
    
    def _extract_literal(self, expr: exp.Expression) -> Any:
        """Extract Python value from literal expression."""
        if isinstance(expr, exp.Literal):
            val_str = str(expr.this)
            
            # Try to infer type
            if expr.is_int:
                return int(val_str)
            elif expr.is_number:
                return float(val_str)
            elif val_str.lower() == 'null':
                return None
            elif val_str.lower() in ('true', 'false'):
                return val_str.lower() == 'true'
            else:
                # String - remove quotes
                return val_str.strip("'\"")
        
        elif isinstance(expr, exp.Null):
            return None
        
        elif isinstance(expr, exp.Boolean):
            return str(expr.this).lower() == 'true'
        
        else:
            # Fallback - convert to string
            return str(expr)

