"""
SQLModel Refactoring Examples (PREFERRED)
Demonstrates how to replace raw SQL with SQLModel (built on SQLAlchemy + Pydantic)

SQLModel provides:
- Type-safe table definitions
- Pydantic validation
- Better IDE support
- Simpler syntax than SQLAlchemy
- Automatic data validation

This is the RECOMMENDED approach for Mock Spark refactoring.
"""

from sqlmodel import SQLModel, Field, Session, create_engine, select, and_, or_, desc, func, col
from typing import Optional, List
from datetime import datetime
from enum import Enum


# ============================================================================
# Example 1: Table Definition (from sql_builder.py and duckdb.py)
# ============================================================================


def create_table_old_way(connection, table_name: str, columns: list):
    """
    OLD WAY: Raw SQL with string interpolation
    Issues:
    - SQL injection risk
    - No type checking
    - No validation
    - Hard to test
    """
    column_defs = []
    for col_name, col_type in columns:
        column_defs.append(f"{col_name} {col_type}")

    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
    connection.execute(create_sql)


def create_table_new_way_sqlmodel():
    """
    NEW WAY: SQLModel table definition (RECOMMENDED)
    Benefits:
    - Type-safe with Python type hints
    - Pydantic validation
    - Auto-generates SQL
    - IDE autocomplete
    - Runtime validation
    """

    class User(SQLModel, table=True):
        """Type-safe user table with validation."""

        id: Optional[int] = Field(default=None, primary_key=True)
        name: str = Field(min_length=1, max_length=100)
        age: int = Field(ge=0, le=150)  # Age between 0 and 150
        email: Optional[str] = Field(default=None, regex=r"^[\w\.-]+@[\w\.-]+\.\w+$")
        created_at: datetime = Field(default_factory=datetime.utcnow)

    # Create table automatically
    engine = create_engine("duckdb:///:memory:")
    SQLModel.metadata.create_all(engine)

    return User, engine


# ============================================================================
# Example 2: Data Insertion with Validation (from duckdb.py)
# ============================================================================


def insert_data_old_way(connection, table_name: str, rows: list):
    """
    OLD WAY: Manual placeholders, no validation
    Issues:
    - No data validation
    - Manual type handling
    - Error-prone
    """
    for row in rows:
        values = list(row.values())
        placeholders = ", ".join(["?" for _ in values])
        connection.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", values)


def insert_data_new_way_sqlmodel():
    """
    NEW WAY: SQLModel with automatic validation
    Benefits:
    - Automatic Pydantic validation
    - Type checking
    - Clear error messages
    - Batch insert support
    """

    class User(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str
        age: int = Field(ge=0)

    engine = create_engine("duckdb:///:memory:")
    SQLModel.metadata.create_all(engine)

    # Insert with validation
    with Session(engine) as session:
        # This will validate automatically
        users = [
            User(name="Alice", age=25),
            User(name="Bob", age=30),
            # User(name="", age=-5)  # Would raise ValidationError!
        ]

        for user in users:
            session.add(user)

        session.commit()

        # Or batch insert
        session.add_all(users)
        session.commit()

    return engine


# ============================================================================
# Example 3: SELECT Query with Type-Safe Results (from duckdb.py)
# ============================================================================


def query_with_filter_old_way(connection, table_name: str, filter_expr: str):
    """
    OLD WAY: String concatenation, untyped results
    Issues:
    - SQL injection if filter_expr is user input
    - Results are tuples, not typed
    - Hard to compose
    """
    if filter_expr:
        query = f"SELECT * FROM {table_name} WHERE {filter_expr}"
    else:
        query = f"SELECT * FROM {table_name}"

    result = connection.execute(query).fetchall()
    return result  # Returns List[Tuple]


def query_with_filter_new_way_sqlmodel():
    """
    NEW WAY: SQLModel with type-safe results
    Benefits:
    - Type-safe queries
    - Results are model instances
    - IDE autocomplete on results
    - SQL injection safe
    """

    class User(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str
        age: int

    engine = create_engine("duckdb:///:memory:")
    SQLModel.metadata.create_all(engine)

    # Setup test data
    with Session(engine) as session:
        session.add_all([User(name="Alice", age=25), User(name="Bob", age=30), User(name="Charlie", age=35)])
        session.commit()

    # Query with type-safe results
    with Session(engine) as session:
        # Simple query
        stmt = select(User).where(User.age > 25)
        results: List[User] = session.exec(stmt).all()

        # IDE knows results is List[User]
        for user in results:
            print(f"{user.name} is {user.age}")  # Autocomplete works!

        # Complex query
        stmt = select(User).where(and_(User.age > 25, User.name.like("B%")))  # type: ignore
        results = session.exec(stmt).all()

    return results


# ============================================================================
# Example 4: Dynamic Table from MockSpark Schema
# ============================================================================


def create_dynamic_table_from_mock_schema():
    """
    Create SQLModel table dynamically from MockSpark schema.
    This is useful for the Mock Spark codebase.
    """
    from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType

    # Mock schema from MockSpark
    mock_schema = MockStructType(
        [MockStructField("id", IntegerType()), MockStructField("name", StringType(), nullable=True)]
    )

    # Create SQLModel class dynamically
    def mock_to_sqlmodel_type(mock_type):
        """Convert MockSpark type to Python type."""
        type_name = type(mock_type).__name__
        mapping = {
            "StringType": str,
            "IntegerType": int,
            "LongType": int,
            "DoubleType": float,
            "BooleanType": bool,
        }
        return mapping.get(type_name, str)

    # Build class attributes
    annotations = {}
    class_attrs = {}

    for field in mock_schema.fields:
        python_type = mock_to_sqlmodel_type(field.dataType)

        if field.nullable:
            annotations[field.name] = Optional[python_type]
            class_attrs[field.name] = Field(default=None)
        else:
            annotations[field.name] = python_type

    # Create the class dynamically
    DynamicTable = type(
        "DynamicTable",
        (SQLModel,),
        {"__tablename__": "dynamic_table", "__annotations__": annotations, "table": True, **class_attrs},
    )

    engine = create_engine("duckdb:///:memory:")
    SQLModel.metadata.create_all(engine)

    return DynamicTable, engine


# ============================================================================
# Example 5: Complex Query with Aggregations (from sql_builder.py)
# ============================================================================


def aggregate_old_way(table_name: str, group_cols: list, agg_col: str):
    """
    OLD WAY: Raw aggregate SQL
    Issues:
    - Manual GROUP BY syntax
    - No validation
    - String manipulation
    """
    group_by = ", ".join(f'"{col}"' for col in group_cols)
    sql = f"""
        SELECT {group_by},
               COUNT(*) as count,
               AVG("{agg_col}") as avg_value,
               MAX("{agg_col}") as max_value
        FROM "{table_name}"
        GROUP BY {group_by}
    """
    return sql


def aggregate_new_way_sqlmodel():
    """
    NEW WAY: SQLModel with type-safe aggregations
    Benefits:
    - Type-safe aggregates
    - Composable queries
    - Result models
    """

    class Employee(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str
        department: str
        salary: float

    engine = create_engine("duckdb:///:memory:")
    SQLModel.metadata.create_all(engine)

    # Setup test data
    with Session(engine) as session:
        employees = [
            Employee(name="Alice", department="Engineering", salary=80000),
            Employee(name="Bob", department="Sales", salary=75000),
            Employee(name="Charlie", department="Engineering", salary=90000),
            Employee(name="Diana", department="Sales", salary=85000),
        ]
        session.add_all(employees)
        session.commit()

    # Aggregate query with type-safe results
    with Session(engine) as session:
        stmt = (
            select(
                Employee.department,
                func.count(Employee.id).label("count"),
                func.avg(Employee.salary).label("avg_salary"),
                func.max(Employee.salary).label("max_salary"),
            )
            .group_by(Employee.department)
            .order_by(desc("avg_salary"))
        )

        results = session.exec(stmt).all()

        for row in results:
            print(f"Dept: {row[0]}, Count: {row[1]}, Avg: ${row[2]:.2f}, Max: ${row[3]:.2f}")

    return results


# ============================================================================
# Example 6: Table with Enums and Constraints
# ============================================================================


class Status(str, Enum):
    """Status enum for type safety."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"


class UserAccount(SQLModel, table=True):
    """
    Advanced SQLModel example with enums, constraints, and relationships.
    Shows the power of SQLModel for complex schemas.
    """

    __tablename__ = "user_accounts"

    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(unique=True, index=True, min_length=3, max_length=50)
    email: str = Field(unique=True, regex=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    age: int = Field(ge=13, le=120)  # Must be between 13 and 120
    status: Status = Field(default=Status.ACTIVE)
    balance: float = Field(default=0.0, ge=0)  # Non-negative
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

    def __repr__(self):
        return f"<User {self.username} ({self.status})>"


# ============================================================================
# Example 7: JOIN Operations with Type Safety
# ============================================================================


def join_tables_new_way_sqlmodel():
    """
    SQLModel join example with type-safe results.
    """

    class Department(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str
        location: str

    class Employee(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str
        department_id: int = Field(foreign_key="department.id")
        salary: float

    engine = create_engine("duckdb:///:memory:")
    SQLModel.metadata.create_all(engine)

    # Setup data
    with Session(engine) as session:
        eng_dept = Department(name="Engineering", location="SF")
        sales_dept = Department(name="Sales", location="NY")
        session.add_all([eng_dept, sales_dept])
        session.commit()

        employees = [
            Employee(name="Alice", department_id=eng_dept.id, salary=80000),
            Employee(name="Bob", department_id=sales_dept.id, salary=75000),
        ]
        session.add_all(employees)
        session.commit()

    # Join query
    with Session(engine) as session:
        stmt = select(Employee, Department).join(Department, Employee.department_id == Department.id)

        results = session.exec(stmt).all()

        for employee, department in results:
            print(f"{employee.name} works in {department.name} at {department.location}")

    return results


# ============================================================================
# Example 8: Replacing sql_builder.py with SQLModel Query Builder
# ============================================================================


class SQLModelQueryBuilder:
    """
    Type-safe query builder for Mock Spark using SQLModel.
    Replaces sql_builder.py with better type safety.
    """

    def __init__(self, model: type[SQLModel]):
        self.model = model
        self._stmt = select(model)
        self._filters = []
        self._order_by = []

    def filter(self, *conditions):
        """Add WHERE conditions."""
        self._filters.extend(conditions)
        return self

    def order_by(self, *columns):
        """Add ORDER BY."""
        self._order_by.extend(columns)
        return self

    def build(self):
        """Build the final query."""
        stmt = self._stmt

        if self._filters:
            stmt = stmt.where(and_(*self._filters))

        if self._order_by:
            stmt = stmt.order_by(*self._order_by)

        return stmt

    def execute(self, session: Session):
        """Execute and return results."""
        stmt = self.build()
        return session.exec(stmt).all()


def test_query_builder():
    """Test the SQLModel query builder."""

    class User(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        name: str
        age: int

    engine = create_engine("duckdb:///:memory:")
    SQLModel.metadata.create_all(engine)

    # Setup data
    with Session(engine) as session:
        users = [User(name="Alice", age=25), User(name="Bob", age=30), User(name="Charlie", age=35)]
        session.add_all(users)
        session.commit()

    # Use query builder
    with Session(engine) as session:
        builder = SQLModelQueryBuilder(User)
        results = builder.filter(User.age > 25).order_by(desc(User.age)).execute(session)

        print("Query builder results:")
        for user in results:
            print(f"  {user.name}: {user.age}")

    return results


# ============================================================================
# Example 9: Replacing DuckDB Storage Backend
# ============================================================================


class DuckDBTableSQLModel:
    """
    Type-safe DuckDB table implementation using SQLModel.
    Replaces storage/backends/duckdb.py DuckDBTable class.
    """

    def __init__(self, model: type[SQLModel], engine):
        self.model = model
        self.engine = engine

        # Create table
        SQLModel.metadata.create_all(engine)

    def insert_data(self, data: List[dict], mode: str = "append"):
        """Type-safe data insertion with Pydantic validation."""
        with Session(self.engine) as session:
            if mode == "overwrite":
                # Delete existing data
                session.exec(select(self.model)).all()
                for obj in session.exec(select(self.model)).all():
                    session.delete(obj)
                session.commit()

            # Insert with validation
            objects = [self.model(**row) for row in data]  # Pydantic validates here!
            session.add_all(objects)
            session.commit()

    def query_data(self, filter_expr=None) -> List:
        """Type-safe querying."""
        with Session(self.engine) as session:
            stmt = select(self.model)

            if filter_expr is not None:
                stmt = stmt.where(filter_expr)

            return session.exec(stmt).all()

    def get_count(self) -> int:
        """Get row count."""
        with Session(self.engine) as session:
            return session.exec(select(func.count()).select_from(self.model)).one()


# ============================================================================
# Example 10: Complete Mock Spark Refactoring Example
# ============================================================================


def complete_mocksp park_refactoring_example():
    """
    Complete example showing how Mock Spark can use SQLModel.
    """
    print("=== Complete Mock Spark SQLModel Example ===\n")

    # 1. Define table from MockSpark schema
    print("1. Creating type-safe table from MockSpark schema...")

    class Employee(SQLModel, table=True):
        """Employee table with full type safety and validation."""

        id: Optional[int] = Field(default=None, primary_key=True)
        name: str = Field(min_length=1, max_length=100)
        department: str
        salary: float = Field(gt=0)  # Must be positive
        hire_date: datetime = Field(default_factory=datetime.utcnow)

    engine = create_engine("duckdb:///:memory:")
    SQLModel.metadata.create_all(engine)
    print("   ✓ Table created with type safety\n")

    # 2. Insert data with validation
    print("2. Inserting data with automatic validation...")
    with Session(engine) as session:
        employees = [
            Employee(name="Alice", department="Engineering", salary=80000),
            Employee(name="Bob", department="Sales", salary=75000),
            Employee(name="Charlie", department="Engineering", salary=90000),
        ]
        session.add_all(employees)
        session.commit()
    print("   ✓ Data inserted and validated\n")

    # 3. Type-safe queries
    print("3. Running type-safe queries...")
    with Session(engine) as session:
        # Simple filter
        stmt = select(Employee).where(Employee.salary > 75000)
        high_earners = session.exec(stmt).all()
        print(f"   High earners: {[e.name for e in high_earners]}")

        # Aggregation
        stmt = select(
            Employee.department, func.avg(Employee.salary).label("avg_salary")
        ).group_by(Employee.department)

        results = session.exec(stmt).all()
        print("\n   Department averages:")
        for dept, avg in results:
            print(f"     {dept}: ${avg:,.2f}")

    # 4. Update with validation
    print("\n4. Updating data with validation...")
    with Session(engine) as session:
        alice = session.exec(select(Employee).where(Employee.name == "Alice")).first()
        if alice:
            alice.salary = 85000  # Type-checked!
            # alice.salary = "invalid"  # Would fail type check!
            session.add(alice)
            session.commit()
    print("   ✓ Data updated\n")

    print("✨ Example completed successfully!")
    print("\n💡 Key Benefits:")
    print("   • Full type safety with Python type hints")
    print("   • Automatic Pydantic validation")
    print("   • IDE autocomplete on all operations")
    print("   • Clear error messages")
    print("   • Cleaner, more maintainable code")


# ============================================================================
# Main Execution
# ============================================================================

if __name__ == "__main__":
    print("=== SQLModel Refactoring Examples (RECOMMENDED) ===\n")
    print("SQLModel = SQLAlchemy + Pydantic Type Safety\n")

    print("Example 1: Table Creation")
    print("-" * 50)
    User, engine = create_table_new_way_sqlmodel()
    print(f"✓ Created {User.__tablename__} with type-safe fields\n")

    print("Example 2: Data Insertion with Validation")
    print("-" * 50)
    insert_data_new_way_sqlmodel()
    print("✓ Data inserted with automatic validation\n")

    print("Example 3: Type-Safe Queries")
    print("-" * 50)
    results = query_with_filter_new_way_sqlmodel()
    print(f"✓ Found {len(results)} users\n")

    print("Example 4: Aggregations")
    print("-" * 50)
    aggregate_new_way_sqlmodel()
    print()

    print("Example 5: Query Builder")
    print("-" * 50)
    test_query_builder()
    print()

    print("\n" + "=" * 60)
    complete_mocksp park_refactoring_example()
    print("=" * 60)
