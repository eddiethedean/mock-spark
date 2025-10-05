Generating comprehensive README examples...
============================================================

## 📊 Basic DataFrame Operations

**Sample Data:**
```python
data = [
    {'name': 'Alice', 'age': 25, 'salary': 55000, 'department': 'Sales', 'active': True},"
    {'name': 'Bob', 'age': 30, 'salary': 75000, 'department': 'Sales', 'active': True},"
    {'name': 'Charlie', 'age': 35, 'salary': 80000, 'department': 'Engineering', 'active': False},"
    # ... more data
]
df = spark.createDataFrame(data)
```

**Basic Operations:**
```python
# Show all data
df.show()
```

**Output:**
```
MockDataFrame[5 rows, 5 columns]

active age department  name    salary
True     25    Sales         Alice     55000   
True     30    Sales         Bob       75000   
False    35    Engineering   Charlie   80000   
True     28    Marketing     David     65000   
True     42    Engineering   Eve       95000   
```

```python
# Filter active employees
active_employees = df.filter(F.col('active') == True)
active_employees.show()
```

**Output:**
```
MockDataFrame[4 rows, 5 columns]

active age department  name  salary
True     25    Sales         Alice   55000   
True     30    Sales         Bob     75000   
True     28    Marketing     David   65000   
True     42    Engineering   Eve     95000   
```

```python
# Select specific columns
df.select('name', 'department', 'salary').show()
```

**Output:**
```
MockDataFrame[5 rows, 3 columns]

name    department  salary
Alice     Sales         55000   
Bob       Sales         75000   
Charlie   Engineering   80000   
David     Marketing     65000   
Eve       Engineering   95000   
```

## 📈 Aggregation & Grouping

**Department Statistics:**
```python
dept_stats = df.groupBy('department').agg(
    F.count('*').alias('employee_count'),
    F.avg('salary').alias('avg_salary'),
    F.max('salary').alias('max_salary'),
    F.min('salary').alias('min_salary')
).orderBy(F.desc('avg_salary'))
dept_stats.show()
```

**Output:**
```
MockDataFrame[3 rows, 5 columns]

department  employee_count avg_salary max_salary min_salary
Engineering   2                87500.0      95000        80000       
Sales         2                65000.0      75000        55000       
Marketing     1                65000.0      65000        65000       
```

**Age-based Analysis:**
```python
# Create age groups
age_groups = df.withColumn(
    'age_group',
    F.when(F.col('age') < 30, 'Young')
     .when(F.col('age') < 40, 'Middle')
     .otherwise('Senior')
)
age_groups.groupBy('age_group').agg(
    F.count('*').alias('count'),
    F.avg('salary').alias('avg_salary')
).show()
```

**Output:**
```
MockDataFrame[1 rows, 3 columns]

age_group            count avg_salary
<mock_spark.funct...   5       74000.0     
```

## 🪟 Window Functions

**Salary Rankings by Department:**
```python
window_spec = Window.partitionBy('department').orderBy(F.desc('salary'))
rankings = df.select(
    F.col('*'),
    F.row_number().over(window_spec).alias('row_number'),
    F.rank().over(window_spec).alias('rank'),
    F.dense_rank().over(window_spec).alias('dense_rank'),
    F.percent_rank().over(window_spec).alias('percent_rank')
)
rankings.show()
```

**Output:**
```
MockDataFrame[5 rows, 8 columns]

department  hire_date  name    salary row_number rank dense_rank percent_rank
Sales         2020-01-15   Alice     55000    2            2      2            None          
Sales         2019-06-10   Bob       75000    1            1      1            None          
Engineering   2021-03-20   Charlie   80000    2            2      2            None          
Marketing     2020-11-05   David     65000    1            1      1            None          
Engineering   2018-09-12   Eve       95000    1            1      1            None          
```

**Salary Comparisons:**
```python
comparisons = df.select(
    F.col('*'),
    F.lag('salary', 1).over(window_spec).alias('prev_salary'),
    F.lead('salary', 1).over(window_spec).alias('next_salary'),
    F.avg('salary').over(window_spec).alias('dept_avg_salary')
)
comparisons.show()
```

**Output:**
```
MockDataFrame[5 rows, 7 columns]

department  hire_date  name    salary prev_salary next_salary dept_avg_salary
Sales         2020-01-15   Alice     55000    75000         None          65000.0          
Sales         2019-06-10   Bob       75000    None          55000         75000.0          
Engineering   2021-03-20   Charlie   80000    95000         None          87500.0          
Marketing     2020-11-05   David     65000    None          None          65000.0          
Engineering   2018-09-12   Eve       95000    None          80000         95000.0          
```

## 🔤 String Functions

**String Manipulation:**
```python
string_ops = df.select(
    F.col('name'),
    F.upper(F.col('name')).alias('upper_name'),
    F.lower(F.col('name')).alias('lower_name'),
    F.length(F.col('name')).alias('name_length'),
    F.trim(F.col('name')).alias('trimmed_name')
)
string_ops.show()
```

**Output:**
```
MockDataFrame[3 rows, 5 columns]

name          upper_name    lower_name    name_length trimmed_name 
Alice Johnson   ALICE JOHNSON   alice johnson   13            Alice Johnson  
Bob Smith       BOB SMITH       bob smith       9             Bob Smith      
Charlie Brown   CHARLIE BROWN   charlie brown   13            Charlie Brown  
```

**Advanced String Operations:**
```python
advanced_strings = df.select(
    F.col('email'),
    F.split(F.col('email'), '@').alias('email_parts'),
    F.regexp_replace(F.col('email'), '@company.com', '@newcorp.com').alias('new_email'),
    F.substring(F.col('phone'), 1, 3).alias('area_code')
)
advanced_strings.show()
```

**Output:**
```
MockDataFrame[3 rows, 4 columns]

email               email_parts          new_email           area_code
alice@company.com     ['alice', 'compan...   alice@newcorp.com     555-1234   
bob@company.com       ['bob', 'company....   bob@newcorp.com       555-5678   
charlie@company.com   ['charlie', 'comp...   charlie@newcorp.com   555-9012   
```

## 🔢 Mathematical Functions

**Mathematical Operations:**
```python
math_ops = df.select(
    F.col('value'),
    F.abs(F.col('value')).alias('abs_value'),
    F.round(F.col('value'), 1).alias('rounded'),
    F.ceil(F.col('value')).alias('ceiling'),
    F.floor(F.col('value')).alias('floor'),
    F.sqrt(F.abs(F.col('value'))).alias('sqrt_abs')
)
math_ops.show()
```

**Output:**
```
MockDataFrame[3 rows, 6 columns]

value abs_value rounded ceiling floor sqrt_abs          
25.7    25.7        26.0      26        25      5.06951674225463    
-15.3   15.3        -15.0     -15       -16     3.9115214431215892  
100.0   100.0       100.0     100       100     10.0                
```

**Business Calculations:**
```python
business_calc = df.select(
    F.col('*'),
    (F.col('price') * F.col('quantity')).alias('total_revenue'),
    F.greatest(F.col('price'), F.lit(100)).alias('min_price'),
    F.least(F.col('price'), F.lit(200)).alias('max_price')
)
business_calc.show()
```

**Output:**
```
MockDataFrame[3 rows, 6 columns]

price quantity value total_revenue min_price max_price
99.99   5          25.7    499.95          99.99       99.99      
149.5   2          -15.3   299.0           149.5       149.5      
75.25   10         100.0   752.5           75.25       75.25      
```

## 🗄️ SQL Operations

**SQL Queries:**
```python
# Create temporary view
df.createOrReplaceTempView('employees')

# Simple query
result1 = spark.sql('SELECT name, salary FROM employees WHERE salary > 60000')
result1.show()
```

**Output:**
```
MockDataFrame[3 rows, 2 columns]

name    salary
Alice     55000   
Bob       75000   
Charlie   80000   
```

```python
# Order by query
result2 = spark.sql('SELECT name, department, salary FROM employees ORDER BY salary DESC')
result2.show()
```

**Output:**
```
MockDataFrame[3 rows, 3 columns]

name    department  salary
Alice     Sales         55000   
Bob       Sales         75000   
Charlie   Engineering   80000   
```

## 🔗 DataFrame Joins

**Inner Join:**
```python
inner_join = employees.join(departments, employees.dept_id == departments.id, 'inner')
inner_join.select('name', 'dept_name', 'budget').show()
```

**Output:**
```
