# 🐼 Pica - Simple SQL Interface for Pandas DataFrames 

Pica is a lightweight Python library that provides a SQL interface for Pandas DataFrames, following the Python DB-API 2.0 specification. It allows you to interact with your DataFrames using familiar SQL syntax while leveraging the power of Pandas under the hood.

## ✨ Features

- 🔍 SQL-like interface for Pandas DataFrames
- 📊 Supports common SQL operations
- 🐍 Python DB-API 2.0 compliant
- 🚀 Easy to use and integrate
- 📝 CSV file support for persistence

## 🛠️ Installation

```bash
pip install pica
```

## 🎯 Quick Start

```python
import pica
import pandas as pd

# Create a connection
conn = pica.connect()

# Register a DataFrame as a table
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

conn.register_table('users', df, {
    'id': 'INTEGER',
    'name': 'TEXT',
    'age': 'INTEGER'
})

# Execute SQL queries
cursor = conn.cursor()
cursor.execute("SELECT name, age FROM users WHERE age > 25")
results = cursor.fetchall()
print(results)  # [('Bob', 30), ('Charlie', 35)]
```

## 🔥 Supported SQL Operations

### SELECT
- Basic SELECT with column selection
- WHERE clause with comparison operators (=, >, <, >=, <=, !=)
- GROUP BY with aggregate functions (COUNT, SUM, AVG, MAX, MIN)
- ORDER BY (ASC/DESC)
- JOIN operations
- Aliases (AS)

Example:
```sql
SELECT name, AVG(age) as avg_age 
FROM users 
WHERE age > 25 
GROUP BY name 
ORDER BY avg_age DESC
```

### INSERT
- Basic INSERT INTO with VALUES

Example:
```sql
INSERT INTO users (name, age) VALUES ('David', 28)
```

### UPDATE
- UPDATE with WHERE clause

Example:
```sql
UPDATE users SET age = 29 WHERE name = 'Alice'
```

### DELETE
- DELETE with WHERE clause

Example:
```sql
DELETE FROM users WHERE age < 25
```

## 📊 Supported Data Types

- INTEGER
- REAL
- BOOLEAN
- DATE
- TEXT

## 🔄 Transaction Support

```python
conn = pica.connect()
try:
    # Perform operations
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET age = 26 WHERE name = 'Alice'")
    conn.commit()
except:
    conn.rollback()
finally:
    conn.close()
```

## 📝 License

MIT License

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.