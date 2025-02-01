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
pip install pica-dbapi
```

## 🎯 Quick Start

```python
import os
import sys
import pandas as pd

# Add the parent directory to the system path to import pica
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pica

def main():
    """Basic example of using Pica with CSV files and DataFrames
    CSVファイルとDataFrameを使用したPicaの基本的な使用例"""
    # Create sample data
    # サンプルデータの作成
    users_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 22],
        'department': ['Sales', 'IT', 'Sales', 'Marketing', 'IT']
    }
    
    orders_data = {
        'order_id': [1, 2, 3, 4, 5],
        'user_id': [1, 2, 1, 3, 5],
        'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Printer'],
        'amount': [1000, 20, 50, 200, 150]
    }

    # Define schemas
    # スキーマの定義
    users_schema = {
        'id': 'INTEGER',
        'name': 'TEXT',
        'age': 'INTEGER',
        'department': 'TEXT'
    }
    
    orders_schema = {
        'order_id': 'INTEGER',
        'user_id': 'INTEGER',
        'product': 'TEXT',
        'amount': 'INTEGER'
    }

    # Create DataFrames
    # DataFrameを作成
    users_df = pd.DataFrame(users_data)
    orders_df = pd.DataFrame(orders_data)

    # Initialize connection with dataframes
    # DataFrameを指定して接続を初期化
    initial_tables = {
        "users": users_df,
        "orders": orders_df
    }
    conn = pica.connect(dataframes=initial_tables)
    cursor = conn.cursor()

    # Register schemas
    # スキーマを登録
    conn.register_schema("users", users_schema)
    conn.register_schema("orders", orders_schema)

    # Example 1: Basic SELECT with WHERE
    # 基本的なSELECTとWHEREの例
    print("\n=== Example 1: Basic SELECT with WHERE ===")
    cursor.execute("SELECT name, age FROM users WHERE age > 25")
    results = cursor.fetchall()
    print("Users over 25:")
    for row in results:
        print(row)

    # Example 2: GROUP BY with aggregation
    # GROUP BYと集計の例
    print("\n=== Example 2: GROUP BY with aggregation ===")
    cursor.execute(""" 
        SELECT department, COUNT(*) as count, AVG(age) as avg_age 
        FROM users 
        GROUP BY department
    """)
    results = cursor.fetchall()
    print("Department statistics:")
    for row in results:
        print(row)

    # Example 3: JOIN operation using two DataFrames
    # 2つのDataFrameを使用したJOIN操作の例
    print("\n=== Example 3: JOIN operation ===")
    cursor.execute(""" 
        SELECT 
            users.name as customer_name,
            orders.product as product_name,
            orders.amount as order_amount
        FROM users
        JOIN orders ON users.id = orders.user_id
        ORDER BY amount DESC
    """)
    results = cursor.fetchall()
    print("User orders:")
    for row in results:
        print(row)

    # Example 4: Using with Pandas DataFrame directly
    # Pandas DataFrameの直接使用例
    print("\n=== Example 4: Using with Pandas DataFrame ===")
    cursor.execute(""" 
        SELECT name, age 
        FROM users 
        WHERE department = 'IT' 
        ORDER BY age DESC
    """)
    results = cursor.fetchall()
    print("IT department members:")
    for row in results:
        print(row)

    # --- Lazy-loading Example ---
    # This example demonstrates the lazy-loading functionality where CSV files are loaded automatically
    # if the connection is initialized without initial DataFrames.
    print("\n=== Example 5: Lazy-loading ===")
    # Set base_dir to the directory containing the CSV files (assuming they are placed in the same directory as this example file)
    base_dir = os.path.dirname(__file__)
    print("base_dir:", base_dir)

    csv_files = [
        os.path.join(base_dir, 'users.csv'),
        os.path.join(base_dir, 'orders.csv')
    ]
    print('file1:', os.path.exists(csv_files[0]))
    print('file2:', os.path.exists(csv_files[1]))
    # Create connection without providing initial dataframes to trigger lazy-loading
    conn_lazy = pica.connect(base_dir=base_dir)
    try:
        cursor = conn_lazy.cursor()
        cursor.execute("SELECT * FROM users")
        results = cursor.fetchall()
        print("Lazy-loaded users data:")
        print(results)
    except Exception as e:
        print("Error during lazy-loading:", e)

if __name__ == "__main__":
    main()
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