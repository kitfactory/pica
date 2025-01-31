import os
import sys
import pandas as pd

# Add the parent directory to the system path to import pica
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pica

def main():
    """
    Basic example of using Pica with CSV files and DataFrames
    CSVファイルとDataFrameを使用したPicaの基本的な使用例
    """
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

    # Example 1: Basic SELECT with WHERE using DataFrame directly
    # DataFrameを直接使用した基本的なSELECTとWHEREの例
    print("\n=== Example 1: Basic SELECT with WHERE ===")
    conn = pica.connect(dataframe=users_df)
    cursor = conn.cursor()
    cursor.execute("SELECT name, age FROM dataframe WHERE age > 25")
    results = cursor.fetchall()
    print("Users over 25:")
    for row in results:
        print(row)

    # Example 2: GROUP BY with aggregation
    # GROUP BYと集計の例
    print("\n=== Example 2: GROUP BY with aggregation ===")
    cursor.execute("""
        SELECT department, COUNT(*) as count, AVG(age) as avg_age 
        FROM dataframe 
        GROUP BY department
    """)
    results = cursor.fetchall()
    print("Department statistics:")
    for row in results:
        print(row)

    # Example 3: JOIN operation using two DataFrames
    # 2つのDataFrameを使用したJOIN操作の例
    print("\n=== Example 3: JOIN operation ===")
    # Register both DataFrames with their schemas
    conn.register_table("users", users_df, users_schema)
    conn.register_table("orders", orders_df, orders_schema)
    cursor.execute("""
        SELECT 
            users.name as customer_name,
            orders.product as product_name,
            orders.amount as order_amount
        FROM users
        JOIN orders ON users.id = orders.user_id
        ORDER BY order_amount DESC
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

if __name__ == "__main__":
    main() 