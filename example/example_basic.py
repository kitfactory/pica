import os
import sys
import pandas as pd

# Remove the following two lines:
# 以下の2行を削除してください。
# Add the parent directory to the system path to import pica
# picaをインポートするためにシステムパスに親ディレクトリを追加する必要はありません。
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    # Lazy-loading の例 ---
    # This example demonstrates the lazy-loading functionality where CSV files are loaded automatically if the connection is initialized without initial DataFrames.
    # この例は、初期 DataFrame なしで接続された場合に、CSVファイルが自動的に読み込まれる lazy-loading 機能を示します.
    print("\n=== Example 5: Lazy-loading ===")
    base_dir = os.path.dirname(__file__)
    print("base_dir:", base_dir)

    csv_files = [
        os.path.join(base_dir, 'users.csv'),
        os.path.join(base_dir, 'orders.csv')
    ]
    print('file1:', os.path.exists(csv_files[0]))
    print('file2:', os.path.exists(csv_files[1]))
    conn_lazy = pica.connect(base_dir=base_dir)
    try:
        cursor = conn_lazy.cursor()
        cursor.execute("SELECT * FROM users")
        results = cursor.fetchall()
        print("Lazy-loaded users data:")
        print(results)
    except Exception as e:
        print("Error during lazy-loading:", e)

    # --- CREATE TABLE and DROP TABLE Example ---
    # CREATE TABLE と DROP TABLE の例
    print("\n=== Example 6: CREATE TABLE and DROP TABLE ===")
    try:
        # Create a new table 'sample' with two columns
        # 2つのカラムを持つ 'sample' テーブルを作成する
        cursor.execute("CREATE TABLE sample (col1 INT, col2 TEXT)")
        print("Created table 'sample'", "\n", "'sample' テーブルを作成しました")

        # Optionally, you might want to perform some operation on the 'sample' table here
        # 必要に応じて、'sample' テーブルに対して操作を実行できます
        
        # Now drop the table
        # その後、テーブルを削除する
        cursor.execute("DROP TABLE sample")
        print("Dropped table 'sample'", "\n", "'sample' テーブルを削除しました")
    except Exception as e:
        print("Error in CREATE/DROP TABLE example:", e)
        print("CREATE/DROP TABLE の例でのエラー:", e)

if __name__ == "__main__":
    main() 