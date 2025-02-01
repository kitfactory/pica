import os
import pandas as pd
import pytest
import pica


# Fixture: サンプルデータとスキーマを定義
@pytest.fixture
def sample_data():
    # サンプルユーザデータ
    users_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 22],
        'department': ['Sales', 'IT', 'Sales', 'Marketing', 'IT']
    }
    # サンプル注文データ
    orders_data = {
        'order_id': [1, 2, 3, 4, 5],
        'user_id': [1, 2, 1, 3, 5],
        'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Printer'],
        'amount': [1000, 20, 50, 200, 150]
    }
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
    # DataFrame の作成
    users_df = pd.DataFrame(users_data)
    orders_df = pd.DataFrame(orders_data)
    
    initial_tables = {
        "users": users_df,
        "orders": orders_df
    }
    return initial_tables, users_schema, orders_schema


# Fixture: Connection の作成とスキーマ登録
@pytest.fixture
def connection(sample_data):
    initial_tables, users_schema, orders_schema = sample_data
    conn = pica.connect(dataframes=initial_tables)
    conn.register_schema("users", users_schema)
    conn.register_schema("orders", orders_schema)
    return conn


# Fixture: Cursor の取得
@pytest.fixture
def cursor(connection):
    return connection.cursor()


# テストケース1: Basic SELECT with WHERE
def test_basic_select_where(cursor):
    query = "SELECT name, age FROM users WHERE age > 25"
    cursor.execute(query)
    results = cursor.fetchall()
    # 期待値: 30歳以上のユーザ (Bob, Charlie, David)
    expected = [("Bob", 30), ("Charlie", 35), ("David", 28)]
    assert results == expected, f"Expected {expected}, got {results}"


# テストケース2: GROUP BY with aggregation
def test_group_by(cursor):
    query = """
        SELECT department, COUNT(*) as count, AVG(age) as avg_age 
        FROM users 
        GROUP BY department
    """
    cursor.execute(query)
    results = cursor.fetchall()
    # 期待値: 部門ごとの集計結果
    # Sales: count=2, avg_age=(25+35)/2 = 30.0
    # IT: count=2, avg_age=(30+22)/2 = 26.0
    # Marketing: count=1, avg_age=28
    expected = [("Sales", 2, 30.0), ("IT", 2, 26.0), ("Marketing", 1, 28.0)]
    # 結果の順序は保証されないので、ソートして比較
    results_sorted = sorted(results, key=lambda x: x[0])
    expected_sorted = sorted(expected, key=lambda x: x[0])
    for exp, res in zip(expected_sorted, results_sorted):
        assert exp[0] == res[0], f"Expected {exp[0]}, got {res[0]}"
        assert exp[1] == res[1], f"Expected {exp[1]}, got {res[1]}"
        assert round(exp[2], 2) == round(res[2], 2), f"Expected {exp[2]}, got {res[2]}"


# テストケース3: JOIN operation using two DataFrames
def test_join_operation(cursor):
    query = """
        SELECT 
            users.name as customer_name,
            orders.product as product_name,
            orders.amount as order_amount
        FROM users
        JOIN orders ON users.id = orders.user_id
        ORDER BY amount DESC
    """
    cursor.execute(query)
    results = cursor.fetchall()
    # 期待値: 注文金額の降順
    expected = [
        ("Alice", "Laptop", 1000),
        ("Charlie", "Monitor", 200),
        ("Eve", "Printer", 150),
        ("Alice", "Keyboard", 50),
        ("Bob", "Mouse", 20)
    ]
    assert results == expected, f"Expected {expected}, got {results}"


# テストケース4: Using with Pandas DataFrame directly
def test_dataframe_usage(cursor):
    query = """
        SELECT name, age 
        FROM users 
        WHERE department = 'IT' 
        ORDER BY age DESC
    """
    cursor.execute(query)
    results = cursor.fetchall()
    # 期待値: IT部門のメンバーを年齢降順に
    expected = [("Bob", 30), ("Eve", 22)]
    assert results == expected, f"Expected {expected}, got {results}"


# --- Additional test cases for CSV lazy-loading ---

def test_lazy_loading_success(tmp_path):
    """
    Test that lazy-loading from CSV files works when the file exists.
    CSVファイルが存在する場合、lazy-loadingが正しく動作することを確認するテスト
    """
    import csv
    # Create temporary directory for CSV files
    base_dir = tmp_path / "data"
    base_dir.mkdir()
    
    # Create users.csv
    users_file = base_dir / "users.csv"
    with users_file.open("w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age", "department"])
        writer.writerow([1, "Alice", 25, "Sales"])
        writer.writerow([2, "Bob", 30, "IT"])
        writer.writerow([3, "Charlie", 35, "Sales"])
        writer.writerow([4, "David", 28, "Marketing"])
        writer.writerow([5, "Eve", 22, "IT"])
    
    # Create orders.csv
    orders_file = base_dir / "orders.csv"
    with orders_file.open("w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "user_id", "product", "amount"])
        writer.writerow([1, 1, "Laptop", 1000])
        writer.writerow([2, 2, "Mouse", 20])

    # Create connection without providing initial dataframes, so that lazy-loading is used
    conn = pica.connect(base_dir=str(base_dir))
    
    # Execute a SQL query to trigger lazy-loading and retrieve users
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")
    results = cursor.fetchall()
    assert len(results) > 0, "Search result for users should not be empty"


def test_lazy_loading_file_not_found(tmp_path):
    """
    Test that FileNotFoundError is raised if the CSV file does not exist.
    CSVファイルが存在しない場合に FileNotFoundError が発生することを確認するテスト
    """
    base_dir = tmp_path / "empty"
    base_dir.mkdir()
    conn = pica.connect(base_dir=str(base_dir))
    import pytest
    with pytest.raises(FileNotFoundError):
        conn.get_table("non_existing_table")


def test_lazy_loading_invalid_csv(tmp_path):
    """
    Test that a DataError is raised when reading an invalid CSV file.
    不正なCSVファイル（例: 空ファイル）を読み込んだ場合に DataError が発生することを確認するテスト
    """
    base_dir = tmp_path / "invalid"
    base_dir.mkdir()
    invalid_file = base_dir / "invalid.csv"
    # Create an invalid (empty) CSV file
    invalid_file.write_text("", encoding="utf-8")
    conn = pica.connect(base_dir=str(base_dir))
    from pica.exceptions import DataError
    import pytest
    with pytest.raises(DataError):
        conn.get_table("invalid") 