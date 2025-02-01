import pytest
import pandas as pd
from datetime import date
from pica import connect
from pica.exceptions import (
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    ProgrammingError,
    NotSupportedError
)

# テストデータの準備
@pytest.fixture
def sample_data():
    """Basic test data setup
    基本的なテストデータのセットアップ
    """
    # Users data
    users_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    return {'users': users_df}

@pytest.fixture
def cursor(sample_data):
    """Create cursor with basic test data
    基本的なテストデータを持つカーソルを作成
    """
    conn = connect()
    
    # Register users table with schema
    users_schema = {
        'id': 'INTEGER',
        'name': 'TEXT',
        'age': 'INTEGER'
    }
    conn.register_table('users', sample_data['users'], users_schema)
    
    return conn.cursor()

@pytest.fixture
def advanced_data():
    """Advanced test data with various data types
    様々なデータ型を含む高度なテストデータ
    """
    # Users data with advanced types
    users_df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 22],
        'department': ['IT', 'HR', 'IT', 'Sales', 'HR'],
        'active': [True, False, True, True, False],
        'joined_date': ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01']
    })
    
    # Orders data
    orders_df = pd.DataFrame({
        'order_id': [1, 2, 3, 4, 5],
        'user_id': [1, 2, 1, 3, 5],
        'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Printer'],
        'amount': [1000.50, 20.99, 50.50, 200.00, 150.00],
        'order_date': ['2023-06-01', '2023-06-02', '2023-06-03', '2023-06-04', '2023-06-05']
    })
    
    return {'users': users_df, 'orders': orders_df}

@pytest.fixture
def advanced_cursor(advanced_data):
    """Create cursor with advanced test data
    高度なテストデータを持つカーソルを作成
    """
    conn = connect()
    
    # Register users table with schema
    users_schema = {
        'id': 'INTEGER',
        'name': 'TEXT',
        'age': 'INTEGER',
        'department': 'TEXT',
        'active': 'BOOLEAN',
        'joined_date': 'DATE'
    }
    conn.register_table('users', advanced_data['users'], users_schema)
    
    # Register orders table with schema
    orders_schema = {
        'order_id': 'INTEGER',
        'user_id': 'INTEGER',
        'product': 'TEXT',
        'amount': 'REAL',
        'order_date': 'DATE'
    }
    conn.register_table('orders', advanced_data['orders'], orders_schema)
    
    return conn.cursor()

def test_basic_select(cursor):
    """Basic SELECT with WHERE clause test
    基本的なSELECT文とWHERE句のテスト
    """
    # Test exact comparison
    cursor.execute("SELECT name, age FROM users WHERE age = 30")
    result = cursor.fetchall()
    assert len(result) == 1
    assert result[0] == ('Bob', 30)

    # Test greater than
    cursor.execute("SELECT name, age FROM users WHERE age > 25")
    result = cursor.fetchall()
    print("\nDEBUG: Greater than result:", result)  # デバッグ出力を追加
    assert len(result) == 2
    assert ('Bob', 30) in result
    assert ('Charlie', 35) in result

    # Test less than
    cursor.execute("SELECT name, age FROM users WHERE age < 30")
    result = cursor.fetchall()
    print("DEBUG: Less than result:", result)  # デバッグ出力を追加
    assert len(result) == 1
    assert result[0] == ('Alice', 25)

def test_data_types(advanced_cursor):
    """Test different data types in SQL queries
    SQLクエリでの異なるデータ型のテスト
    """
    # Test TEXT type
    advanced_cursor.execute("SELECT name FROM users WHERE id = 1")
    result = advanced_cursor.fetchone()
    assert isinstance(result[0], str)
    assert result[0] == 'Alice'

    # Test INTEGER type
    advanced_cursor.execute("SELECT age FROM users WHERE id = 1")
    result = advanced_cursor.fetchone()
    assert isinstance(result[0], int)
    assert result[0] == 25

    # Test BOOLEAN type
    advanced_cursor.execute("SELECT active FROM users WHERE id = 1")
    result = advanced_cursor.fetchone()
    assert isinstance(result[0], bool)
    assert result[0] is True

    # Test DATE type
    advanced_cursor.execute("SELECT joined_date FROM users WHERE id = 1")
    result = advanced_cursor.fetchone()
    assert isinstance(result[0], date)
    assert result[0] == date(2023, 1, 1)

    # Test REAL type
    advanced_cursor.execute("SELECT amount FROM orders WHERE order_id = 1")
    result = advanced_cursor.fetchone()
    assert isinstance(result[0], float)
    assert result[0] == 1000.50

def test_type_based_comparisons(advanced_cursor):
    """Test comparisons with different data types
    異なるデータ型での比較テスト
    """
    # Test INTEGER comparison
    advanced_cursor.execute("SELECT name FROM users WHERE age > 30")
    result = advanced_cursor.fetchall()
    assert len(result) == 1
    assert result[0][0] == 'Charlie'

    # Test BOOLEAN comparison
    advanced_cursor.execute("SELECT name FROM users WHERE active = true")
    result = advanced_cursor.fetchall()
    assert len(result) == 3
    assert set(row[0] for row in result) == {'Alice', 'Charlie', 'David'}

    # Test DATE comparison
    advanced_cursor.execute("SELECT name FROM users WHERE joined_date < '2023-03-01'")
    result = advanced_cursor.fetchall()
    assert len(result) == 2
    assert set(row[0] for row in result) == {'Alice', 'Bob'}

    # Test REAL comparison
    advanced_cursor.execute("SELECT product FROM orders WHERE amount > 100.0")
    result = advanced_cursor.fetchall()
    assert len(result) == 3
    assert set(row[0] for row in result) == {'Laptop', 'Monitor', 'Printer'}

def test_aggregate_functions(advanced_cursor):
    """Test aggregate functions in SQL queries
    SQLクエリでの集計関数のテスト
    """
    advanced_cursor.execute('''
    SELECT 
        department,
        COUNT(*) as count,
        AVG(age) as avg_age,
        MAX(age) as max_age,
        MIN(age) as min_age,
        SUM(age) as total_age
    FROM users
    GROUP BY department
    ''')
    result = advanced_cursor.fetchall()
    assert len(result) == 3
    
    result_dict = {row[0]: row[1:] for row in result}
    
    it_stats = result_dict['IT']
    assert it_stats[0] == 2  # count
    assert it_stats[1] == 30.0  # avg_age
    assert it_stats[2] == 35  # max_age
    assert it_stats[3] == 25  # min_age
    assert it_stats[4] == 60  # total_age

def test_join_with_conditions(advanced_cursor):
    """Test JOIN operations with conditions
    条件付きJOIN操作のテスト
    """
    advanced_cursor.execute('''
    SELECT u.name, o.product, o.amount
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE u.active = true AND o.amount > 100
    ORDER BY o.amount DESC
    ''')
    result = advanced_cursor.fetchall()
    assert len(result) == 2
    assert result[0] == ('Alice', 'Laptop', 1000.50)
    assert result[1] == ('Charlie', 'Monitor', 200.00)

def test_invalid_sql(cursor):
    """Test invalid SQL statements
    無効なSQL文のテスト
    """
    with pytest.raises(ProgrammingError):
        cursor.execute("SELECT * FROM nonexistent_table")
    
    with pytest.raises(NotSupportedError):
        cursor.execute("CREATE TABLE test (id INTEGER)")

def test_fetch_methods(advanced_cursor):
    """Test different fetch methods
    異なるフェッチメソッドのテスト
    """
    advanced_cursor.execute("SELECT name FROM users ORDER BY age")
    
    assert advanced_cursor.fetchone() == ('Eve',)
    assert advanced_cursor.fetchmany(2) == [('Alice',), ('David',)]
    assert advanced_cursor.fetchall() == [('Bob',), ('Charlie',)]

def test_parameterized_query(advanced_cursor):
    """Test parameterized queries
    パラメータ化されたクエリのテスト
    """
    # Test positional parameters
    advanced_cursor.execute("SELECT name FROM users WHERE age > ? AND active = ?", [25, True])
    result = advanced_cursor.fetchall()
    assert len(result) == 2
    assert set(row[0] for row in result) == {'Charlie', 'David'}

    # Test named parameters
    advanced_cursor.execute(
        "SELECT name FROM users WHERE department = :dept AND age <= :age",
        {'dept': 'IT', 'age': 25}
    )
    result = advanced_cursor.fetchall()
    assert len(result) == 1
    assert result[0][0] == 'Alice' 