import pytest
import pandas as pd
from pica import connect
from pica.exceptions import DatabaseError

# テストデータの準備
@pytest.fixture
def sample_data():
    # Users data
    users_df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 22],
        'department': ['Sales', 'IT', 'Sales', 'Marketing', 'IT']
    })
    
    # Orders data
    orders_df = pd.DataFrame({
        'order_id': [1, 2, 3, 4, 5],
        'user_id': [1, 2, 1, 3, 5],
        'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Printer'],
        'amount': [1000, 20, 50, 200, 150]
    })
    
    return {'users': users_df, 'orders': orders_df}

@pytest.fixture
def cursor(sample_data):
    conn = connect(dataframe=sample_data['users'])
    conn.register_table('orders', sample_data['orders'])
    return conn.cursor()

def test_basic_select(cursor):
    """Basic SELECT with WHERE clause test
    基本的なSELECT文とWHERE句のテスト
    """
    cursor.execute("SELECT name, age FROM users WHERE age > 25")
    result = cursor.fetchall()
    assert len(result) == 3
    assert ('Bob', 30) in result
    assert ('Charlie', 35) in result
    assert ('David', 28) in result

def test_group_by(cursor):
    """GROUP BY with aggregation test
    GROUP BY句と集計関数のテスト
    """
    cursor.execute("""
        SELECT department, COUNT(*) as count, AVG(age) as avg_age
        FROM users
        GROUP BY department
    """)
    result = cursor.fetchall()
    assert len(result) == 3
    
    # Convert to dict for easier comparison
    result_dict = {dept: (count, avg_age) for dept, count, avg_age in result}
    assert result_dict['IT'] == (2, 26.0)
    assert result_dict['Sales'] == (2, 30.0)
    assert result_dict['Marketing'] == (1, 28.0)

def test_join(cursor):
    """JOIN operation test
    JOIN操作のテスト
    """
    cursor.execute("""
        SELECT users.name, orders.product, orders.amount
        FROM users
        JOIN orders ON users.id = orders.user_id
        ORDER BY orders.amount DESC
    """)
    result = cursor.fetchall()
    assert len(result) == 5
    assert result[0] == ('Alice', 'Laptop', 1000)
    assert result[-1] == ('Bob', 'Mouse', 20)

def test_invalid_sql(cursor):
    """Invalid SQL test
    不正なSQL文のテスト
    """
    with pytest.raises(DatabaseError):
        cursor.execute("SELECT * FROM nonexistent_table")

def test_where_conditions(cursor):
    """Various WHERE conditions test
    様々なWHERE条件のテスト
    """
    # Equal condition
    cursor.execute("SELECT name FROM users WHERE department = 'IT'")
    result = cursor.fetchall()
    assert len(result) == 2
    assert ('Bob',) in result
    assert ('Eve',) in result

    # Greater than condition
    cursor.execute("SELECT name FROM users WHERE age >= 30")
    result = cursor.fetchall()
    assert len(result) == 2
    assert ('Bob',) in result
    assert ('Charlie',) in result

def test_order_by(cursor):
    """ORDER BY test
    ORDER BY句のテスト
    """
    cursor.execute("SELECT name, age FROM users ORDER BY age DESC")
    result = cursor.fetchall()
    assert len(result) == 5
    assert result[0] == ('Charlie', 35)
    assert result[-1] == ('Eve', 22) 