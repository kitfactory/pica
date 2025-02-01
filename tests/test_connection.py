import pytest
import pandas as pd
from pica import connect
from pica.exceptions import (
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    ProgrammingError
)

def test_connection_creation():
    """Connection creation test
    接続作成のテスト
    """
    conn = connect()
    assert conn is not None
    cursor = conn.cursor()
    assert cursor is not None

def test_table_registration():
    """Table registration test with schema
    スキーマ付きテーブル登録のテスト
    """
    conn = connect()
    
    test_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    
    schema = {
        'id': 'INTEGER',
        'name': 'TEXT'
    }
    
    # Register table with schema
    conn.register_table('test_table', test_df, schema)
    assert 'test_table' in conn.tables
    
    # Test basic query
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test_table")
    result = cursor.fetchall()
    assert len(result) == 3

"""
# 高度なデータ型のテスト - 後で有効化
def test_advanced_table_registration():
    conn = connect()
    
    test_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'active': [True, False, True],
        'joined_date': ['2023-01-01', '2023-02-01', '2023-03-01']
    })
    
    schema = {
        'id': 'INTEGER',
        'name': 'TEXT',
        'age': 'INTEGER',
        'active': 'BOOLEAN',
        'joined_date': 'DATE'
    }
    
    conn.register_table('test_table', test_df, schema)
    
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test_table WHERE active = true AND age > 30")
    result = cursor.fetchall()
    assert len(result) == 1
    assert result[0][0] == 3  # id
    assert result[0][1] == 'Charlie'  # name
    assert result[0][2] == 35  # age
    assert result[0][3] == True  # active

def test_invalid_table_registration():
    conn = connect()
    
    # Test with invalid DataFrame
    with pytest.raises(ProgrammingError):
        conn.register_table('invalid_table', None, {})
    
    # Test with invalid schema
    df = pd.DataFrame({'col': [1, 2, 3]})
    with pytest.raises(DataError):
        conn.register_table('invalid_table', df, {'col': 'INVALID_TYPE'})

def test_transaction():
    conn = connect()
    
    df = pd.DataFrame({
        'id': [1, 2],
        'value': ['A', 'B']
    })
    schema = {
        'id': 'INTEGER',
        'value': 'TEXT'
    }
    conn.register_table('test_table', df, schema)
    
    cursor = conn.cursor()
    cursor.execute("INSERT INTO test_table (id, value) VALUES (3, 'C')")
    conn.commit()
    
    cursor.execute("SELECT * FROM test_table")
    assert len(cursor.fetchall()) == 3
    
    cursor.execute("INSERT INTO test_table (id, value) VALUES (4, 'D')")
    conn.rollback()
    
    cursor.execute("SELECT * FROM test_table")
    assert len(cursor.fetchall()) == 3

def test_connection_close():
    conn = connect()
    conn.close()
    with pytest.raises(InterfaceError):
        conn.cursor()
""" 