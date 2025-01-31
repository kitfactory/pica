import pytest
import pandas as pd
from pica import connect
from pica.exceptions import DatabaseError

def test_connection_creation():
    """Connection creation test
    接続作成のテスト
    """
    # 空のDataFrameを使用
    df = pd.DataFrame()
    conn = connect(dataframe=df)
    assert conn is not None
    cursor = conn.cursor()
    assert cursor is not None

def test_table_registration():
    """Table registration test
    テーブル登録のテスト
    """
    df = pd.DataFrame()
    conn = connect(dataframe=df)
    
    test_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    
    # Register table
    conn.register_table('test_table', test_df)
    assert 'test_table' in conn.tables
    
    # Test query on registered table
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM test_table")
    result = cursor.fetchall()
    assert len(result) == 3

def test_invalid_table_registration():
    """Invalid table registration test
    不正なテーブル登録のテスト
    """
    df = pd.DataFrame()
    conn = connect(dataframe=df)
    with pytest.raises(ValueError):
        conn.register_table('invalid_table', None)

def test_connection_close():
    """Connection close test
    接続クローズのテスト
    """
    df = pd.DataFrame()
    conn = connect(dataframe=df)
    conn.close()
    with pytest.raises(DatabaseError):
        conn.cursor() 