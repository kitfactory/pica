import os
import pandas as pd
import pytest

from cursor import Cursor

class DummyConnection:
    """A dummy connection object to simulate base_dir for CSV files. / CSVファイル用の base_dir をシミュレートするダミー接続オブジェクト."""
    def __init__(self, base_dir):
        self.base_dir = base_dir

@pytest.fixture
def temp_cursor(tmp_path):
    """Fixture to create a Cursor instance with a dummy connection.
    ダミー接続を設定した Cursor インスタンスを返すフィクスチャ."""
    dummy_conn = DummyConnection(str(tmp_path))
    cur = Cursor()
    cur.connection = dummy_conn
    return cur


def test_create_table_success(temp_cursor):
    """Test that a CREATE TABLE statement creates a new CSV file with specified headers.
    CREATE TABLE文で、指定されたカラムを持つ新規CSVファイルが作成されることをテストする."""
    sql = "CREATE TABLE test_table (id INT, name TEXT)"
    temp_cursor.execute(sql)
    table_file = os.path.join(temp_cursor.connection.base_dir, "test_table.csv")
    assert os.path.exists(table_file), "CSV file should be created. / CSVファイルが作成されているべきです。"

    df = pd.read_csv(table_file)
    expected_columns = ["id", "name"]
    assert list(df.columns) == expected_columns, f"Expected columns {expected_columns}, got {list(df.columns)}"
    assert df.empty, "DataFrame should be empty. / DataFrameは空であるべきです。"


def test_create_table_if_not_exists_success(temp_cursor):
    """Test that executing a CREATE TABLE with IF NOT EXISTS does nothing if the table already exists.
    IF NOT EXISTSが指定されている場合、既存テーブルに対してCREATE TABLE文を実行しても何も起こらないことをテストする."""
    sql = "CREATE TABLE test_table (id INT, name TEXT)"
    temp_cursor.execute(sql)
    table_file = os.path.join(temp_cursor.connection.base_dir, "test_table.csv")
    assert os.path.exists(table_file)

    # Execute with IF NOT EXISTS; no exception should be raised
    sql_if = "CREATE TABLE IF NOT EXISTS test_table (id INT, name TEXT)"
    temp_cursor.execute(sql_if)

    # Confirm that the file still exists and has the original headers
    df = pd.read_csv(table_file)
    expected_columns = ["id", "name"]
    assert list(df.columns) == expected_columns


def test_create_table_already_exists_error(temp_cursor):
    """Test that executing a CREATE TABLE without IF NOT EXISTS on an existing table raises an error.
    IF NOT EXISTSなしで既存のテーブルに対してCREATE TABLE文を実行するとエラーとなることをテストする."""
    sql = "CREATE TABLE test_table (id INT, name TEXT)"
    temp_cursor.execute(sql)
    with pytest.raises(ValueError, match="Table 'test_table' already exists."):
        temp_cursor.execute(sql) 