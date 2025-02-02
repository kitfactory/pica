import os
import pytest
from cursor import Cursor


class DummyConnection:
    """A dummy connection object to simulate base_dir and table storage for CSV files.
    CSVファイルとテーブルオブジェクトをシミュレートするためのダミー接続オブジェクト."""
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.tables = {}


@pytest.fixture
def temp_cursor(tmp_path):
    """Fixture to create a Cursor instance with a dummy connection.
    ダミー接続を設定した Cursor インスタンスを返すフィクスチャ."""
    dummy_conn = DummyConnection(str(tmp_path))
    cur = Cursor()
    cur.connection = dummy_conn
    return cur


def test_drop_table_success(temp_cursor):
    """Test that a DROP TABLE statement deletes the CSV file and removes the table object from connection.
    DROP TABLE文で、CSVファイルおよび接続内のテーブルオブジェクトが削除されることをテストする."""
    # Create the table using CREATE TABLE
    sql_create = "CREATE TABLE test_table (id INT, name TEXT)"
    temp_cursor.execute(sql_create)
    
    # Add a dummy table object to simulate an in-memory table
    temp_cursor.connection.tables["test_table"] = "dummy_object"

    table_file = os.path.join(temp_cursor.connection.base_dir, "test_table.csv")
    assert os.path.exists(table_file), "CSV file should exist after table creation."
    assert "test_table" in temp_cursor.connection.tables, "Table object should exist in connection."

    # Execute DROP TABLE
    sql_drop = "DROP TABLE test_table"
    temp_cursor.execute(sql_drop)

    # Verify that the CSV file is deleted
    assert not os.path.exists(table_file), "CSV file should be deleted after DROP TABLE."
    # Verify that the table object is removed
    assert "test_table" not in temp_cursor.connection.tables, "Table object should be removed from connection."


def test_drop_table_error(temp_cursor):
    """Test that attempting to drop a non-existent table raises an error.
    存在しないテーブルに対してDROP TABLE文を実行するとエラーになることをテストする."""
    sql_drop = "DROP TABLE non_existent_table"
    with pytest.raises(ValueError, match="Table 'non_existent_table' does not exist."):
        temp_cursor.execute(sql_drop) 