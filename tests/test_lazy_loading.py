import os
import pandas as pd
import pytest
import sqlparse

from pica.connection import Connection
from pica.cursor import Cursor
from pica.exceptions import DatabaseError

# Dummy parsed object to simulate parsed_info
class DummyParsed:
    pass


@pytest.fixture
def temp_connection(tmp_path):
    # Create a Connection with empty tables and base_dir set to tmp_path
    # Adjust construction if necessary to use pica.connect(); here we assume Connection can be constructed directly.
    conn = Connection(dataframes={}, base_dir=str(tmp_path))
    return conn


def test_insert_lazy_loading_success(tmp_path, temp_connection):
    # Create a temporary CSV file for table 'test_table'
    csv_content = "id,name\n1,Alice\n2,Bob\n"
    csv_file = tmp_path / "test_table.csv"
    csv_file.write_text(csv_content)

    # Create a dummy parsed object with parsed_info containing table_name
    dummy = DummyParsed()
    dummy.parsed_info = {"table_name": "test_table"}

    # Create a cursor and call _insert, which should trigger lazy-loading
    cursor = Cursor(temp_connection)
    cursor._insert(dummy)

    # Verify that 'test_table' is loaded in the connection
    assert "test_table" in temp_connection.tables
    df_loaded = temp_connection.tables["test_table"]
    df_expected = pd.read_csv(str(csv_file))
    pd.testing.assert_frame_equal(df_loaded, df_expected)


def test_insert_lazy_loading_file_not_found(temp_connection):
    # Create a dummy parsed object with a non-existent table name
    dummy = DummyParsed()
    dummy.parsed_info = {"table_name": "non_existent_table"}

    cursor = Cursor(temp_connection)
    # Expecting an exception since the file does not exist
    with pytest.raises(Exception):
        cursor._insert(dummy)


def test_update_lazy_loading_success(monkeypatch):
    # Monkeypatch lazy-loading to simulate success
    monkeypatch.setattr("pica.lazy_loader.load_table_if_needed", dummy_load_table_if_needed_success)

    # Create dummy connection without the table loaded
    conn = DummyConnection()
    cursor = Cursor(conn)

    # Prepare an UPDATE query that updates col2 for row where col1 == 1
    query_str = "UPDATE test_table SET col2 = 'updated' WHERE col1 = 1"
    parsed = sqlparse.parse(query_str)[0]
    # Inject parsed_info with table_name so that lazy-loading is triggered
    parsed.parsed_info = {"table_name": "test_table"}

    # Execute update: lazy-loading should load table and then update it
    cursor._update(parsed)

    # Verify: table is now lazy-loaded and the row with col1 == 1 has col2 updated
    updated_df = conn.tables.get("test_table")
    assert updated_df is not None, "Lazy-loaded table should exist"
    row = updated_df.loc[updated_df["col1"] == 1]
    assert not row.empty, "Row with col1 == 1 should exist"
    assert row.iloc[0]["col2"] == "updated", "col2 should be updated to 'updated'"


def test_update_lazy_loading_missing_table(monkeypatch):
    # Monkeypatch lazy-loading to simulate failure (table not loaded)
    monkeypatch.setattr("pica.lazy_loader.load_table_if_needed", dummy_load_table_if_needed_failure)

    # Create dummy connection without the target table
    conn = DummyConnection()
    cursor = Cursor(conn)

    query_str = "UPDATE non_existent_table SET col2 = 'updated'"
    parsed = sqlparse.parse(query_str)[0]
    parsed.parsed_info = {"table_name": "non_existent_table"}

    # Expect failure when table is not found after fallback
    with pytest.raises(ValueError, match="Table non_existent_table not found"):
        cursor._update(parsed)


def dummy_load_table_if_needed_success(connection, table_name):
    # Simulate lazy-loading by creating a dummy DataFrame if table not present
    if table_name not in connection.tables:
        df = pd.DataFrame({"col1": [1, 2], "col2": ["old", "old"]})
        connection.tables[table_name] = df.copy()


def dummy_load_table_if_needed_failure(connection, table_name):
    # Simulate lazy-loading failure by doing nothing
    pass


class DummyConnection:
    def __init__(self):
        self.tables = {}


def test_delete_lazy_loading_success(tmp_path):
    """Test DELETE operation lazy-loading successfully loads table from CSV and deletes all rows."""
    # Define a dummy connection class with lazy-loading capability
    class DummyConnection:
        def __init__(self, base_dir):
            self.base_dir = base_dir
            self.tables = {}
        def load_table_if_needed(self, table_name):
            csv_file = os.path.join(self.base_dir, f"{table_name}.csv")
            if os.path.exists(csv_file):
                self.tables[table_name] = pd.read_csv(csv_file)
            else:
                raise FileNotFoundError(f"CSV file {csv_file} not found")

    # Create a temporary CSV file with sample data
    csv_content = "id,name\n1,Alice\n2,Bob\n3,Charlie"
    csv_file = tmp_path / "test_table.csv"
    csv_file.write_text(csv_content)

    conn = DummyConnection(str(tmp_path))
    cursor = Cursor(conn)

    # Create dummy tokens for DELETE statement: DELETE FROM test_table
    class DummyToken:
        def __init__(self, value):
            self.value = value
            self.is_whitespace = False
        def __str__(self):
            return self.value

    tokens = [DummyToken("DELETE"), DummyToken("FROM"), DummyToken("test_table")]

    class DummyParsed:
        def __init__(self, tokens):
            self.tokens = tokens
    parsed = DummyParsed(tokens)

    # Call _delete() to delete all rows from test_table
    cursor._delete(parsed)

    # After deletion, the table should be empty
    assert conn.tables["test_table"].empty
    # _rowcount should equal the number of rows originally loaded (3)
    assert cursor._rowcount == 3


def test_delete_lazy_loading_file_not_found(tmp_path):
    """Test DELETE operation raises error when CSV file for lazy-loading is missing."""
    # Define a dummy connection class with lazy-loading capability
    class DummyConnection:
        def __init__(self, base_dir):
            self.base_dir = base_dir
            self.tables = {}
        def load_table_if_needed(self, table_name):
            csv_file = os.path.join(self.base_dir, f"{table_name}.csv")
            if os.path.exists(csv_file):
                import pandas as pd
                self.tables[table_name] = pd.read_csv(csv_file)
            else:
                raise FileNotFoundError(f"CSV file {csv_file} not found")

    conn = DummyConnection(str(tmp_path))
    cursor = Cursor(conn)

    # Create dummy tokens for DELETE statement: DELETE FROM nonexistent
    class DummyToken:
        def __init__(self, value):
            self.value = value
            self.is_whitespace = False
        def __str__(self):
            return self.value

    tokens = [DummyToken("DELETE"), DummyToken("FROM"), DummyToken("nonexistent")]

    class DummyParsed:
        def __init__(self, tokens):
            self.tokens = tokens
    parsed = DummyParsed(tokens)

    with pytest.raises(ValueError) as excinfo:
        cursor._delete(parsed)
    assert "Failed to load table nonexistent" in str(excinfo.value)


def test_join_lazy_loading_success(tmp_path, temp_connection):
    """Test JOIN operation lazy-loading: successfully load join table from CSV and perform join."""
    import pandas as pd

    # Create CSV files for main_table and join_table
    main_csv = tmp_path / "main_table.csv"
    join_csv = tmp_path / "join_table.csv"
    main_csv.write_text("id,value\n1,A\n2,B")
    join_csv.write_text("id,extra\n1,X\n2,Y")

    # Set the base_dir for lazy-loading
    temp_connection.base_dir = str(tmp_path)

    # Execute a JOIN query
    query = "SELECT main_table.id, main_table.value, join_table.extra FROM main_table JOIN join_table ON main_table.id = join_table.id"
    cursor = Cursor(temp_connection)
    cursor.execute(query)

    # Expected merge using pandas
    df_main = pd.read_csv(str(main_csv))
    df_join = pd.read_csv(str(join_csv))
    expected_df = pd.merge(df_main, df_join, on="id")

    pd.testing.assert_frame_equal(cursor.result_set.reset_index(drop=True), expected_df.reset_index(drop=True))


def test_join_lazy_loading_file_not_found(tmp_path, temp_connection):
    """Test JOIN operation lazy-loading raises error when join table CSV is missing."""
    # Create CSV file only for main_table, no join_table CSV
    main_csv = tmp_path / "main_table.csv"
    main_csv.write_text("id,value\n1,A\n2,B")

    # Set the base_dir for lazy-loading
    temp_connection.base_dir = str(tmp_path)

    # Execute a JOIN query where join_table CSV does not exist
    query = "SELECT main_table.id, main_table.value, join_table.extra FROM main_table JOIN join_table ON main_table.id = join_table.id"
    cursor = Cursor(temp_connection)

    with pytest.raises(DatabaseError) as excinfo:
        cursor.execute(query)
    assert "Join table join_table CSV file" in str(excinfo.value) 