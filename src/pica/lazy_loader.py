'''lazy_loader.py
This module provides a function to perform lazy loading of tables from CSV files if not already loaded.
CSVファイルからのlazy-loadingを実施する機能を提供します。

Note: This module uses print statements for logging, as logging (Logger) is not used per the guidelines.
'''

import os
import pandas as pd


def load_table_if_needed(connection, table_name):
    """
    Check if the specified table is loaded in memory; if not, load it from a CSV file.
    指定されたテーブルがメモリ上にロードされているかを確認し、未ロードの場合はCSVファイルからロードする。

    Parameters:
        connection: An object representing the connection which must have a 'tables' dict and 'base_dir' attribute.
                    接続オブジェクト。'tables' 辞書と 'base_dir' 属性が必要です。
        table_name: str
                    チェックまたはロードするテーブル名。

    Returns:
        pd.DataFrame: The DataFrame corresponding to the loaded table.
                      ロードされたテーブルに対応するDataFrameを返します。

    Raises:
        Exception: If the table could not be loaded from the CSV file.
                   CSVファイルからテーブルをロードできなかった場合に例外を発生させます。
    """
    print(f"DEBUG: load_table_if_needed called with table_name = {table_name}")

    if table_name in connection.tables:
        print(f"DEBUG: Table '{table_name}' is already loaded.")
        return connection.tables[table_name]

    csv_path = os.path.join(connection.base_dir, table_name + '.csv')
    try:
        print(f"DEBUG: Attempting to load table '{table_name}' from file: {csv_path}")
        df = pd.read_csv(csv_path)
        connection.tables[table_name] = df
        print(f"DEBUG: Successfully loaded table '{table_name}' from {csv_path}")
        return df
    except Exception as e:
        print(f"DEBUG: Failed to load table '{table_name}' from {csv_path} - Error: {e}")
        raise e 