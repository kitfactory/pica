from .exceptions import (
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    ProgrammingError
)
from .connection import Connection
from .cursor import Cursor
from typing import Dict, Optional
import pandas as pd

__version__ = "0.1.0"

def connect(base_dir: str = ".", dataframes: Optional[Dict[str, pd.DataFrame]] = None) -> Connection:
    """
    Create a new database connection.
    新しいデータベース接続を作成します。

    Args:
        base_dir (str): Base directory for CSV files. Default is current directory.
                       CSVファイルの基準ディレクトリ。デフォルトは現在のディレクトリ。
        dataframes (Dict[str, pd.DataFrame], optional): Dictionary of table names and their DataFrames.
                                                      テーブル名とDataFrameの辞書。

    Returns:
        Connection: A new Connection object
                   新しい接続オブジェクト
    """
    return Connection(base_dir=base_dir, dataframes=dataframes)

__all__ = [
    'connect',
    'Connection',
    'Error',
    'InterfaceError',
    'DatabaseError',
    'DataError',
    'OperationalError',
    'ProgrammingError'
]
