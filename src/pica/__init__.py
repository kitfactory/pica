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

__version__ = "0.1.0"

def connect() -> Connection:
    """Create a new database connection
    新しいデータベース接続を作成

    Returns:
        Connection: Database connection object
                   データベース接続オブジェクト
    """
    return Connection()

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
