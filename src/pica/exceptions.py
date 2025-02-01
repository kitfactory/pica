class Error(Exception):
    """Base class for exceptions in this module
    このモジュールの例外の基本クラス
    """
    pass

class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface rather than the database itself
    データベース自体ではなく、データベースインターフェースに関連するエラーの例外
    """
    pass

class DatabaseError(Error):
    """Exception raised for errors that are related to the database
    データベースに関連するエラーの例外
    """
    pass

class DataError(DatabaseError):
    """Exception raised for errors that are related to the processed data
    処理されたデータに関連するエラーの例外
    """
    pass

class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's operation
    データベースの操作に関連するエラーの例外
    """
    pass

class ProgrammingError(DatabaseError):
    """Exception raised for programming errors
    プログラミングエラーの例外
    """
    pass

class IntegrityError(DatabaseError):
    """
    Exception raised for errors in database integrity.
    データベースの整合性エラーの際に発生する例外
    """
    pass

class InternalError(DatabaseError):
    """
    Exception raised for internal database errors.
    内部データベースエラーの際に発生する例外
    """
    pass

class NotSupportedError(DatabaseError):
    """
    Exception raised for operations that are not supported.
    サポートされていない操作の際に発生する例外
    """
    pass 