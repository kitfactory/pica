import os
import pandas as pd
import datetime
import platform
from typing import Optional, Dict, Any, Union
from .cursor import Cursor
from .exceptions import (
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    ProgrammingError
)

# Linux x86_64環境でのみfireducksをインポート
HAS_FIREDUCKS = False
if platform.system() == 'Linux' and platform.machine() == 'x86_64':
    try:
        import fireducks as fd
        HAS_FIREDUCKS = True
    except ImportError:
        HAS_FIREDUCKS = False

class Warning(Exception):
    """Exception for important warnings
    重要な警告に関する例外クラス
    """
    pass

class InterfaceError(Error):
    """Exception for errors related to database interface
    データベースインターフェースに関連するエラー
    """
    pass

class DatabaseError(Error):
    """Exception for errors related to database operations
    データベース操作に関連するエラー
    """
    pass

class DataError(DatabaseError):
    """Exception for errors related to data processing
    データ処理に関連するエラー
    """
    pass

class OperationalError(DatabaseError):
    """Exception for errors related to database operations
    データベース操作に関連するエラー
    """
    pass

class IntegrityError(DatabaseError):
    """Exception for errors related to relational integrity
    リレーショナルの整合性に関連するエラー
    """
    pass

class InternalError(DatabaseError):
    """Exception for internal database errors
    データベース内部のエラー
    """
    pass

class ProgrammingError(DatabaseError):
    """Exception for programming errors
    プログラミングエラー
    """
    pass

class NotSupportedError(DatabaseError):
    """Exception for unsupported operations
    サポートされていない操作
    """
    pass

class Connection:
    """Database connection supporting Pandas + CSV + fireducks automatic conversion
    Pandas + CSV + fireducks の自動変換をサポートするデータベース接続

    Args:
        csv_file (str, optional): Path to CSV file
                                CSVファイルへのパス
        dataframe (pd.DataFrame, optional): Pandas DataFrame
                                          PandasのDataFrame
        use_fireducks (bool): Whether to use fireducks for high-speed operations
                            高速操作のためにfireducksを使用するかどうか

    Raises:
        InterfaceError: When neither csv_file nor dataframe is provided
                       CSVファイルもDataFrameも提供されていない場合
        OperationalError: When file operation fails
                        ファイル操作に失敗した場合
    """

    def __init__(self, csv_file: Optional[str] = None, dataframe: Optional[pd.DataFrame] = None, use_fireducks: bool = False):
        """Initialize database connection
        データベース接続を初期化

        Args:
            csv_file (str, optional): Path to CSV file
                                    CSVファイルへのパス
            dataframe (pd.DataFrame, optional): Pandas DataFrame
                                              PandasのDataFrame
            use_fireducks (bool): Whether to use fireducks for high-speed operations
                                高速操作のためにfireducksを使用するかどうか

        Raises:
            InterfaceError: When neither csv_file nor dataframe is provided
                           CSVファイルもDataFrameも提供されていない場合
            OperationalError: When file operation fails
                            ファイル操作に失敗した場合
        """
        self.tables: Dict[str, Union[pd.DataFrame, 'fd.DataFrame']] = {}
        self.schema: Dict[str, Dict[str, str]] = {}
        self.use_fireducks = use_fireducks and HAS_FIREDUCKS
        self.basedir = os.path.dirname(csv_file) if csv_file else os.getcwd()

        if csv_file:
            try:
                df = pd.read_csv(csv_file, dtype=str)
                table_name = os.path.splitext(os.path.basename(csv_file))[0]
                self.tables[table_name] = fd.from_pandas(df) if self.use_fireducks else df
            except OSError as e:
                raise OperationalError(f"Failed to open file: {str(e)}")
            except pd.errors.EmptyDataError:
                raise DataError(f"Empty CSV file: {csv_file}")
            except pd.errors.ParserError:
                raise DataError(f"Failed to parse CSV file: {csv_file}")
        elif dataframe is not None:
            if not isinstance(dataframe, pd.DataFrame):
                raise InterfaceError("dataframe must be a pandas DataFrame")
            self.tables['dataframe'] = fd.from_pandas(dataframe) if self.use_fireducks else dataframe
        else:
            raise InterfaceError("Either csv_file or dataframe must be provided")

    def create_table(self, name: str, schema: dict) -> None:
        """Create a new table
        テーブルを作成

        Args:
            name (str): Table name
                       テーブル名
            schema (dict): Column names and their types
                          カラム名と型の定義

        Raises:
            ProgrammingError: When table name is invalid
                             テーブル名が無効な場合
            DataError: When schema definition is invalid
                      スキーマ定義が無効な場合
            DatabaseError: For other database operation errors
                          その他のデータベース操作エラー
        """
        if not name or not isinstance(name, str):
            raise ProgrammingError("Invalid table name")
        if not schema or not isinstance(schema, dict):
            raise DataError("Invalid schema definition")
        if name in self.tables:
            raise IntegrityError(f"Table {name} already exists")

        try:
            df = pd.DataFrame(columns=schema.keys())
            df = self._convert_dataframe_types(df, schema)
            self.tables[name] = fd.from_pandas(df) if self.use_fireducks else df
            self.schema[name] = schema
        except ValueError as e:
            raise DataError(f"Failed to convert data types: {str(e)}")
        except Exception as e:
            raise DatabaseError(f"Failed to create table: {str(e)}")

    def register_table(self, name: str, dataframe: Any, schema: dict) -> None:
        """Register existing DataFrame as a table
        既存のDataFrameをテーブルとして登録

        Args:
            name (str): Table name
                       テーブル名
            dataframe: pandas DataFrame
                      pandas DataFrame
            schema (dict): Column names and their types
                          カラム名と型の定義

        Raises:
            ProgrammingError: When table name or schema is invalid
                             テーブル名やスキーマが無効な場合
            DataError: When DataFrame type conversion fails
                      DataFrameの型変換に失敗した場合
            IntegrityError: When table already exists
                           テーブルが既に存在する場合
        """
        if not name or not isinstance(name, str):
            raise ProgrammingError("Invalid table name")
        if name in self.tables:
            raise IntegrityError(f"Table {name} already exists")
        if not isinstance(dataframe, pd.DataFrame):
            raise ProgrammingError("dataframe must be a pandas DataFrame")

        try:
            dataframe = self._convert_dataframe_types(dataframe, schema)
            self.tables[name] = dataframe
            self.schema[name] = schema
        except ValueError as e:
            raise DataError(f"Failed to convert data types: {str(e)}")
        except Exception as e:
            raise DatabaseError(f"Failed to register table: {str(e)}")

    def _convert_dataframe_types(self, df: pd.DataFrame, schema: dict) -> pd.DataFrame:
        """Convert DataFrame types based on schema
        データフレームの型をスキーマに基づいて変換

        Args:
            df (pd.DataFrame): Target DataFrame
                             変換対象のDataFrame
            schema (dict): Type definitions
                          型定義

        Returns:
            pd.DataFrame: DataFrame with converted types
                         型変換後のDataFrame

        Raises:
            DataError: When type conversion fails
                      型変換に失敗した場合
            ValueError: When type definition is invalid
                       無効な型定義の場合
        """
        valid_types = {"INTEGER", "REAL", "BOOLEAN", "DATE", "TEXT"}
        try:
            for col, dtype in schema.items():
                if dtype not in valid_types:
                    raise ValueError(f"Invalid data type: {dtype}")
                
                if dtype == "INTEGER":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype == "REAL":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif dtype == "BOOLEAN":
                    df[col] = df[col].map(lambda x: x.lower() == "true" if isinstance(x, str) else bool(x))
                elif dtype == "DATE":
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            return df
        except ValueError as e:
            raise DataError(f"Failed to convert data types: {str(e)}")
        except Exception as e:
            raise DatabaseError(f"Unexpected error occurred: {str(e)}")

    def _get_csv_path(self, table_name: str) -> str:
        """Get CSV file path for the table
        テーブルに対応する CSV ファイルのパスを取得
        """
        return os.path.join(self.basedir, f"{table_name}.csv")

    def commit(self) -> None:
        """Save changes to CSV files
        変更をCSVファイルに保存

        Raises:
            OperationalError: When file operation fails
                             ファイル操作に失敗した場合
            DatabaseError: For other database operation errors
                          その他のデータベース操作エラー
        """
        try:
            for table_name, df in self.tables.items():
                file_path = self._get_csv_path(table_name)
                if self.use_fireducks:
                    df = df.to_pandas()
                df.to_csv(file_path, index=False)
        except OSError as e:
            raise OperationalError(f"Failed to save file: {str(e)}")
        except Exception as e:
            raise DatabaseError(f"Failed to commit: {str(e)}")

    def rollback(self) -> None:
        """Restore to last saved CSV state
        最後に保存したCSVの状態に戻す

        Raises:
            OperationalError: When file operation fails
                             ファイル操作に失敗した場合
            DataError: When data loading fails
                      データの読み込みに失敗した場合
            DatabaseError: For other database operation errors
                          その他のデータベース操作エラー
        """
        try:
            for table_name in list(self.tables.keys()):
                file_path = self._get_csv_path(table_name)
                if not os.path.exists(file_path):
                    raise OperationalError(f"File does not exist: {file_path}")
                
                try:
                    df = pd.read_csv(file_path, dtype=str)
                    df = self._convert_dataframe_types(df, self.schema.get(table_name, {}))
                    self.tables[table_name] = fd.from_pandas(df) if self.use_fireducks else df
                except pd.errors.EmptyDataError:
                    raise DataError(f"Empty CSV file: {file_path}")
                except pd.errors.ParserError:
                    raise DataError(f"Failed to parse CSV file: {file_path}")
        except (OperationalError, DataError):
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to rollback: {str(e)}")
    def cursor(self) -> Cursor:
        """Get cursor object
        カーソルオブジェクトを取得

        Returns:
            Cursor: New cursor object
                   新しいカーソルオブジェクト

        Raises:
            InterfaceError: When cursor creation fails
                           カーソルの作成に失敗した場合
        """
        try:
            return Cursor(self)
        except Exception as e:
            raise InterfaceError(f"Failed to create cursor: {str(e)}")

    def close(self) -> None:
        """Close the connection
        接続を閉じる
        """
        self.tables.clear()

