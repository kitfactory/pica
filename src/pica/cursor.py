import sqlparse
import pandas as pd
import platform
from typing import Any, List, Optional, Sequence, Union, Dict, Tuple
from .exceptions import (
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    ProgrammingError,
    NotSupportedError
)
from datetime import date, datetime
import re
import numpy as np
import datetime

# Linux x86_64環境でのみfireducksをインポート
# Import fireducks only on Linux x86_64 environment
HAS_FIREDUCKS = False
if platform.system() == 'Linux' and platform.machine() == 'x86_64':
    try:
        import fireducks as fd
        HAS_FIREDUCKS = True
    except ImportError:
        HAS_FIREDUCKS = False

class Cursor:
    """Database cursor for executing SQL queries and managing results
    SQLクエリの実行と結果を管理するデータベースカーソル

    Args:
        connection (Connection): Database connection object
                               データベース接続オブジェクト
    """

    # 集計関数の定義
    AGGREGATE_FUNCTIONS = {
        'COUNT': {
            'pandas_func': 'count',
            'allow_star': True,    # COUNT(*)をサポート
            'needs_column': False  # カラム指定が任意
        },
        'SUM': {
            'pandas_func': 'sum',
            'allow_star': False,
            'needs_column': True
        },
        'AVG': {
            'pandas_func': 'mean',  # pandasではmeanを使用
            'allow_star': False,
            'needs_column': True
        },
        'MAX': {
            'pandas_func': 'max',
            'allow_star': False,
            'needs_column': True
        },
        'MIN': {
            'pandas_func': 'min',
            'allow_star': False,
            'needs_column': True
        }
    }

    def __init__(self, connection: 'Connection'):
        self.connection = connection
        self.arraysize = 1  # Default size for fetchmany
        self.last_query: Optional[str] = None
        self.result_set = None
        self._description = None
        self._rowcount = -1
        self._current_row = 0

    @property
    def description(self) -> Optional[List[tuple]]:
        """Get column information for the last query
        最後のクエリのカラム情報を取得

        Returns:
            Optional[List[tuple]]: List of 7-item sequences containing column information
                                 カラム情報を含む7項目のシーケンスのリスト
                                 (name, type_code, display_size, internal_size,
                                  precision, scale, null_ok)
        """
        if self.result_set is None:
            return None
        
        # Convert DataFrame column information to DBAPI description format
        return [(name,                    # name
                None,                     # type_code
                None,                     # display_size
                None,                     # internal_size
                None,                     # precision
                None,                     # scale
                True)                     # null_ok
                for name in self.result_set.columns]

    @property
    def rowcount(self) -> int:
        """Get the number of rows affected by the last query
        最後のクエリで影響を受けた行数を取得

        Returns:
            int: Number of affected rows or -1
                 影響を受けた行数または-1
        """
        return self._rowcount

    def _format_parameter(self, value: Any) -> str:
        """Format a parameter value according to its type
        型に応じてパラメータ値をフォーマット

        Args:
            value: Parameter value to format
                  フォーマットするパラメータ値

        Returns:
            str: Formatted parameter value
                 フォーマット済みのパラメータ値

        Raises:
            DataError: When parameter type is not supported
                      サポートされていない型の場合
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            # SQLのブール値リテラルとして扱う
            return 'TRUE' if value else 'FALSE'
        elif isinstance(value, (str, date, datetime)):
            # Escape special characters and quote the value
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, (list, tuple)):
            # Handle IN clause parameters
            return f"({', '.join(self._format_parameter(v) for v in value)})"
        else:
            raise DataError(f"Unsupported parameter type: {type(value)}")

    def _prepare_query(self, operation: str, parameters: Union[Sequence[Any], Dict[str, Any]]) -> str:
        """Prepare SQL query with parameters
        パラメータ付きSQLクエリを準備

        Args:
            operation (str): SQL query with placeholders
                           プレースホルダを含むSQLクエリ
            parameters: Query parameters (sequence or dict)
                       クエリパラメータ（シーケンスまたは辞書）

        Returns:
            str: Prepared SQL query
                 準備されたSQLクエリ

        Raises:
            ProgrammingError: When parameter count mismatch or invalid placeholder
                             パラメータ数の不一致や無効なプレースホルダの場合
        """
        if parameters is None:
            return operation

        if isinstance(parameters, dict):
            # Named parameters (e.g., :name, :value)
            pattern = r':([a-zA-Z_][a-zA-Z0-9_]*)'
            named_params = re.finditer(pattern, operation)
            result = operation
            
            for match in named_params:
                param_name = match.group(1)
                if param_name not in parameters:
                    raise ProgrammingError(f"Parameter '{param_name}' not provided")
                value = self._format_parameter(parameters[param_name])
                result = result.replace(f":{param_name}", value)
            
            return result
        else:
            # Positional parameters (?)
            parts = operation.split('?')
            if len(parts) - 1 != len(parameters):
                raise ProgrammingError(
                    f"Parameter count mismatch. Expected {len(parts) - 1}, got {len(parameters)}"
                )
            
            result = []
            for i, part in enumerate(parts[:-1]):
                result.append(part)
                result.append(self._format_parameter(parameters[i]))
            result.append(parts[-1])
            
            return ''.join(result)

    def execute(self, operation: str, parameters: Union[Sequence[Any], Dict[str, Any], None] = None) -> None:
        """Execute SQL query with parameters
        パラメータ付きSQLクエリを実行

        Args:
            operation (str): SQL query with placeholders
                           プレースホルダを含むSQLクエリ
            parameters: Query parameters (sequence or dict)
                       クエリパラメータ（シーケンスまたは辞書）

        Raises:
            DatabaseError: When execution fails
                         実行が失敗した場合
        """
        try:
            query = self._prepare_query(operation, parameters)
            self.last_query = query
            self._current_row = 0
            parsed = sqlparse.parse(query)[0]
                        
            stmt_type = parsed.get_type()

            if stmt_type == "SELECT":
                try:
                    self._select(parsed)
                except ValueError as e:
                    if "Table" in str(e) and "not found" in str(e):
                        raise ProgrammingError(f"Table does not exist: {str(e)}")
                    raise
            elif stmt_type == "INSERT":
                self._insert(parsed)
            elif stmt_type == "UPDATE":
                self._update(parsed)
            elif stmt_type == "DELETE":
                self._delete(parsed)
            else:
                raise NotSupportedError(f"Unsupported SQL statement type: {stmt_type}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            if isinstance(e, (ProgrammingError, NotSupportedError)):
                raise
            raise DatabaseError(f"Query execution failed: {str(e)}")

    def _select(self, parsed) -> None:
        """Execute SELECT statement
        SELECT文を実行
        """
        tokens = [t for t in parsed.tokens if not t.is_whitespace]
        
        # Get selected columns and their aliases
        select_clause = self._parse_select_clause(tokens)
        
        # Set description for selected columns using aliases
        self._description = [(alias, None, None, None, None, None, True) for _, alias in select_clause]
        
        # Get base table and its alias
        table_name = self._get_table_name(tokens, "FROM")
        table_alias = self._get_table_alias(tokens, "FROM")

        if table_name not in self.connection.tables:
            raise ValueError(f"Table {table_name} not found")

        df = self.connection.tables[table_name].copy()

        # テーブルエイリアスを保存
        table_aliases = {}
        if table_alias:
            table_aliases.update(table_alias)

        # Handle JOIN if present
        join_info = self._find_join_clause(tokens)
        if join_info:
            join_table, join_condition = join_info
            join_alias = self._get_table_alias(tokens, "JOIN")
            if join_alias:
                table_aliases.update(join_alias)
                        
            if join_table not in self.connection.tables:
                raise ValueError(f"Join table {join_table} not found")
            
            right_df = self.connection.tables[join_table].copy()
            left_col, right_col = self._parse_join_condition(join_condition)
            
            # エイリアスを使用している場合は、実際のカラム名に変換
            if '.' in left_col:
                alias, col = left_col.split('.')
                for table, a in table_aliases.items():
                    if a == alias:
                        left_col = col
                        break
            
            if '.' in right_col:
                alias, col = right_col.split('.')
                for table, a in table_aliases.items():
                    if a == alias:
                        right_col = col
                        break
            
            df = pd.merge(df, right_df, left_on=left_col, right_on=right_col)

        # WHERE句の処理
        where_token = None
        for token in tokens:
            if isinstance(token, sqlparse.sql.Where):
                where_token = token
                break

        if where_token:
            mask = self._evaluate_where_condition(df, where_token, table_aliases)
            df = df[mask]

        # ORDER BY句の処理を先に実行
        order_by = self._find_order_by_clause(tokens)
        if order_by:
            df = self._apply_order_by(df, order_by, table_aliases)

        # GROUP BY句の処理
        group_by = self._find_group_by_clause(tokens)
        if group_by:
            df = self._apply_group_by(df, group_by, select_clause)

        # Apply column selection and aliases
        df = self._apply_select_clause(df, select_clause, table_aliases)

        self.result_set = df
        self._rowcount = len(df)

    def _parse_select_clause(self, tokens) -> List[tuple]:
        """Parse SELECT clause to get columns and aliases
        SELECT句を解析してカラムとエイリアスを取得

        Args:
            tokens: List of SQL tokens
                   SQLトークンのリスト

        Returns:
            List[tuple]: List of (column, alias) pairs
                        (カラム, エイリアス)のペアのリスト
        """
        select_tokens = []
        in_select = False
        has_wildcard = False
        table_name = None
        
        for token in tokens:
            if token.value.upper() == 'SELECT':
                in_select = True
            elif token.value.upper() == 'FROM':
                # FROMの後のトークンがテーブル名
                break
            elif in_select and not token.is_whitespace and token.value != ',':
                if token.value == '*':
                    has_wildcard = True
                    continue
                
                if isinstance(token, sqlparse.sql.IdentifierList):
                    items = token.get_identifiers()
                else:
                    items = [token]
                
                for item in items:
                    if isinstance(item, sqlparse.sql.Function):
                        # 集計関数の処理
                        func_name = item.tokens[0].value.upper()
                        args = ''.join(str(t) for t in item.tokens[1:])
                        col_name = f"{func_name}{args}"
                        alias = item.get_alias() if item.has_alias() else col_name
                        select_tokens.append((col_name, alias))
                    elif isinstance(item, sqlparse.sql.Identifier):
                        # 集計関数のパターンをチェック
                        item_str = str(item)
                        agg_func = self._parse_aggregate_function(item_str)
                        if agg_func:
                            func_name, column, alias = agg_func
                            if column == '*':
                                col_name = f"{func_name}(*)"
                            else:
                                col_name = f"{func_name}({column})"
                            select_tokens.append((col_name, alias))
                        else:
                            # 通常のカラム
                            if item.has_alias():
                                select_tokens.append((str(item.get_real_name()), str(item.get_alias())))
                            else:
                                col_name = str(item).strip()
                                select_tokens.append((col_name, col_name))


        # ワイルドカードの処理
        if has_wildcard:
            # テーブル名を取得
            for token in tokens:
                if token.value.upper() == 'FROM':
                    for t in tokens[tokens.index(token) + 1:]:
                        if not t.is_whitespace:
                            table_name = str(t).split()[0]  # エイリアスがある場合は除去
                            break
                    break
            
            if table_name and table_name in self.connection.tables:
                df = self.connection.tables[table_name]
                # 全カラムに対してタプルを生成
                select_tokens = [(col, col) for col in df.columns]
            else:
                raise ValueError(f"Table {table_name} not found")

        return select_tokens

    def _apply_select_clause(self, df: pd.DataFrame, select_clause: List[Tuple[str, str]], table_aliases: Dict[str, str]) -> pd.DataFrame:
        """Apply SELECT clause to DataFrame
        SELECT句をDataFrameに適用
        """

        # 元のDataFrameをコピー
        original_df = df.copy()
        selected_columns = []

        for col, alias in select_clause:

            # Handle table alias in column name
            if '.' in col:
                table_alias, col_name = col.split('.')
                if table_alias not in table_aliases.values():
                    raise ValueError(f"Invalid table alias: {table_alias}")
                col = col_name

            # 集計関数の処理
            agg_match = re.match(r'^(COUNT|SUM|AVG|MAX|MIN)\((.*?)\)', col)
            if agg_match:
                func_name = agg_match.group(1).upper()
                arg = agg_match.group(2)

                # 集計関数の結果カラム名を決定
                if func_name == 'COUNT' and arg == '*':
                    result_col = 'count'
                else:
                    result_col = f"{func_name}({arg})"

                # 結果カラムが存在するか確認
                if result_col in original_df.columns:
                    selected_columns.append(original_df[result_col].rename(alias))
                    continue
                elif alias in original_df.columns:
                    selected_columns.append(original_df[alias].rename(alias))
                    continue
                else:
                    raise ValueError(f"Aggregate result column not found: {result_col}")

            # 通常のカラム処理
            if col not in original_df.columns:
                raise ValueError(f"Column {col} not found")

            selected_columns.append(original_df[col].rename(alias))

        result = pd.concat(selected_columns, axis=1)
        
        return result

    def _is_aggregate_function(self, column: str) -> bool:
        """Check if column is an aggregate function
        カラムが集計関数かどうかをチェック

        Args:
            column (str): Column name or function
                     カラム名または関数

        Returns:
            bool: True if column is an aggregate function
                  カラムが集計関数の場合はTrue
        """
        agg_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
        return any(column.upper().startswith(func) for func in agg_functions)

    def _parse_aggregate_function(self, column: str) -> Optional[Tuple[str, str, Optional[str]]]:
        """Parse aggregate function from column string
        カラム文字列から集計関数を解析

        Args:
            column (str): Column string (e.g., "COUNT(*) as count")
                     カラム文字列（例："COUNT(*) as count"）

        Returns:
            Optional[Tuple[str, str, Optional[str]]]: (function name, column name, alias) if matched
                                                 マッチした場合は(関数名, カラム名, エイリアス)
        """
        # COUNT(*), SUM(column), AVG(column) as alias などのパターンにマッチ
        pattern = r'^(COUNT|SUM|AVG|MIN|MAX)\((.*?)\)(?:\s+as\s+(\w+))?$'
        match = re.match(pattern, column, re.IGNORECASE)
        
        if match:
            func_name = match.group(1).upper()
            col_name = match.group(2)
            alias = match.group(3)
            return func_name, col_name, alias
        
        return None

    def _insert(self, parsed) -> None:
        """Process INSERT statement
        INSERT文の処理

        Args:
            parsed: Parsed SQL statement
                   パース済みのSQL文
        """
        table_name = str(parsed.tokens[2])
        values = str(parsed.tokens[-1]).replace("VALUES", "").strip(" ()").split(",")
        df = self.connection.tables[table_name]
        new_row = pd.Series(values, index=df.columns)
        self.connection.tables[table_name] = df.append(new_row, ignore_index=True)

    def _update(self, parsed) -> None:
        """Process UPDATE statement
        UPDATE文の処理

        Args:
            parsed: Parsed SQL statement
                   パース済みのSQL文

        Raises:
            ValueError: When table is not found or invalid syntax
                       テーブルが見つからないか、構文が無効な場合
            DatabaseError: When update operation fails
                         更新操作が失敗した場合
        """
        tokens = [t for t in parsed.tokens if not t.is_whitespace]
        
        # Get table name
        table_name = str(tokens[1])
        if table_name not in self.connection.tables:
            raise ValueError(f"Table {table_name} not found")
        
        df = self.connection.tables[table_name]
        
        # Parse SET clause
        set_clause = self._parse_set_clause(tokens)
        if not set_clause:
            raise ValueError("No SET clause found in UPDATE statement")
            
        # Parse SET assignments
        assignments = self._parse_assignments(set_clause)
            
        # WHERE句の処理
        where_clause = self._find_where_clause(tokens)
        if where_clause:
            mask = self._evaluate_where_condition(df, where_clause)
            self._rowcount = mask.sum()
            
            # Update matching rows
            for column, value in assignments.items():
                df.loc[mask, column] = value
        else:
            # Update all rows
            self._rowcount = len(df)
            for column, value in assignments.items():
                df[column] = value
                
        self.connection.tables[table_name] = df

    def _delete(self, parsed) -> None:
        """Process DELETE statement
        DELETE文の処理

        Args:
            parsed: Parsed SQL statement
                   パース済みのSQL文

        Raises:
            ValueError: When table is not found or invalid syntax
                       テーブルが見つからないか、構文が無効な場合
            DatabaseError: When delete operation fails
                         削除操作が失敗した場合
        """
        tokens = [t for t in parsed.tokens if not t.is_whitespace]
        
        # Find table name after FROM
        table_name = self._get_table_name(tokens, "FROM")
        if not table_name:
            raise ValueError("No table name found in DELETE statement")
        if table_name not in self.connection.tables:
            raise ValueError(f"Table {table_name} not found")
            
        df = self.connection.tables[table_name]
        
        # WHERE句の処理
        where_clause = self._find_where_clause(tokens)
        if where_clause:
            mask = self._evaluate_where_condition(df, where_clause)
            self._rowcount = mask.sum()
            df = df[~mask]
        else:
            self._rowcount = len(df)
            df = df.iloc[0:0]  # Empty the DataFrame but keep structure
            
        self.connection.tables[table_name] = df

    def _get_table_name(self, tokens: List[Any], keyword: str) -> str:
        """Get table name from tokens after specified keyword
        指定されたキーワードの後のトークンからテーブル名を取得

        Args:
            tokens (List[Any]): List of SQL tokens
                               SQLトークンのリスト
            keyword (str): Keyword to search for (e.g., "FROM", "INTO")
                          検索するキーワード（例："FROM"、"INTO"）

        Returns:
            str: Table name without alias
                 エイリアスを除いたテーブル名

        Raises:
            ValueError: If table name is not found
                       テーブル名が見つからない場合
        """
        for i, token in enumerate(tokens):
            if token.value.upper() == keyword:
                if i + 1 >= len(tokens):
                    raise ValueError(f"No table name found after {keyword}")
                
                table_token = tokens[i + 1]
                
                # テーブル名を取得（エイリアスがある場合は除去）
                if isinstance(table_token, sqlparse.sql.Identifier):
                    table_parts = str(table_token).split()
                    return table_parts[0]  # エイリアスがあっても最初の部分がテーブル名
                
                return str(table_token)
        
        raise ValueError(f"Keyword {keyword} not found")

    def _get_table_alias(self, tokens: List[Any], keyword: str) -> Optional[Dict[str, str]]:
        """Get table alias mapping from tokens after specified keyword
        指定されたキーワードの後のトークンからテーブルのエイリアスマッピングを取得

        Args:
            tokens (List[Any]): List of SQL tokens
                               SQLトークンのリスト
            keyword (str): Keyword to search for (e.g., "FROM", "JOIN")
                          検索するキーワード（例："FROM"、"JOIN"）

        Returns:
            Optional[Dict[str, str]]: Dictionary mapping table names to their aliases, or None if no aliases
                                     テーブル名とそのエイリアスのマッピング辞書、エイリアスがない場合はNone
        """
        for i, token in enumerate(tokens):
            if token.value.upper() == keyword:
                if i + 1 >= len(tokens):
                    return None
                
                table_token = tokens[i + 1]
                
                if isinstance(table_token, sqlparse.sql.Identifier):
                    table_parts = str(table_token).split()
                    if len(table_parts) > 1:
                        return {table_parts[0]: table_parts[1]}
        
        return None

    def _find_where_clause(self, tokens) -> Optional[sqlparse.sql.Comparison]:
        """Find and parse WHERE clause
        WHERE句を検索して解析

        Args:
            tokens: List of SQL tokens
                   SQLトークンのリスト

        Returns:
            Optional[sqlparse.sql.Comparison]: WHERE condition if found, None otherwise
                                             WHERE条件が見つかった場合はその条件、見つからない場合はNone
        """
        for i, token in enumerate(tokens):
            if token.value.upper() == 'WHERE':
                if i + 1 >= len(tokens):
                    raise ValueError("Invalid WHERE clause")
                
                # 次のトークンが比較式
                next_token = tokens[i + 1]
                
                if isinstance(next_token, sqlparse.sql.Where):
                    # WHERE句の中から比較式を探す
                    for t in next_token.tokens:
                        if isinstance(t, sqlparse.sql.Comparison):
                            return t
                    
                    # 比較式が見つからない場合
                    raise ValueError("No comparison found in WHERE clause")
                elif isinstance(next_token, sqlparse.sql.Comparison):
                    return next_token
                
                raise ValueError("Invalid WHERE clause format")
        return None

    def _parse_set_clause(self, tokens: List[Any]) -> Optional[str]:
        """Parse SET clause from tokens
        トークンからSET句を解析

        Args:
            tokens: List of SQL tokens
                   SQLトークンのリスト

        Returns:
            Optional[str]: SET clause if found, None otherwise
                          SET句（見つからない場合はNone）
        """
        for i, token in enumerate(tokens):
            if isinstance(token, sqlparse.sql.Token) and token.value.upper() == 'SET':
                if i + 1 < len(tokens):
                    return str(tokens[i + 1])
        return None

    def _parse_assignments(self, set_clause: str) -> Dict[str, str]:
        """Parse assignment expressions from SET clause
        SET句から代入式を解析

        Args:
            set_clause (str): SET clause to parse
                             解析するSET句

        Returns:
            Dict[str, str]: Dictionary of column-value assignments
                           カラムと値の代入辞書
        """
        assignments = {}
        for assignment in set_clause.split(','):
            column, value = assignment.split('=')
            column = column.strip()
            value = value.strip()
            # Remove quotes if present
            if value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            assignments[column] = value
        return assignments

    def _parse_in_condition(self, where_clause: str) -> tuple[str, List[str]]:
        """Parse IN condition from WHERE clause
        WHERE句からIN条件を解析

        Args:
            where_clause (str): WHERE clause containing IN condition
                               IN条件を含むWHERE句

        Returns:
            tuple[str, List[str]]: Column name and list of values
                                  カラム名と値のリスト
        """
        parts = where_clause.upper().split(' IN ')
        column = parts[0].replace('WHERE', '').strip()
        values_str = parts[1].strip('() ').split(',')
        values = [v.strip().strip("'") for v in values_str]
        return column, values

    def _convert_value(self, value: Any) -> Any:
        """Convert pandas/numpy value to Python native type
        pandas/numpy の値を Python のネイティブ型に変換

        Args:
            value: Value to convert
                  変換する値

        Returns:
            Any: Converted value
                 変換された値
        """
        if pd.isna(value):
            return None
        elif isinstance(value, (pd.Timestamp, np.datetime64)):
            return value.to_pydatetime().date()
        elif isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        return value

    def fetchone(self) -> Optional[Tuple]:
        """Fetch next row
        次の行を取得

        Returns:
            Optional[Tuple]: Next row or None if no more rows
                           次の行（もう行がない場合はNone）
        """
        if self.result_set is None:
            raise ProgrammingError("No result set available")
        
        if self._current_row >= len(self.result_set):
            return None
        
        visible_columns = [col[0] for col in self._description] if self._description else []
        row = self.result_set.iloc[self._current_row][visible_columns]
        self._current_row += 1
        
        converted_row = [self._convert_value(value) for value in row]
        return tuple(converted_row)

    def fetchmany(self, size: Optional[int] = None) -> List[tuple]:
        """Fetch the next size rows
        次のsize行を取得

        Args:
            size: Number of rows to fetch
                 取得する行数

        Returns:
            List[tuple]: List of rows
                        行のリスト
        """
        if self.result_set is None:
            raise ProgrammingError("No result set available")

        if size is None:
            size = self.arraysize

        end_row = min(self._current_row + size, len(self.result_set))
        rows = []
        
        visible_columns = [col[0] for col in self._description] if self._description else []
        
        for i in range(self._current_row, end_row):
            row = self.result_set.iloc[i][visible_columns]
            converted_row = [self._convert_value(value) for value in row]
            rows.append(tuple(converted_row))
        
        self._current_row = end_row
        return rows

    def fetchall(self) -> List[Tuple]:
        if self.result_set is None:
            return []

        visible_columns = [col[0] for col in self._description] if self._description else []
        result = []
        
        # 現在のカーソル位置以降の行のみを処理
        for idx in range(self._current_row, len(self.result_set)):
            row = self.result_set.iloc[idx]
            converted_row = [self._convert_value(value) for value in row[visible_columns]]
            result.append(tuple(converted_row))

        # カーソル位置を最後に更新
        self._current_row = len(self.result_set)
        return result

    def close(self) -> None:
        """Close the cursor and clean up resources
        カーソルを閉じてリソースをクリーンアップ
        """
        self.result_set = None

    def executemany(self, operation: str, seq_of_parameters: Sequence[Union[Sequence[Any], Dict[str, Any]]]) -> None:
        """Execute the same SQL query with different parameters
        同じSQLクエリを異なるパラメータで実行

        Args:
            operation (str): SQL query with placeholders
                           プレースホルダを含むSQLクエリ
            seq_of_parameters: Sequence of parameter sequences or dicts
                             パラメータシーケンスまたは辞書のシーケンス

        Raises:
            DatabaseError: When execution fails
                         実行が失敗した場合
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def _evaluate_where_condition(self, df: pd.DataFrame, condition, table_aliases: Dict[str, str]) -> pd.Series:
        """Evaluate WHERE condition
        WHERE条件を評価

        Args:
            df (pd.DataFrame): Target DataFrame
            condition: SQL WHERE condition
            table_aliases (Dict[str, str]): Dictionary mapping table names to their aliases

        Returns:
            pd.Series: Boolean mask for filtering
        """
        if isinstance(condition, sqlparse.sql.Statement):
            # Statementの場合は最初のトークンを使用
            condition = condition.tokens[0]

        if isinstance(condition, sqlparse.sql.Comparison):
            result = self._evaluate_comparison(df, condition, table_aliases)
            return result
        elif isinstance(condition, sqlparse.sql.Where):
            # WHERE句の場合、トークンを解析して条件を評価
            tokens = [t for t in condition.tokens if not t.is_whitespace and str(t).upper() != 'WHERE']
            
            result = None
            current_operator = None
            i = 0
            
            while i < len(tokens):
                token = tokens[i]
                token_str = str(token).upper()
                
                if token_str in ('AND', 'OR'):
                    current_operator = token_str
                    i += 1
                elif isinstance(token, sqlparse.sql.Comparison):
                    # 比較条件の場合は直接評価
                    condition_result = self._evaluate_comparison(df, token, table_aliases)
                    
                    if result is None:
                        result = condition_result
                    elif current_operator == 'AND':
                        result = result & condition_result
                    elif current_operator == 'OR':
                        result = result | condition_result
                    i += 1
                else:
                    # トークンを組み合わせて条件を構築
                    if i + 2 < len(tokens):
                        condition_str = f"{token} {tokens[i+1]} {tokens[i+2]}"
                        
                        try:
                            parsed_condition = sqlparse.parse(condition_str)[0]
                            condition_result = self._evaluate_where_condition(df, parsed_condition, table_aliases)
                            
                            if result is None:
                                result = condition_result
                            elif current_operator == 'AND':
                                result = result & condition_result
                            elif current_operator == 'OR':
                                result = result | condition_result
                            i += 3
                        except Exception as e:
                            i += 1
                    else:
                        i += 1
            
            if result is None:
                result = pd.Series(True, index=df.index)
            
            return result
        elif isinstance(condition, sqlparse.sql.Identifier):
            # エイリアス付きのカラム参照を処理
            column_name = str(condition)
            if '.' in column_name:
                alias, col = column_name.split('.')
                # エイリアスが有効かチェック
                if not any(a == alias for t, a in table_aliases.items()):
                    raise ValueError(f"Invalid table alias: {alias}")
                column_name = col

            if column_name not in df.columns:
                raise ValueError(f"Column {column_name} not found")
            return df[column_name]
        else:
            raise ValueError(f"Unsupported condition type: {type(condition)}")

    def _evaluate_comparison(self, df: pd.DataFrame, condition: sqlparse.sql.Comparison, table_aliases: Dict[str, str]) -> pd.Series:
        """Evaluate comparison condition
        比較条件を評価

        Args:
            df (pd.DataFrame): Target DataFrame
                             対象のDataFrame
            condition (sqlparse.sql.Comparison): Comparison condition
                                               比較条件
            table_aliases (Dict[str, str]): Table aliases
                                          テーブルエイリアス

        Returns:
            pd.Series: Boolean mask of matching rows
                      条件に一致する行のブールマスク
        """
        
        # Get comparison parts
        parts = [str(t).strip() for t in condition.tokens if not t.is_whitespace]
        
        if len(parts) != 3:
            raise ValueError(f"Invalid comparison format: {condition}")
        
        left_operand, operator, right_operand = parts
        
        # Handle column name with table alias
        column_name = left_operand
        if '.' in left_operand:
            alias, col = left_operand.split('.')
            if not any(a == alias for t, a in table_aliases.items()):
                raise ValueError(f"Invalid table alias: {alias}")
            column_name = col
        
        # Get column values
        if column_name not in df.columns:
            raise ValueError(f"Column {column_name} not found")
        column = df[column_name]

        # Convert right operand based on column type
        if pd.api.types.is_bool_dtype(column.dtype):
            # 文字列'TRUE'/'FALSE'またはPythonのブール値を処理
            if isinstance(right_operand, bool):
                right_value = right_operand
            else:
                right_value = right_operand.upper() == 'TRUE'
        elif pd.api.types.is_numeric_dtype(column.dtype):
            right_value = float(right_operand)
        elif pd.api.types.is_datetime64_any_dtype(column.dtype) or any(isinstance(x, datetime.date) for x in column):
            try:
                right_value = pd.to_datetime(right_operand).date()
                # 列の値を日付型に変換
                column = pd.to_datetime(column).dt.date
            except ValueError:
                raise ValueError(f"Invalid date format: {right_operand}")
        else:
            # 文字列の場合は引用符を除去
            right_value = right_operand.strip("'")

        # Apply comparison operator
        if operator == '=':
            result = column == right_value
        elif operator == '>':
            result = column > right_value
        elif operator == '<':
            result = column < right_value
        elif operator == '>=':
            result = column >= right_value
        elif operator == '<=':
            result = column <= right_value
        elif operator == '!=':
            result = column != right_value
        else:
            raise ValueError(f"Unsupported operator: {operator}")


        return result

    def _sql_like_to_regex(self, pattern: str) -> str:
        """Convert SQL LIKE pattern to regex
        SQL LIKEパターンを正規表現に変換

        Args:
            pattern (str): SQL LIKE pattern
                          SQL LIKEパターン

        Returns:
            str: Equivalent regex pattern
                 等価な正規表現パターン
        """
        # エスケープ処理
        pattern = re.escape(pattern)
        # SQL LIKE のワイルドカードを正規表現に変換
        pattern = pattern.replace(r'\%', '.*').replace(r'\_', '.')
        return f'^{pattern}$'

    def _parse_like_condition(self, where_clause: str) -> tuple[str, str]:
        """Parse LIKE condition from WHERE clause
        WHERE句からLIKE条件を解析

        Returns:
            tuple[str, str]: Column name and pattern
                            カラム名とパターン
        """
        parts = where_clause.split('LIKE')
        column = parts[0].replace('WHERE', '').strip()
        pattern = parts[1].strip().strip("'")
        return column, pattern

    def _find_order_by_clause(self, tokens: List[Any]) -> Optional[List[tuple[str, bool]]]:
        """Find and parse ORDER BY clause
        ORDER BY句を検索して解析

        Args:
            tokens: List of SQL tokens
                   SQLトークンのリスト

        Returns:
            Optional[List[tuple[str, bool]]]: List of (column, ascending) pairs if found
                                            (カラム名, 昇順フラグ)のリストが見つかった場合
        """
        for i, token in enumerate(tokens):
            if isinstance(token, sqlparse.sql.Token) and token.value.upper() == 'ORDER BY':
                if i + 1 >= len(tokens):
                    raise ValueError("Invalid ORDER BY clause")

                order_items = []
                next_token = tokens[i + 1]

                if isinstance(next_token, sqlparse.sql.IdentifierList):
                    items = next_token.get_identifiers()
                else:
                    items = [next_token]


                for item in items:
                    parts = str(item).split()
                    column = parts[0]
                    ascending = True if len(parts) == 1 or parts[1].upper() != 'DESC' else False
                    order_items.append((column, ascending))

                return order_items

        return None

    def _apply_order_by(self, df: pd.DataFrame, order_by: List[tuple[str, bool]], table_aliases: Dict[str, str]) -> pd.DataFrame:
        """Apply ORDER BY clause to DataFrame
        ORDER BY句をDataFrameに適用
        """
        if not order_by:
            return df

        columns = []
        ascending = []

        for item in order_by:
            column = item[0]
            is_ascending = item[1]

            # エイリアス付きのカラム参照を処理
            if '.' in column:
                alias, col = column.split('.')

                # エイリアスが有効かチェック
                if not any(a == alias for t, a in table_aliases.items()):
                    raise ValueError(f"Invalid table alias: {alias}")

                # エイリアス付きのカラム名を実際のカラム名に変換
                column = col

            if column not in df.columns:
                raise ValueError(f"Column {column} not found")

            columns.append(column)
            ascending.append(is_ascending)
        
        result = df.sort_values(by=columns, ascending=ascending)
        
        return result

    def _find_join_clause(self, tokens: List[Any]) -> Optional[Tuple[str, sqlparse.sql.Comparison]]:
        """Find and parse JOIN clause
        JOIN句を検索して解析

        Args:
            tokens (List[Any]): List of SQL tokens
                                   SQLトークンのリスト

        Returns:
            Optional[Tuple[str, sqlparse.sql.Comparison]]: (join table name, join condition) if found
                                                                  (結合テーブル名, 結合条件)が見つかった場合
        """
        for i, token in enumerate(tokens):
            if token.value.upper() == 'JOIN':
                if i + 3 >= len(tokens):  # Need at least JOIN table ON condition
                    raise ValueError("Invalid JOIN clause")
                
                # Get join table name (remove alias if present)
                join_table_token = tokens[i + 1]
                if isinstance(join_table_token, sqlparse.sql.Identifier):
                    join_table = str(join_table_token).split()[0]  # Get first part before alias
                else:
                    join_table = str(join_table_token)
                
                # Check for ON keyword
                on_token = tokens[i + 2]
                if not isinstance(on_token, sqlparse.sql.Token) or on_token.value.upper() != 'ON':
                    raise ValueError("JOIN must be followed by ON")
                
                # Get join condition
                condition_token = tokens[i + 3]
                if not isinstance(condition_token, sqlparse.sql.Comparison):
                    raise ValueError("Invalid JOIN condition")
                
                return join_table, condition_token
        
        return None

    def _parse_join_condition(self, condition: sqlparse.sql.Comparison) -> Tuple[str, str]:
        """Parse JOIN condition
        JOIN条件を解析

        Args:
            condition (sqlparse.sql.Comparison): JOIN condition
                                               JOIN条件

        Returns:
            Tuple[str, str]: (left column, right column)
                            (左カラム, 右カラム)
        """
        parts = [str(t).strip() for t in condition.tokens if not t.is_whitespace]
        if len(parts) != 3 or parts[1] != '=':
            raise ValueError("Invalid JOIN condition format")
        
        left_col = parts[0]
        right_col = parts[2]
        
        # エイリアス付きのカラム参照を処理
        if '.' in left_col:
            _, left_col = left_col.split('.')
        if '.' in right_col:
            _, right_col = right_col.split('.')
        
        return left_col, right_col

    def _find_group_by_clause(self, tokens) -> Optional[List[str]]:
        """Find and parse GROUP BY clause
        GROUP BY句を検索して解析

        Returns:
            Optional[List[str]]: List of grouping columns or None
        """
        for i, token in enumerate(tokens):
            if token.value.upper() == 'GROUP BY':
                group_cols = []
                j = i + 1
                while j < len(tokens) and tokens[j].value.upper() not in ('HAVING', 'ORDER BY', 'LIMIT'):
                    if not tokens[j].is_whitespace and tokens[j].value != ',':
                        col = str(tokens[j]).strip()
                        group_cols.append(col)
                    j += 1
                return group_cols
        return None

    def _apply_group_by(self, df: pd.DataFrame, group_by_columns: List[str], select_clause: List[Tuple[str, str]]) -> pd.DataFrame:
        """Apply GROUP BY clause to DataFrame
        DataFrameにGROUP BY句を適用

        Args:
            df (pd.DataFrame): Input DataFrame
                             入力DataFrame
            group_by_columns (List[str]): Columns to group by
                                        グループ化するカラム
            select_clause (List[Tuple[str, str]]): List of (column, alias) tuples
                                                  (カラム, エイリアス)のタプルのリスト

        Returns:
            pd.DataFrame: Grouped DataFrame
                         グループ化されたDataFrame
        """

        # Create aggregation dictionary
        agg_dict = {}
        for col, alias in select_clause:
            if col in group_by_columns:
                continue

            agg_match = re.match(r'(\w+)\((.*?)\)', col)
            if agg_match:
                func_name = agg_match.group(1).upper()
                col_name = agg_match.group(2)

                if func_name == 'COUNT' and col_name == '*':
                    agg_dict['id'] = [('count', 'count')]
                elif col_name in df.columns:
                    if col_name not in agg_dict:
                        agg_dict[col_name] = []
                    if func_name == 'AVG':
                        agg_dict[col_name].append(('mean', alias))
                    elif func_name == 'MAX':
                        agg_dict[col_name].append(('max', alias))
                    elif func_name == 'MIN':
                        agg_dict[col_name].append(('min', alias))
                    elif func_name == 'SUM':
                        agg_dict[col_name].append(('sum', alias))


        # Apply aggregation
        grouped = df.groupby(group_by_columns)
        result_parts = []

        # Add group by columns
        result_parts.append(grouped.first().reset_index()[group_by_columns])

        # Process each aggregation
        for col, aggs in agg_dict.items():
            for agg_func, alias in aggs:
                result = grouped[col].agg(agg_func).reset_index(drop=True).rename(alias)
                result_parts.append(result)

        # Combine results
        result = pd.concat(result_parts, axis=1)
        

        return result

    def _join(self, df: pd.DataFrame, join_clause: str, table_aliases: Dict[str, str]) -> pd.DataFrame:
        """Apply JOIN clause to DataFrame
        JOIN句をDataFrameに適用
        """

        # Parse join clause
        join_parts = join_clause.split()
        join_table = join_parts[0]
        if join_table not in self.connection.tables:
            raise ValueError(f"Table {join_table} not found")


        # Get join condition
        on_index = join_parts.index("ON")
        condition = " ".join(join_parts[on_index + 1:])

        # Get right table DataFrame
        right_df = self.connection.tables[join_table].copy()

        # Process join condition
        left_col, right_col = self._parse_join_condition(condition, table_aliases)

        # Perform join
        result = pd.merge(df, right_df, left_on=left_col, right_on=right_col)

        return result
