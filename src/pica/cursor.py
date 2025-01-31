import sqlparse
import pandas as pd
import platform
from typing import Any, List, Optional, Sequence, Union, Dict
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
            return str(value).upper()
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
            
            # デバッグ出力を追加
            print("\nDEBUG: Original SQL:", query)
            print("DEBUG: Parsed SQL structure:")
            for token in parsed.tokens:
                print(f"  Token: {token}, Type: {type(token)}, ttype: {token.ttype}")
                if hasattr(token, 'tokens'):
                    print("  Subtokens:")
                    for subtoken in token.tokens:
                        print(f"    Subtoken: {subtoken}, Type: {type(subtoken)}")
            
            stmt_type = parsed.get_type()

            if stmt_type == "SELECT":
                self._select(parsed)
            elif stmt_type == "INSERT":
                self._insert(parsed)
            elif stmt_type == "UPDATE":
                self._update(parsed)
            elif stmt_type == "DELETE":
                self._delete(parsed)
            else:
                raise ValueError(f"Unsupported SQL query: {query}")
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {str(e)}")

    def _select(self, parsed) -> None:
        """Execute SELECT statement
        SELECT文を実行
        """
        tokens = [t for t in parsed.tokens if not t.is_whitespace]
        print("\nDEBUG: Tokens:", [str(t) for t in tokens])  # デバッグ出力
        
        # Get selected columns and their aliases
        select_clause = self._parse_select_clause(tokens)
        print("DEBUG: Select clause:", select_clause)  # デバッグ出力
        
        # Get base table
        table_name = self._get_table_name(tokens, "FROM")
        if table_name not in self.connection.tables:
            raise ValueError(f"Table {table_name} not found")

        df = self.connection.tables[table_name]
        print("DEBUG: Initial DataFrame:\n", df)  # デバッグ出力

        # Handle JOIN if present
        join_info = self._find_join_clause(tokens)
        if join_info:
            join_table, join_condition = join_info
            if join_table not in self.connection.tables:
                raise ValueError(f"Join table {join_table} not found")
            
            right_df = self.connection.tables[join_table]
            left_col, right_col = self._parse_join_condition(join_condition)
            df = pd.merge(df, right_df, left_on=left_col, right_on=right_col)
            print("DEBUG: After JOIN:\n", df)  # デバッグ出力

        # WHERE句の処理
        where_token = self._find_where_clause(tokens)
        if where_token:
            print("DEBUG: Found WHERE token:", where_token)  # デバッグ出力
            print("DEBUG: WHERE token type:", type(where_token))  # デバッグ出力
            if isinstance(where_token, sqlparse.sql.Comparison):
                mask = self._evaluate_where_condition(df, where_token)
                df = df[mask]
                print("DEBUG: After WHERE:\n", df)  # デバッグ出力
            else:
                raise ValueError("Invalid WHERE clause structure")

        # GROUP BY句の処理
        group_by = self._find_group_by_clause(tokens)
        if group_by:
            df = self._apply_group_by(df, group_by, select_clause)
            print("DEBUG: After GROUP BY:\n", df)  # デバッグ出力

        # Apply column selection and aliases
        df = self._apply_select_clause(df, select_clause)
        print("DEBUG: After column selection:\n", df)  # デバッグ出力

        # ORDER BY句の処理
        order_by = self._find_order_by_clause(tokens)
        if order_by:
            df = self._apply_order_by(df, order_by)
            print("DEBUG: After ORDER BY:\n", df)  # デバッグ出力

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
        
        for token in tokens:
            if token.value.upper() == 'SELECT':
                in_select = True
            elif token.value.upper() in ('FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT'):
                break
            elif in_select and not token.is_whitespace and token.value != ',':
                if isinstance(token, sqlparse.sql.IdentifierList):
                    items = token.get_identifiers()
                else:
                    items = [token]
                
                for item in items:
                    print("DEBUG: Processing item:", item)  # デバッグ出力
                    print("DEBUG: Item type:", type(item))  # デバッグ出力
                    print("DEBUG: Item value:", item.value)  # デバッグ出力
                    print("DEBUG: Item tokens:", [str(t) for t in item.tokens] if hasattr(item, 'tokens') else 'No tokens')  # デバッグ出力
                    
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

        print("DEBUG: Final select tokens:", select_tokens)  # デバッグ出力
        return select_tokens

    def _apply_select_clause(self, df: pd.DataFrame, columns: List[tuple]) -> pd.DataFrame:
        """Apply column selection and aliases to DataFrame
        DataFrameにカラム選択とエイリアスを適用

        Args:
            df (pd.DataFrame): Input DataFrame
            columns (List[tuple]): List of (column, alias) pairs

        Returns:
            pd.DataFrame: DataFrame with selected columns and aliases
        """
        print("DEBUG: DataFrame columns:", df.columns)  # デバッグ出力
        print("DEBUG: Attempting to select columns:", columns)  # デバッグ出力

        # 結果のカラムを保持するリスト
        result_columns = []
        rename_dict = {}
        
        # 各カラムを処理
        for col, alias in columns:
            # GROUP BY結果のカラムを処理
            if alias in df.columns:
                result_columns.append(alias)
                continue
            
            # 通常のカラムを処理
            if '.' in col:
                _, col = col.split('.')
            
            if col in df.columns:
                result_columns.append(col)
                if alias != col:  # エイリアスが異なる場合は名前変更
                    rename_dict[col] = alias
            else:
                raise ValueError(f"Column not found: {col}")
        
        try:
            result = df[result_columns]
            if rename_dict:  # エイリアスの適用
                result = result.rename(columns=rename_dict)
            return result
        except KeyError as e:
            raise ValueError(f"Column not found: {str(e)}")

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

    def _get_table_name(self, tokens: List[Any], keyword: str) -> Optional[str]:
        """Get table name after specified keyword
        指定されたキーワードの後のテーブル名を取得

        Args:
            tokens: List of SQL tokens
                   SQLトークンのリスト
            keyword: Keyword to search for (e.g., "FROM")
                    検索するキーワード（例："FROM"）

        Returns:
            Optional[str]: Table name if found, None otherwise
                          テーブル名（見つからない場合はNone）
        """
        for i, token in enumerate(tokens):
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == keyword:
                if i + 1 < len(tokens):
                    return str(tokens[i + 1])
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

    def fetchone(self) -> Optional[tuple]:
        """Fetch the next row from the query result
        クエリ結果から次の1行を取得

        Returns:
            Optional[tuple]: Next row or None if no more rows
                           次の行またはこれ以上行がない場合はNone
        """
        if self.result_set is None or self._current_row >= len(self.result_set):
            return None
        row = self.result_set.iloc[self._current_row]
        self._current_row += 1
        return tuple(row)

    def fetchmany(self, size: Optional[int] = None) -> List[tuple]:
        """Fetch the next set of rows from the query result
        クエリ結果から次の行セットを取得

        Args:
            size (Optional[int]): Number of rows to fetch
                                取得する行数

        Returns:
            List[tuple]: List of rows
                        行のリスト
        """
        if size is None:
            size = 1
        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def fetchall(self) -> List[tuple]:
        """Fetch all remaining rows from the query result
        クエリ結果の残りすべての行を取得

        Returns:
            List[tuple]: All remaining rows
                        残りすべての行
        """
        if self.result_set is None:
            return []
        remaining = self.result_set.iloc[self._current_row:].values.tolist()
        self._current_row = len(self.result_set)
        return [tuple(row) for row in remaining]

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

    def _evaluate_where_condition(self, df: pd.DataFrame, condition) -> pd.Series:
        """Evaluate WHERE condition
        WHERE条件を評価

        Args:
            df (pd.DataFrame): Target DataFrame
            condition: SQL WHERE condition

        Returns:
            pd.Series: Boolean mask for filtering
        """
        print("DEBUG: Evaluating WHERE condition:", condition)  # 追加
        print("DEBUG: Condition type:", type(condition))  # 追加
        print("DEBUG: Condition tokens:", [str(t) for t in condition.tokens])  # 追加

        if isinstance(condition, sqlparse.sql.Comparison):
            result = self._evaluate_comparison(df, condition)
            print("DEBUG: Comparison result type:", type(result))  # 追加
            print("DEBUG: Comparison result:", result)  # 追加
            return result
        else:
            # Handle other types of conditions (AND, OR, etc.)
            raise NotImplementedError(f"Condition type not supported: {type(condition)}")

    def _evaluate_comparison(self, df: pd.DataFrame, comparison) -> pd.Series:
        """Evaluate comparison condition
        比較条件を評価

        Args:
            df (pd.DataFrame): Target DataFrame
            comparison: SQL comparison condition

        Returns:
            pd.Series: Boolean mask for filtering
        """
        print("DEBUG: Evaluating comparison:", comparison)  # 追加
        print("DEBUG: Comparison tokens:", [str(t) for t in comparison.tokens])  # 追加
        
        # Extract left and right operands and operator
        left = str(comparison.left).strip()
        operator = str(comparison.token_next(0)[1]).strip()
        right = str(comparison.right).strip()
        
        print("DEBUG: Left operand:", left)  # 追加
        print("DEBUG: Operator:", operator)  # 追加
        print("DEBUG: Right operand:", right)  # 追加
        print("DEBUG: Column type:", df[left].dtype)  # 追加

        result = None
        if operator == '=':
            result = df[left] == right.strip("'\"")
        elif operator == '>':
            result = df[left] > float(right)
        elif operator == '<':
            result = df[left] < float(right)
        elif operator == '>=':
            result = df[left] >= float(right)
        elif operator == '<=':
            result = df[left] <= float(right)
        elif operator == '!=':
            result = df[left] != right.strip("'\"")
        else:
            raise ValueError(f"Unsupported operator: {operator}")

        print("DEBUG: Comparison evaluation result type:", type(result))  # 追加
        print("DEBUG: Comparison evaluation result:", result)  # 追加
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
            Optional[List[tuple[str, bool]]]: List of (column, ascending) pairs or None
                                            (カラム名, 昇順フラグ)のリストまたはNone
        """
        for i, token in enumerate(tokens):
            if (isinstance(token, sqlparse.sql.Token) and 
                token.ttype is sqlparse.tokens.Keyword and 
                token.value.upper() == 'ORDER BY'):
                if i + 1 >= len(tokens):
                    raise ValueError("Invalid ORDER BY clause")
                
                order_tokens = []
                j = i + 1
                while j < len(tokens) and tokens[j].value.upper() not in ('LIMIT',):
                    if not tokens[j].is_whitespace:
                        order_tokens.append(tokens[j])
                    j += 1
                
                result = []
                current_col = []
                
                for token in order_tokens:
                    if token.value == ',':
                        if current_col:
                            result.append(self._parse_order_item(current_col))
                            current_col = []
                    else:
                        current_col.append(token)
                
                if current_col:
                    result.append(self._parse_order_item(current_col))
                
                return result
        return None

    def _parse_order_item(self, tokens) -> tuple[str, bool]:
        """Parse ORDER BY item
        ORDER BY項目を解析

        Args:
            tokens: List of SQL tokens
                   SQLトークンのリスト

        Returns:
            tuple[str, bool]: (column_name, ascending)
                         (カラム名, 昇順フラグ)
        """
        print("DEBUG: Parsing order item tokens:", tokens)  # デバッグ出力
        
        # 空白を除去
        tokens = [t for t in tokens if not t.is_whitespace]
        
        # カラム名を取得（最初のトークン）
        if isinstance(tokens[0], sqlparse.sql.Identifier):
            col = str(tokens[0].get_real_name())
        else:
            col = str(tokens[0])
        
        # 昇順/降順を判定（最後のトークン）
        ascending = True
        if len(tokens) > 1 and str(tokens[-1]).upper() == 'DESC':
            ascending = False
        
        print(f"DEBUG: Parsed order item - column: {col}, ascending: {ascending}")  # デバッグ出力
        return col, ascending

    def _find_limit_clause(self, tokens: List[Any]) -> Optional[int]:
        """Find and parse LIMIT clause
        LIMIT句を検索して解析

        Args:
            tokens: List of SQL tokens
                   SQLトークンのリスト

        Returns:
            Optional[int]: Limit value or None
                          制限値またはNone
        """
        for i, token in enumerate(tokens):
            if (isinstance(token, sqlparse.sql.Token) and 
                token.ttype is sqlparse.tokens.Keyword and 
                token.value.upper() == 'LIMIT'):
                if i + 1 >= len(tokens):
                    raise ValueError("Invalid LIMIT clause")
                
                try:
                    return int(str(tokens[i + 1]))
                except ValueError:
                    raise ValueError("Invalid LIMIT value")
        return None

    def _apply_order_by(self, df: pd.DataFrame, order_by: List[tuple[str, bool]]) -> pd.DataFrame:
        """Apply ORDER BY clause to DataFrame
        ORDER BY句をDataFrameに適用

        Args:
            df (pd.DataFrame): Target DataFrame
                             対象のDataFrame
            order_by: List of (column, ascending) pairs
                     (カラム名, 昇順フラグ)のリスト

        Returns:
            pd.DataFrame: Sorted DataFrame
                         ソート済みDataFrame
        """
        columns = [col for col, _ in order_by]
        ascending = [asc for _, asc in order_by]
        return df.sort_values(by=columns, ascending=ascending)

    def _find_join_clause(self, tokens) -> Optional[tuple]:
        """Find and parse JOIN clause
        JOIN句を検索して解析

        Args:
            tokens: List of SQL tokens
                   SQLトークンのリスト

        Returns:
            Optional[tuple]: (join_table, join_condition) if JOIN is found, None otherwise
                            JOINが見つかった場合は(結合テーブル, 結合条件)、見つからない場合はNone
        """
        for i, token in enumerate(tokens):
            if token.value.upper() == 'JOIN':
                # Get join table name
                if i + 1 >= len(tokens):
                    raise ValueError("Invalid JOIN clause: missing table name")
                join_table = str(tokens[i + 1])

                # Find ON clause
                for j in range(i + 2, len(tokens)):
                    if tokens[j].value.upper() == 'ON':
                        if j + 1 >= len(tokens):
                            raise ValueError("Invalid JOIN clause: missing condition")
                        # Get everything until the next major clause
                        condition_tokens = []
                        k = j + 1
                        while k < len(tokens) and tokens[k].value.upper() not in ('WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT'):
                            if not tokens[k].is_whitespace:
                                condition_tokens.append(tokens[k])
                            k += 1
                        condition = ' '.join(str(t) for t in condition_tokens)
                        return join_table, condition
                raise ValueError("Invalid JOIN clause: missing ON condition")
        return None

    def _parse_join_condition(self, condition: str) -> tuple:
        """Parse JOIN condition to get column names
        JOIN条件を解析してカラム名を取得

        Args:
            condition (str): JOIN condition (e.g., "users.id = orders.user_id")
                            JOIN条件（例："users.id = orders.user_id"）

        Returns:
            tuple: (left_column, right_column)
                   (左カラム, 右カラム)

        Raises:
            ValueError: When condition format is invalid
                       条件フォーマットが無効な場合
        """
        try:
            left_side, right_side = condition.split('=')
            left_col = left_side.strip().split('.')[-1]  # Remove table qualifier
            right_col = right_side.strip().split('.')[-1]  # Remove table qualifier
            return left_col, right_col
        except ValueError:
            raise ValueError(f"Invalid JOIN condition format: {condition}")

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

    def _apply_group_by(self, df: pd.DataFrame, group_by: List[str], select_clause: List[tuple]) -> pd.DataFrame:
        """Apply GROUP BY operations
        GROUP BY操作を適用

        Args:
            df (pd.DataFrame): Input DataFrame
            group_by (List[str]): Grouping columns
            select_clause (List[tuple]): Selected columns and aggregations

        Returns:
            pd.DataFrame: Grouped and aggregated DataFrame
        """
        print("DEBUG: Group by columns:", group_by)  # デバッグ出力
        print("DEBUG: Select clause for aggregation:", select_clause)  # デバッグ出力

        # Extract group by columns without table qualifiers
        group_cols = [col.split('.')[-1] for col in group_by]
        
        # Prepare aggregation dictionary with output column names
        agg_dict = {}
        output_columns = []
        
        for col, alias in select_clause:
            if col.split('.')[-1] in group_cols:
                output_columns.append((col.split('.')[-1], alias))
                continue
            
            # 集計関数のパターンをチェック
            agg_func = self._parse_aggregate_function(col)
            if agg_func:
                func_name, column, _ = agg_func
                func_info = self.AGGREGATE_FUNCTIONS[func_name]
                
                if column == '*':
                    # COUNT(*)の場合は最初のカラムを使用
                    agg_col = df.columns[0]
                    agg_dict[agg_col] = (func_info['pandas_func'], alias)
                else:
                    # その他の集計関数
                    agg_dict[column] = (func_info['pandas_func'], alias)
                output_columns.append((col, alias))
            else:
                # 集計関数でないカラムはGROUP BY句に含まれている必要がある
                if col not in group_cols:
                    raise ValueError(f"Column '{col}' must appear in the GROUP BY clause")
                output_columns.append((col, alias))

        print("DEBUG: Aggregation dictionary:", agg_dict)  # デバッグ出力
        
        # Perform groupby operation with named aggregations
        named_agg = {alias: (col, func) for col, (func, alias) in agg_dict.items()}
        result = df.groupby(group_cols, as_index=False).agg(**named_agg)
        
        # Add group by columns if they're not already in the result
        for col, alias in output_columns:
            if col.split('.')[-1] in group_cols and alias not in result.columns:
                result[alias] = df.groupby(group_cols)[col.split('.')[-1]].first()
        
        print("DEBUG: After groupby operation:\n", result)  # デバッグ出力
        
        return result

    def _parse_aggregate_function(self, token: str) -> Optional[tuple]:
        """Parse aggregate function
        集計関数を解析する

        Args:
            token (str): Token to parse (e.g., "COUNT(*)", "AVG(age)")
                        解析するトークン（例："COUNT(*)", "AVG(age)"）

        Returns:
            Optional[tuple]: (function_name, column, alias) if token is aggregate function
                            トークンが集計関数の場合は(関数名, カラム, エイリアス)を返す
                            それ以外の場合はNone
        """
        print("DEBUG: Parsing aggregate function:", token)  # デバッグ出力

        # Check if token contains aggregate function
        token_upper = token.upper()
        for func_name in self.AGGREGATE_FUNCTIONS:
            if func_name + '(' in token_upper:
                # Extract column and alias using case-sensitive pattern
                pattern = f"{func_name}\((.*?)\)(?: [Aa][Ss] (\w+))?"
                match = re.match(pattern, token, re.IGNORECASE)
                if not match:
                    raise ValueError(f"Invalid aggregate function format: {token}")
                
                print("DEBUG: Matched groups:", match.groups())  # デバッグ出力
                
                column = match.group(1).strip()
                alias = match.group(2) if match.group(2) else token
                
                # Validate function usage
                func_info = self.AGGREGATE_FUNCTIONS[func_name]
                if column == '*' and not func_info['allow_star']:
                    raise ValueError(f"{func_name}(*) is not supported")
                if func_info['needs_column'] and (not column or column == '*'):
                    raise ValueError(f"{func_name} requires a column name")
                
                # Keep original column name case, but use uppercase for function name
                if column != '*':
                    # Extract original case column name from the token
                    col_start = token.find('(') + 1
                    col_end = token.find(')')
                    column = token[col_start:col_end].strip()
                
                return func_name, column, alias
        
        return None
