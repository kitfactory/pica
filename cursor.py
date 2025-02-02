# Supported SQL Commands: For detailed information, please refer to docs/support.md.
# サポートされるSQL構文: 詳細については docs/support.md を参照してください. 

# --- Added new methods for CREATE TABLE support ---

class Cursor:
    def _create(self, sql: str) -> None:
        """Execute a CREATE TABLE statement.
        CREATE TABLE文を実行する。
        
        Parameters:
        -----------
        sql : str
            The SQL command string. / SQLコマンド文字列。
        
        Raises:
        -------
        ValueError:
            If the statement is invalid or table exists (when IF NOT EXISTS is not specified).
            文が無効な場合、または既にテーブルが存在している場合（IF NOT EXISTSが指定されていない場合）に例外を発生させる。
        
        Logs:
        -----
        Logs the input SQL and the actions executed.
        入力SQLと実行されたアクションをログ出力する。
        """
        import os, re, pandas as pd
        print(f"DEBUG: _create called with SQL: {sql}")  # 入力ログ

        # IF NOT EXISTS句の有無をチェック
        if_not_exists = bool(re.search(r'(?i)IF NOT EXISTS', sql))

        # SQL文からテーブル名および列定義を解析する
        sql_upper = sql.upper()
        index_table = sql_upper.find("TABLE")
        if index_table == -1:
            raise ValueError("Invalid CREATE TABLE statement: no TABLE keyword found")

        after_table = sql[index_table + len("TABLE"):].strip()
        if if_not_exists:
            after_table = re.sub(r'^(?i)IF NOT EXISTS', '', after_table).strip()

        # テーブル名を抽出（空白または(まで）
        match = re.match(r'([^\s(]+)', after_table)
        if not match:
            raise ValueError("Invalid CREATE TABLE statement: Unable to parse table name")
        table_name = match.group(1)

        # カラム定義を括弧内から抽出
        start_paren = after_table.find('(')
        end_paren = after_table.find(')')
        if start_paren == -1 or end_paren == -1 or end_paren < start_paren:
            raise ValueError("Invalid CREATE TABLE statement: No column definitions found.")
        columns_str = after_table[start_paren + 1:end_paren].strip()
        if not columns_str:
            raise ValueError("Invalid CREATE TABLE statement: Column definitions are empty.")

        columns = []
        for col_def in columns_str.split(','):
            col_def = col_def.strip()
            if col_def:
                # カラム定義は 'column_name type' の形式と仮定し、最初のトークンをカラム名とする
                col_name = col_def.split()[0]
                columns.append(col_name)

        # テーブルに相当するCSVファイルのパスを決定（例：base_dir/table_name.csv）
        base_dir = self.connection.base_dir if hasattr(self.connection, 'base_dir') else '.'
        file_path = os.path.join(base_dir, f"{table_name}.csv")
        print(f"DEBUG: Resolved file path for table '{table_name}': {file_path}")

        # ファイルの存在チェック
        if os.path.exists(file_path):
            print(f"DEBUG: File '{file_path}' already exists.")
            if if_not_exists:
                print("DEBUG: IF NOT EXISTS specified, no action taken.")
                return
            else:
                raise ValueError(f"Table '{table_name}' already exists.")
        
        # 指定されたカラムのみの空DataFrameを作成し、CSVファイルとして保存
        df = pd.DataFrame(columns=columns)
        df.to_csv(file_path, index=False)
        print(f"DEBUG: Created table '{table_name}' with columns {columns}")
        
    def _drop(self, sql: str) -> None:
        """Execute a DROP TABLE statement.
        DROP TABLE文を実行する。
        
        Parameters:
        -----------
        sql : str
            The SQL command string. / SQLコマンド文字列。
        
        Raises:
        -------
        ValueError:
            If the statement is invalid or the table does not exist.
            文が無効である場合や、テーブルが存在しない場合に例外を発生させる。
        
        Logs:
        -----
        Logs the input SQL and the actions executed.
        入力SQLと実行されたアクションをログ出力する。
        """
        import os, re
        print(f"DEBUG: _drop called with SQL: {sql}")

        # SQL文からテーブル名を抽出する
        sql_upper = sql.upper()
        index_table = sql_upper.find("TABLE")
        if index_table == -1:
            raise ValueError("Invalid DROP TABLE statement: no TABLE keyword found")

        after_table = sql[index_table + len("TABLE"):].strip()
        # テーブル名を抽出（空白またはセミコロンまで）
        match = re.match(r'([^\s;]+)', after_table)
        if not match:
            raise ValueError("Invalid DROP TABLE statement: Unable to parse table name")
        table_name = match.group(1)

        # テーブルに相当するCSVファイルのパスを決定（例：base_dir/table_name.csv）
        base_dir = self.connection.base_dir if hasattr(self.connection, 'base_dir') else '.'
        file_path = os.path.join(base_dir, f"{table_name}.csv")
        print(f"DEBUG: Resolved file path for table '{table_name}': {file_path}")

        # ファイルの存在チェック
        if not os.path.exists(file_path):
            raise ValueError(f"Table '{table_name}' does not exist.")
        else:
            os.remove(file_path)
            print(f"DEBUG: Deleted file for table '{table_name}'.")

        # 接続内のテーブルオブジェクトがあれば削除する（例: self.connection.tables）
        if hasattr(self.connection, 'tables'):
            if table_name in self.connection.tables:
                del self.connection.tables[table_name]
                print(f"DEBUG: Removed table object for '{table_name}' from connection.")
            else:
                print(f"DEBUG: No table object found for '{table_name}' in connection.")

    def execute(self, sql: str, *args, **kwargs):
        """Execute an SQL command by dispatching to the appropriate internal method.
        SQL文を実行し、内部メソッドへ振り分ける。
        """
        stripped = sql.strip().upper()
        if stripped.startswith("CREATE"):
            return self._create(sql)
        elif stripped.startswith("DROP"):
            return self._drop(sql)
        
        # 既存の処理（例：SELECT, INSERT, UPDATE, DELETE など）
        # ... existing dispatch logic ...

        raise NotImplementedError("SQL command not supported in this simplified demo.")

# --- End of added methods --- 