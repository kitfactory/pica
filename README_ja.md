# 🐼 Pica - Pandas DataFrameのためのシンプルなSQLインターフェース

Picaは、Python DB-API 2.0仕様に準拠したPandas DataFrame用の軽量なPythonライブラリです。PandasのパワーをそのままにSQLの構文で直感的にDataFrameを操作することができます。

## ✨ 特徴

- 🔍 Pandas DataFrameのためのSQL風インターフェース
- 📊 一般的なSQL操作をサポート
- 🐍 Python DB-API 2.0準拠
- 🚀 使いやすく、導入が簡単
- 📝 永続化のためのCSVファイルサポート

## 🛠️ インストール

```bash
pip install pica-dbapi
```

## 🎯 クイックスタート

```python
import os
import sys
import pandas as pd

# 上位ディレクトリをシステムパスに追加してpicaをインポート
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pica

def main():
    """CSVファイルとDataFrameを使用したPicaの基本的な使用例"""
    # サンプルデータの作成
    users_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 22],
        'department': ['Sales', 'IT', 'Sales', 'Marketing', 'IT']
    }

    orders_data = {
        'order_id': [1, 2, 3, 4, 5],
        'user_id': [1, 2, 1, 3, 5],
        'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Printer'],
        'amount': [1000, 20, 50, 200, 150]
    }

    # スキーマの定義
    users_schema = {
        'id': 'INTEGER',
        'name': 'TEXT',
        'age': 'INTEGER',
        'department': 'TEXT'
    }

    orders_schema = {
        'order_id': 'INTEGER',
        'user_id': 'INTEGER',
        'product': 'TEXT',
        'amount': 'INTEGER'
    }

    # DataFrameを作成
    users_df = pd.DataFrame(users_data)
    orders_df = pd.DataFrame(orders_data)

    # DataFrameを指定して接続を初期化
    initial_tables = {
        "users": users_df,
        "orders": orders_df
    }
    conn = pica.connect(dataframes=initial_tables)
    cursor = conn.cursor()

    # スキーマを登録
    conn.register_schema("users", users_schema)
    conn.register_schema("orders", orders_schema)

    # 例1: WHEREを使用した基本的なSELECT
    print("\n=== 例1: WHEREを使用した基本的なSELECT ===")
    cursor.execute("SELECT name, age FROM users WHERE age > 25")
    results = cursor.fetchall()
    print("25歳を超えるユーザ:")
    for row in results:
        print(row)

    # 例2: 集計とGROUP BYの例
    print("\n=== 例2: 集計とGROUP BYの例 ===")
    cursor.execute(""" 
        SELECT department, COUNT(*) as count, AVG(age) as avg_age 
        FROM users 
        GROUP BY department
    """)
    results = cursor.fetchall()
    print("部署別統計:")
    for row in results:
        print(row)

    # 例3: JOIN操作の例（2つのDataFrameを使用）
    print("\n=== 例3: JOIN操作の例 ===")
    cursor.execute(""" 
        SELECT 
            users.name as customer_name,
            orders.product as product_name,
            orders.amount as order_amount
        FROM users
        JOIN orders ON users.id = orders.user_id
        ORDER BY amount DESC
    """)
    results = cursor.fetchall()
    print("ユーザの注文:")
    for row in results:
        print(row)

    # 例4: Pandas DataFrameを直接使用する例
    print("\n=== 例4: Pandas DataFrameを直接使用する例 ===")
    cursor.execute(""" 
        SELECT name, age 
        FROM users 
        WHERE department = 'IT' 
        ORDER BY age DESC
    """)
    results = cursor.fetchall()
    print("IT部署のメンバー:")
    for row in results:
        print(row)

    # --- Lazy-loadingの例 ---
    print("\n=== 例5: Lazy-loadingの例 ===")
    # CSVファイルが置かれているディレクトリを指定（この例ではこのファイルと同じディレクトリと仮定）
    base_dir = os.path.dirname(__file__)
    print("base_dir:", base_dir)

    csv_files = [
        os.path.join(base_dir, 'users.csv'),
        os.path.join(base_dir, 'orders.csv')
    ]
    print('file1:', os.path.exists(csv_files[0]))
    print('file2:', os.path.exists(csv_files[1]))
    # 初期データフレームなしで接続を作成してlazy-loadingをトリガー
    conn_lazy = pica.connect(base_dir=base_dir)
    try:
        cursor = conn_lazy.cursor()
        cursor.execute("SELECT * FROM users")
        results = cursor.fetchall()
        print("Lazy-loadedされたusersデータ:")
        print(results)
    except Exception as e:
        print("Lazy-loading中のエラー:", e)

if __name__ == "__main__":
    main()
```

## 🔥 サポートされているSQL操作

### SELECT
- 基本的なSELECTとカラム選択
- WHERE句と比較演算子 (=, >, <, >=, <=, !=)
- GROUP BYと集計関数 (COUNT, SUM, AVG, MAX, MIN)
- ORDER BY (昇順/降順)
- JOIN操作
- エイリアス (AS)

例：
```sql
SELECT name, AVG(age) as avg_age 
FROM users 
WHERE age > 25 
GROUP BY name 
ORDER BY avg_age DESC
```

### INSERT
- 基本的なINSERT INTOとVALUES

例：
```sql
INSERT INTO users (name, age) VALUES ('David', 28)
```

### UPDATE
- WHERE句付きのUPDATE

例：
```sql
UPDATE users SET age = 29 WHERE name = 'Alice'
```

### DELETE
- WHERE句付きのDELETE

例：
```sql
DELETE FROM users WHERE age < 25
```

## 📊 サポートされているデータ型

- INTEGER（整数）
- REAL（実数）
- BOOLEAN（真偽値）
- DATE（日付）
- TEXT（テキスト）

## 🔄 トランザクションサポート

```python
conn = pica.connect()
try:
    # 操作を実行
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET age = 26 WHERE name = 'Alice'")
    conn.commit()
except:
    conn.rollback()
finally:
    conn.close()
```

## 📝 ライセンス

MITライセンス

## 🤝 コントリビューション

コントリビューションを歓迎します！お気軽にプルリクエストを送ってください。