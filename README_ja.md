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
import pica
import pandas as pd

# 接続を作成
conn = pica.connect()

# DataFrameをテーブルとして登録
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

conn.register_table('users', df, {
    'id': 'INTEGER',
    'name': 'TEXT',
    'age': 'INTEGER'
})

# SQLクエリを実行
cursor = conn.cursor()
cursor.execute("SELECT name, age FROM users WHERE age > 25")
results = cursor.fetchall()
print(results)  # [('Bob', 30), ('Charlie', 35)]
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