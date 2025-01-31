# 🎯 Pica - シンプルなCSV/Pandas DB-APIインターフェース

🌟 PicaはCSVファイルとPandas DataFrameのためのDB-API 2.0準拠のインターフェースを提供するPythonライブラリです。CSVデータの操作を従来のデータベースのように簡単に行うことができます！

## ✨ 特徴

- 🔌 DB-API 2.0準拠のインターフェース
- 📊 Pandas DataFrameとのシームレスな統合
- 📁 CSVファイルの直接操作
- 🚀 シンプルで直感的なSQLライクなクエリ
- 🛠 設定不要

## 📥 インストール

> pip install pica

## 🚀 クイックスタート

```python
import pica

# CSVファイルに接続
conn = pica.connect('data.csv')
cursor = conn.cursor()

# SQLクエリの実行
cursor.execute("SELECT * FROM data WHERE age > 25")
results = cursor.fetchall()

# Pandas DataFrameとして操作
df = cursor.to_dataframe()
```

## 🎯 サポートされているSQL操作

現在サポートされているSQL操作：

- SELECT: 基本的な列選択クエリ
- WHERE: 比較演算子（=, >, <, >=, <=, !=）を使用した条件フィルタリング
- ORDER BY: 結果のソート（昇順/降順）
- LIMIT: 返される行数の制限
- GROUP BY: 集計関数を使用したグループ化
- HAVING: グループ化された結果のフィルタリング
- JOIN: 複数のCSVファイル間の内部結合

## 📚 使用例

### 基本的なクエリ:
# 条件付きクエリ
cursor.execute("SELECT name, age FROM users WHERE age > 30 ORDER BY name")

### Pandasとの使用:
# CSVのインポートとクエリ
df = pd.read_csv('data.csv')
conn = pica.connect(dataframe=df)
cursor = conn.cursor()
cursor.execute("SELECT * FROM data GROUP BY category HAVING COUNT(*) > 5")

### 複数ファイルの操作:
# 結合操作
cursor.execute("""
    SELECT users.name, orders.product 
    FROM users 
    JOIN orders ON users.id = orders.user_id
""")

## 🔧 必要要件

- Python 3.10以上
- pandas
- pytest（開発用）

## 📖 ドキュメント

詳細なドキュメントは以下のドキュメントページをご覧ください: https://pica.readthedocs.io/

## 🤝 コントリビューション

コントリビューションを歓迎します！お気軽にPull Requestを送ってください。

## 📝 ライセンス

このプロジェクトはMITライセンスの下で公開されています - 詳細はLICENSEファイルをご覧ください。