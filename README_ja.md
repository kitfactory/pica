# Pica DBAPI 🎉
ようこそ、Pica DBAPI へ！
Pica は Pandas Integrated CSV API の略称です。

## 特徴 🌟
- Pandas と CSV をベースにした軽量なDBAPI 📊
- シンプルで直感的なAPI 🤩
- SELECT, INSERT, UPDATE, DELETE, JOIN, GROUP BY などの一般的なSQL操作に対応 🛠️
- CREATE TABLE と DROP TABLE 操作 🗃️
- CSVファイルの自動 lazy-loading 🚀

## インストール 🔧
```bash
pip install pica-dbapi
```

## サポートされるSQL操作 📝
Pica でサポートされる SQL 操作は次の通りです:

- **SELECT**: CSVファイルからデータを取得する。🔍
- **INSERT**: CSVファイルに新しいレコードを挿入する。➕
- **UPDATE**: CSVファイル内の既存レコードを更新する。🔄
- **DELETE**: CSVファイルからレコードを削除する。❌
- **JOIN**: 複数のCSVファイルの行を結合する。🔗
- **GROUP BY**: GROUP BY句を使用してレコードを集約する。📊
- **CREATE TABLE**: 指定されたカラムを持つ新しいCSVファイルを作成する。🆕
- **DROP TABLE**: CSVファイルを削除し、対応するテーブルオブジェクトを除去する。🗑️

## クイックスタート 🚀
以下は基本的なCRUD操作を示すシンプルな例です：

```python
import pica

# 接続を作成
conn = pica.connect()
cursor = conn.cursor()

try:
    # 新しいテーブルを作成
    cursor.execute("""
        CREATE TABLE fruits (
            name TEXT,
            price INTEGER
        )
    """)

    # データを挿入
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Apple', 100)")
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Banana', 80)")
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Orange', 120)")

    # データを選択して表示
    cursor.execute("SELECT * FROM fruits")
    results = cursor.fetchall()
    for row in results:
        print(row)

    # データを更新
    cursor.execute("UPDATE fruits SET price = 90 WHERE name = 'Banana'")

    # データを削除
    cursor.execute("DELETE FROM fruits WHERE name = 'Orange'")

    # 変更内容をCSVファイルに保存
    conn.commit()

    # テーブルを削除（CSVファイルも削除されます）
    cursor.execute("DROP TABLE fruits")

except Exception as e:
    print(f"エラーが発生しました: {e}")
finally:
    conn.close()
```

より詳細な例については、リポジトリの `example` ディレクトリをご確認ください。

## 貢献 🤝
ご意見・ご提案、大歓迎です！
Issue の投稿や Pull Request の作成をお願いします。💬✨

## ライセンス 📄
このプロジェクトは MIT License の元でライセンスされています。