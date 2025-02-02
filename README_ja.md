# Pica DBAPI 🎉
ようこそ、Pica DBAPI へ！
Pica は Pandas Integrated CSV API の略称です。

## 特徴 🌟
- Pandas と CSV をベースにした軽量なDBAPI 📊
- シンプルで直感的なAPI 🤩
- SELECT, INSERT, UPDATE, DELETE, JOIN, GROUP BY などの一般的なSQL操作に対応 🛠️
- CSVファイルの自動 lazy-loading 🚀
- CREATE TABLE と DROP TABLE 操作 🗃️
- pytest を用いた包括的なテストカバレッジ ✅

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
以下は example_basic.py に基づいたクイックスタートガイドです:

```bash
# リポジトリをクローンする 📥
git clone https://github.com/kitfactory/pica.git
cd pica

# 仮想環境を作成し、依存関係をインストールする 🛠️
python -m venv .venv
# Windowsの場合:
.venv\Scripts\activate
# Unix/Linuxの場合:
# source .venv/bin/activate

# Pica を編集可能モードでインストールする 🔧
pip install -e .

# 例を実行する ▶️
python example/example_basic.py
```

この例では、以下の機能がデモされています:
- WHERE句を使用した基本的なSELECT 🔍
- GROUP BY を伴う集計 📊
- CSVファイルをベースとしたテーブル間のJOIN操作 🔗
- Pandas DataFrame の直接利用 🐼
- 初期DataFrameが提供されない場合のCSVファイル自動 lazy-loading 🚀
- CREATE TABLE と DROP TABLE の機能 🗃️ 🗑️

## 貢献 🤝
ご意見・ご提案、大歓迎です！
Issue の投稿や Pull Request の作成をお願いします。💬✨

## ライセンス 📄
このプロジェクトは MIT License の元でライセンスされています。