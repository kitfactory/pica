# Lazy-Loading 対応作業手順

以下は、INSERT、UPDATE、DELETE、JOIN 等の SQL 操作に対して lazy-loading を順次対応するための作業手順です。

1. [x] lazy-loading を担当する統合関数（例：load_table_if_needed）の実装
   - 指定されたテーブルがメモリ上に存在するかをチェックし、未ロードなら CSV 等から自動読み込みを実施。

2. SQL 文のパースが完了した後、SQL処理実行直前に、対象テーブル名一覧を抽出し、各テーブルに対して load_table_if_needed を呼び出す処理の追加
   - 抽出された各テーブルに対して、必要に応じて lazy-loading を実施。

3. [x] SELECT 文に関しては既に lazy-loading が動作しているため、動作確認およびテストの実施。

4. [x] INSERT 操作において、対象テーブルがロード済みかのチェックと、未ロードなら lazy-loading を実行する処理の追加。

5. [x] UPDATE 操作において、更新対象のテーブルが未ロードの場合は自動的に lazy-loading を実行する処理の追加。

6. [ ] DELETE 操作において、対象テーブルが未ロードの場合は自動的に lazy-loading を実行する処理の追加（INSERT および UPDATE と同様の方法）。

7. JOIN 操作において、結合に参加する各テーブルがロードされているかを確認し、必要なテーブルはすべて lazy-loading によってロードする処理の追加。

8. 各操作ごとにユニットテストや統合テストを実施し、lazy-loading 機能が正しく動作していることを検証。 