"""
Basic example of Pica DBAPI usage
基本的なPica DBAPIの使用例
"""
import pica

# Create a connection
# 接続を作成
conn = pica.connect()
cursor = conn.cursor()

try:
    # Create a new table
    # 新しいテーブルを作成
    cursor.execute("""
        CREATE TABLE fruits (
            name TEXT,
            price INTEGER
        )
    """)

    # Insert data
    # データを挿入
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Apple', 100)")
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Banana', 80)")
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Orange', 120)")

    # Select and show data
    # データを選択して表示
    cursor.execute("SELECT * FROM fruits")
    results = cursor.fetchall()
    for row in results:
        print(row)

    # Update data
    # データを更新
    cursor.execute("UPDATE fruits SET price = 90 WHERE name = 'Banana'")

    # Delete data
    # データを削除
    cursor.execute("DELETE FROM fruits WHERE name = 'Orange'")

    # Save changes to CSV file
    # 変更内容をCSVファイルに保存
    conn.commit()

    # Drop the table (this will delete the CSV file)
    # テーブルを削除（CSVファイルも削除されます）
    cursor.execute("DROP TABLE fruits")

except Exception as e:
    print(f"An error occurred: {e}")
    print(f"エラーが発生しました: {e}")
finally:
    conn.close()

if __name__ == "__main__":
    pass 