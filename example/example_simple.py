import pica

def main():
    """
    Simple example of using Pica DBAPI with basic operations
    Pica DBAPIの基本的な操作を示すシンプルな例
    """
    # Create a connection
    # 接続を作成
    conn = pica.connect()
    cursor = conn.cursor()

    try:
        # Create a new table
        # 新しいテーブルを作成
        print("\n=== Creating table ===")
        cursor.execute("""
            CREATE TABLE fruits (
                name TEXT,
                price INTEGER
            )
        """)
        print("Table 'fruits' created successfully")

        # Insert some data
        # データを挿入
        print("\n=== Inserting data ===")
        cursor.execute("INSERT INTO fruits (name, price) VALUES ('Apple', 100)")
        cursor.execute("INSERT INTO fruits (name, price) VALUES ('Banana', 80)")
        cursor.execute("INSERT INTO fruits (name, price) VALUES ('Orange', 120)")
        print("Data inserted successfully")

        # Select and show all data
        # 全データを選択して表示
        print("\n=== Selecting all data ===")
        cursor.execute("SELECT * FROM fruits")
        results = cursor.fetchall()
        for row in results:
            print(row)

        # Update data
        # データを更新
        print("\n=== Updating data ===")
        cursor.execute("UPDATE fruits SET price = 90 WHERE name = 'Banana'")
        print("Data updated successfully")

        # Show updated data
        # 更新されたデータを表示
        print("\n=== Showing updated data ===")
        cursor.execute("SELECT * FROM fruits")
        results = cursor.fetchall()
        for row in results:
            print(row)

        # Delete data
        # データを削除
        print("\n=== Deleting data ===")
        cursor.execute("DELETE FROM fruits WHERE name = 'Orange'")
        print("Data deleted successfully")

        # Show remaining data
        # 残りのデータを表示
        print("\n=== Showing remaining data ===")
        cursor.execute("SELECT * FROM fruits")
        results = cursor.fetchall()
        for row in results:
            print(row)

        # Drop the table
        # テーブルを削除
        print("\n=== Dropping table ===")
        cursor.execute("DROP TABLE fruits")
        print("Table dropped successfully")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main() 