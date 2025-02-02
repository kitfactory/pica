# Pica DBAPI 🎉
Welcome to Pica DBAPI - a lightweight and fun **Pandas Integrated CSV API**!  
Pica stands for Pandas Integrated CSV API.

## Features 🌟
- Lightweight DBAPI built on Pandas and CSV 📊  
- Simple and intuitive API 🤩  
- Supports common SQL operations: SELECT, INSERT, UPDATE, DELETE, JOIN, GROUP BY 🛠️  
- Automatic lazy-loading of CSV files 🚀  
- CREATE TABLE and DROP TABLE operations 🗃️  
- Comprehensive test coverage with pytest ✅

## Installation 🔧
```bash
pip install pica-dbapi
```

## Supported SQL Operations 📝
Pica supports the following SQL operations:

- **SELECT**: Retrieve data from CSV files. 🔍
- **INSERT**: Insert new records into CSV files. ➕
- **UPDATE**: Update existing records in CSV files. 🔄
- **DELETE**: Delete records from CSV files. ❌
- **JOIN**: Join rows from multiple CSV files. 🔗
- **GROUP BY**: Aggregate records using GROUP BY clauses. 📊
- **CREATE TABLE**: Create a new CSV file with specified columns. 🆕
- **DROP TABLE**: Delete the CSV file and remove the corresponding table object. 🗑️

## Quick Start 🚀
Here's a simple example demonstrating basic CRUD operations with Pica:

```python
import pica

# Create a connection
conn = pica.connect()
cursor = conn.cursor()

try:
    # Create a new table
    cursor.execute("""
        CREATE TABLE fruits (
            name TEXT,
            price INTEGER
        )
    """)

    # Insert data
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Apple', 100)")
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Banana', 80)")
    cursor.execute("INSERT INTO fruits (name, price) VALUES ('Orange', 120)")

    # Select and show data
    cursor.execute("SELECT * FROM fruits")
    results = cursor.fetchall()
    for row in results:
        print(row)

    # Update data
    cursor.execute("UPDATE fruits SET price = 90 WHERE name = 'Banana'")

    # Delete data
    cursor.execute("DELETE FROM fruits WHERE name = 'Orange'")

    # Save changes to CSV file
    conn.commit()

    # Drop the table
    cursor.execute("DROP TABLE fruits")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    conn.close()
```

For more examples, please check the `example` directory in our repository.

## Contributing 🤝
Contributions and suggestions are welcome! Please open an issue or submit a pull request. 💬✨

## License 📄
This project is licensed under the MIT License.
