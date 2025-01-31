# 🎯 Pica - Simple CSV/Pandas DB-API Interface

🌟 Pica is a Python library that provides a DB-API 2.0 compliant interface for CSV files and Pandas DataFrames. It makes working with CSV data as easy as using a traditional database!

## ✨ Features

- 🔌 DB-API 2.0 compliant interface
- 📊 Seamless integration with Pandas DataFrames
- 📁 Direct CSV file operations
- 🚀 Simple and intuitive SQL-like queries
- 🛠 Zero configuration required

## 📥 Installation

pip install pica

## 🚀 Quick Start

import pica

# Connect to a CSV file
conn = pica.connect('data.csv')
cursor = conn.cursor()

# Execute SQL queries
cursor.execute("SELECT * FROM data WHERE age > 25")
results = cursor.fetchall()

# Work with Pandas DataFrame
df = cursor.to_dataframe()

## 🎯 Supported SQL Operations

Currently supported SQL operations include:

- SELECT: Basic queries with column selection
- WHERE: Filter conditions with comparison operators (=, >, <, >=, <=, !=)
- ORDER BY: Sort results (ASC/DESC)
- LIMIT: Limit number of returned rows
- GROUP BY: Group results with aggregation functions
- HAVING: Filter grouped results
- JOIN: Inner joins between multiple CSV files

## 📚 Examples

### Basic Query:
# Query with conditions
cursor.execute("SELECT name, age FROM users WHERE age > 30 ORDER BY name")

### Using with Pandas:
# Import CSV and query
df = pd.read_csv('data.csv')
conn = pica.connect(dataframe=df)
cursor = conn.cursor()
cursor.execute("SELECT * FROM data GROUP BY category HAVING COUNT(*) > 5")

### Multiple File Operations:
# Join operations
cursor.execute("""
    SELECT users.name, orders.product 
    FROM users 
    JOIN orders ON users.id = orders.user_id
""")

## 🔧 Requirements

- Python 3.10+
- pandas
- pytest (for development)

## 📖 Documentation

For detailed documentation, please visit our documentation page: https://pica.readthedocs.io/

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.