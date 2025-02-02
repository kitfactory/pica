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
Below is a quick start guide demonstrating key features (as shown in example_basic.py):

```bash
# Clone the repository 📥
git clone https://github.com/kitfactory/pica.git
cd pica

# Create a virtual environment and install dependencies 🛠️
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Unix/Linux:
# source .venv/bin/activate

# Install Pica in editable mode 🔧
pip install -e .

# Run the example ▶️
python example/example_basic.py
```

This example demonstrates various features including:
- Basic SELECT with WHERE clause 🔍
- GROUP BY with aggregation 📊
- JOIN operations between CSV-backed tables 🔗
- Direct usage with Pandas DataFrame 🐼
- Automatic lazy-loading of CSV files when initial DataFrames are not provided 🚀
- CREATE TABLE and DROP TABLE functionalities 🗃️ 🗑️

## Contributing 🤝
Contributions and suggestions are welcome! Please open an issue or submit a pull request. 💬✨

## License 📄
This project is licensed under the MIT License.
