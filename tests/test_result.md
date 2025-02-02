# Test Results 🧪

Last test run: 2024-03-21

## Summary 📊
- Total tests: 31
- Passed: 31 ✅
- Failed: 0 ❌
- Warnings: 2 ⚠️
- Execution time: 1.34s ⚡

## Test Details 📝

### Connection Tests
- ✅ test_connection_creation
- ✅ test_table_registration

### Create Table Tests
- ✅ test_create_table_success
- ✅ test_create_table_if_not_exists_success
- ✅ test_create_table_already_exists_error

### Cursor Tests
- ✅ test_basic_select
- ✅ test_data_types
- ✅ test_type_based_comparisons
- ✅ test_aggregate_functions
- ✅ test_join_with_conditions
- ✅ test_invalid_sql
- ✅ test_fetch_methods
- ✅ test_parameterized_query
- ✅ test_fruit_operations

### Drop Table Tests
- ✅ test_drop_table_success
- ✅ test_drop_table_error

### Example Basic Tests
- ✅ test_basic_select_where
- ✅ test_group_by
- ✅ test_join_operation
- ✅ test_dataframe_usage
- ✅ test_lazy_loading_success
- ✅ test_lazy_loading_file_not_found
- ✅ test_lazy_loading_invalid_csv

### Lazy Loading Tests
- ✅ test_insert_lazy_loading_success
- ✅ test_insert_lazy_loading_file_not_found
- ✅ test_update_lazy_loading_success
- ✅ test_update_lazy_loading_missing_table
- ✅ test_delete_lazy_loading_success
- ✅ test_delete_lazy_loading_file_not_found
- ✅ test_join_lazy_loading_success
- ✅ test_join_lazy_loading_file_not_found

## Warnings ⚠️
1. Regular Expression Warning:
   - Location: `cursor.py:41`
   - Message: DeprecationWarning: Flags not at the start of the expression '^(?i)IF NOT EXISTS' but at position 1

2. Pandas Type Warning:
   - Location: `src/pica/cursor.py:591`
   - Message: FutureWarning: Setting an item of incompatible dtype is deprecated 