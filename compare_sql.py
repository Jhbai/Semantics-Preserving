

import sqlglot
from sqlglot import exp
from sqlglot.optimizer import normalize
import re

def map_table_names(expression, table_mapping):
    """
    Recursively replaces table names in an expression tree based on a mapping.
    Modifies the tree in-place.
    """
    for table_expr in expression.find_all(exp.Table):
        # Ensure we are looking at the base name for mapping
        base_name = table_expr.this.name
        if base_name in table_mapping:
            mapped_name = table_mapping[base_name]
            # Parse the new name to correctly handle multipart identifiers (catalog.db.table)
            # We create a dummy SELECT statement to parse the table correctly
            parsed_mapped_table = sqlglot.parse_one(f"SELECT * FROM {mapped_name}", read='trino').find(exp.Table)
            if parsed_mapped_table:
                # Replace the current table_expr's parts with the mapped parts
                table_expr.this.replace(parsed_mapped_table.this)
                table_expr.set('db', parsed_mapped_table.db)
                table_expr.set('catalog', parsed_mapped_table.catalog)

def preprocess_tree(sql_tree, table_mapping):
    """
    Applies all necessary transformations to a SQL tree before comparison.
    """
    # 1. Remove all comments by clearing the comments attribute of each node
    for node in sql_tree.walk():
        if hasattr(node, 'comments') and node.comments:
            node.comments = []

    # 2. Apply table name mapping
    if table_mapping:
        map_table_names(sql_tree, table_mapping)

    # 3. Normalize specific constructs
    def transform_node(node):
        # Handle CAST(STR_TO_TIME(...)) first
        if isinstance(node, exp.Cast) and isinstance(node.this, exp.Func) and node.this.name.upper() == 'STR_TO_TIME':
            # Replace CAST(STR_TO_TIME(date_str, format) AS DATE) with CAST(date_str AS DATE)
            simplified_date_expr = node.this.this # This is the date string literal
            node.this = simplified_date_expr # Directly modify the 'this' argument of the CAST node
            return node # Return the modified CAST node
        # Handle TO_DATE functions
        elif isinstance(node, exp.Func) and node.name.upper() == 'TO_DATE':
            # Replace TO_DATE(date_str, format) with CAST(date_str AS DATE)
            date_string_expr = node.this
            return exp.Cast(this=date_string_expr, to=exp.DataType(this=exp.DataType.Type.DATE)) # Return a new CAST node
        # Handle standalone STR_TO_TIME functions (not inside a CAST)
        elif isinstance(node, exp.Func) and node.name.upper() == 'STR_TO_TIME':
            # Replace STR_TO_TIME(date_str, format) with date_str
            return node.this
        
        # Standardize CREATE TABLE properties (remove Oracle-specific ON COMMIT etc.)
        if isinstance(node, exp.Create):
            node.set('properties', [])
            # Ensure temporary flag is consistent for comparison if it exists
            if node.args.get('temporary'):
                node.set('kind', 'TABLE') # Ensure it's not CREATE TEMPORARY VIEW etc.
        
        return node

    sql_tree = sql_tree.transform(transform_node)

    # 4. Use sqlglot's built-in normalizer for a final pass
    # This handles quoting, alias normalization, etc.
    return normalize.normalize(sql_tree)

def compare_sql_files(oracle_file, trino_file, table_mapping=None):
    """
    Compares two SQL files by normalizing their expression trees and comparing the resulting SQL text.
    """
    try:
        with open(oracle_file, 'r', encoding='utf-8') as f:
            oracle_sql = f.read()
        with open(trino_file, 'r', encoding='utf-8') as f:
            trino_sql = f.read()

        # Remove comments from raw SQL strings before parsing
        oracle_sql = re.sub(r'--.*$', '', oracle_sql, flags=re.MULTILINE) # Single-line comments
        oracle_sql = re.sub(r'/\*.*?\*/', '', oracle_sql, flags=re.DOTALL) # Multi-line comments

        trino_sql = re.sub(r'--.*$', '', trino_sql, flags=re.MULTILINE)
        trino_sql = re.sub(r'/\*.*?\*/', '', trino_sql, flags=re.DOTALL)

        # Parse SQL
        oracle_expressions = sqlglot.parse(oracle_sql, read='oracle')
        trino_expressions = sqlglot.parse(trino_sql, read='trino')

        if len(oracle_expressions) != len(trino_expressions):
            return False, f"Mismatch in number of statements: Oracle has {len(oracle_expressions)}, Trino has {len(trino_expressions)}."

        for i, oracle_expr in enumerate(oracle_expressions):
            trino_expr = trino_expressions[i]

            # Transpile Oracle to Trino first
            try:
                # Removed pretty=True from transpile to avoid re-introducing comments
                transpiled_sql = sqlglot.transpile(oracle_expr.sql(), read='oracle', write='trino')[0]
                oracle_tree_transpiled = sqlglot.parse_one(transpiled_sql, read='trino')
            except Exception as e:
                return False, f"Error transpiling Oracle statement #{i+1}: {e}"

            # Apply all preprocessing steps to both trees
            final_oracle_tree = preprocess_tree(oracle_tree_transpiled, table_mapping)
            final_trino_tree = preprocess_tree(trino_expr, None) # No mapping needed for trino file

            # Generate the final, normalized SQL strings for comparison
            # Removed pretty=True from sql() to avoid re-introducing comments
            final_oracle_sql = final_oracle_tree.sql()
            final_trino_sql = final_trino_tree.sql()

            print(f"--- Comparing Statement #{i+1} ---")
            print(f"Oracle (Processed):\n{final_oracle_sql}")
            print(f"Trino (Processed):\n{final_trino_sql}")
            print("------------------------------------")

            if final_oracle_sql != final_trino_sql:
                return False, f"SQL statements at index {i+1} do not match after normalization."

        return True, "All SQL statements appear to be semantically equivalent."

    except FileNotFoundError as e:
        return False, f"Error: {e}"
    except Exception as e:
        return False, f"An unexpected error occurred: {e}"

if __name__ == '__main__':
    ORACLE_FILE_PATH = 'oracle.txt'
    TRINO_FILE_PATH = 'trino.txt'

    TABLE_MAPPING = {
        'sales': 'iceberg.default.sales',
        'products': 'iceberg.default.products',
        'categories': 'iceberg.default.categories',
        'departments': 'iceberg.default.departments',
        'employees': 'iceberg.default.employees'
    }

    are_equivalent, message = compare_sql_files(ORACLE_FILE_PATH, TRINO_FILE_PATH, table_mapping=TABLE_MAPPING)

    print(f"Comparison Result: {'Equivalent' if are_equivalent else 'Not Equivalent'}")
    print(f"Details: {message}")
