import sqlglot
import re
from rapidfuzz import fuzz

def get_fingerprints(sql_code, dialect):
    """
    Parses a string of SQL code and returns a list of normalized fingerprints.
    This function expects relatively clean SQL (no PL/SQL procedural blocks).

    Args:
        sql_code (str): The SQL code string.
        dialect (str): The SQL dialect to use for parsing ('oracle', 'trino', etc.).

    Returns:
        list: A list of normalized string representations (fingerprints) of the SQL statements.
              Returns an empty list if parsing fails or no valid statements are found.
    """
    fingerprints = []
    try:
        # sqlglot.parse can handle multiple statements separated by semicolons
        expressions = sqlglot.parse(sql_code, read=dialect)
        for exp in expressions:
            if exp:
                # Transpile to a neutral dialect (internal representation) with pretty formatting for normalization
                fingerprints.append(exp.sql(pretty=True))
    except Exception as e:
        # Silently ignore parsing errors for non-standard SQL or incomplete statements
        pass
    return fingerprints

def compare_sql_logic(oracle_filepath, trino_filepath):
    """
    Compares the logical similarity of SQL in Oracle and Trino files.

    Args:
        oracle_filepath (str): Path to the Oracle SQL file.
        trino_filepath (str): Path to the Trino SQL file.

    Returns:
        float: A similarity score between 0.0 and 1.0.
    """
    try:
        with open(oracle_filepath, 'r', encoding='utf-8') as f:
            oracle_raw_code = f.read()
        with open(trino_filepath, 'r', encoding='utf-8') as f:
            trino_raw_code = f.read()

        # --- Oracle SQL Extraction ---
        # Use regex to remove the entire PL/SQL block, leaving only standard SQL.
        # This regex looks for blocks starting with DECLARE or BEGIN, ending with END; and optional /.
        plsql_pattern = re.compile(r"(DECLARE|BEGIN).+?END;\s*?^/$", re.MULTILINE | re.DOTALL | re.IGNORECASE)
        oracle_standard_sql = plsql_pattern.sub('', oracle_raw_code)

        # --- Trino SQL Extraction ---
        # Clean up comments and custom markers from the Trino file.
        trino_clean_code = re.sub(r'-- \[Gemini\].*?(\n|$)', '', trino_raw_code) # Remove our custom line comments
        trino_clean_code = re.sub(r'/\*.*?\*/', '', trino_clean_code, flags=re.DOTALL) # Remove block comments
        trino_clean_code = re.sub(r'-- STATEMENT SEPARATOR --', '', trino_clean_code) # Remove separators

        # --- Generate Logical Fingerprints ---
        oracle_fingerprints = get_fingerprints(oracle_standard_sql, 'oracle')
        trino_fingerprints = get_fingerprints(trino_clean_code, 'trino')

        if not trino_fingerprints:
            # If there's no valid Trino SQL to compare against, similarity is 0.
            return 0.0
        
        if not oracle_fingerprints:
            # If there's no valid standard Oracle SQL (only PL/SQL), similarity is 0.
            return 0.0

        # --- Similarity Calculation ---
        # For each Oracle fingerprint, find its best matching Trino fingerprint.
        total_similarity = 0
        for o_fp in oracle_fingerprints:
            best_match_score = max(
                (fuzz.token_set_ratio(o_fp, t_fp) / 100.0 for t_fp in trino_fingerprints),
                default=0.0 # If no Trino fingerprints, default to 0 for this Oracle statement
            )
            total_similarity += best_match_score
        
        # Return the average best match score.
        return total_similarity / len(oracle_fingerprints)

    except FileNotFoundError:
        print("Error: One or both SQL files not found.")
        return 0.0
    except Exception as e:
        print(f"An unexpected error occurred during comparison: {e}")
        return 0.0

if __name__ == '__main__':
    # --- Test Case Setup ---
    # Create a sample oracle.txt with mixed standard SQL and PL/SQL
    sample_oracle_code = """
-- Standard SQL query
SELECT product_id, product_name FROM products WHERE category_id = 1;

-- PL/SQL block
DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM employees;
  DBMS_OUTPUT.PUT_LINE('Employee count: ' || v_count);
END;
/

-- Another standard SQL query
INSERT INTO sales (item_id, quantity) VALUES (101, 5);
"""
    with open('D:/google_cli/sql_comparator/oracle.txt', 'w', encoding='utf-8') as f:
        f.write(sample_oracle_code)

    # Create a sample trino.txt with corresponding Trino SQL
    sample_trino_code = """
-- Trino equivalent of the first query
SELECT product_id, product_name FROM products WHERE category_id = 1;

-- Trino equivalent of the second query
INSERT INTO sales (item_id, quantity) VALUES (101, 5);

-- Some unrelated Trino comment
-- [Gemini] This was a PL/SQL block, manually converted.
"""
    with open('D:/google_cli/sql_comparator/trino.txt', 'w', encoding='utf-8') as f:
        f.write(sample_trino_code)

    # --- Run Comparison ---
    oracle_file = 'D:/google_cli/sql_comparator/oracle.txt'
    trino_file = 'D:/google_cli/sql_comparator/trino.txt'
    
    similarity_score = compare_sql_logic(oracle_file, trino_file)
    
    print(f"The logical similarity score between the two SQL files is: {similarity_score:.4f}")