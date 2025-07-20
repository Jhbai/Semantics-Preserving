

import sqlglot
import re

def transpile_oracle_to_trino(oracle_sql):
    # Isolate the two MERGE statements from the PL/SQL block
    merge_statements = re.findall(r"MERGE INTO.*?IS NOT NULL;", oracle_sql, re.DOTALL | re.IGNORECASE)
    
    transpiled_merges = []
    
    for merge_sql in merge_statements:
        # Perform manual replacements for Oracle-specific syntax
        trino_sql = merge_sql.replace("SYSDATE", "CURRENT_TIMESTAMP")
        trino_sql = trino_sql.replace("SYSDATE - 1/2", "CURRENT_TIMESTAMP - INTERVAL '12' HOUR")
        trino_sql = re.sub(r"WHERE t.eqp_id IS NOT NULL;", ";", trino_sql)

        
        # Transpile using sqlglot
        try:
            transpiled_sql = sqlglot.transpile(trino_sql, read='oracle', write='trino', pretty=True)[0]
            transpiled_merges.append(transpiled_sql)
        except Exception as e:
            transpiled_merges.append(f"-- Could not transpile a MERGE statement, please review manually: {e}\n/*\n{merge_sql}\n*/")
            
    return "\n\n-- STATEMENT SEPARATOR --\n\n".join(transpiled_merges)

def main():
    try:
        with open("D:/sql_comparator/oracle.txt", "r", encoding="utf-8") as f:
            oracle_sql = f.read()

        trino_sql = transpile_oracle_to_trino(oracle_sql)

        with open("D:/sql_comparator/trino.txt", "w", encoding="utf-8") as f:
            f.write(trino_sql)

        print("Transpilation from Oracle to Trino completed successfully.")
        print("Output written to D:/google_cli/sql_comparator/trino.txt")
        
    except FileNotFoundError:
        print("Error: oracle.txt not found. Please make sure the file exists.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
