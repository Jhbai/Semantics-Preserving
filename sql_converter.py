

import sqlglot
import re

def transpile_oracle_to_trino(oracle_sql):
    """
    Transpiles standard Oracle SQL to Trino, and isolates PL/SQL blocks for manual conversion.

    Args:
        oracle_sql: A string containing Oracle SQL and potentially PL/SQL.

    Returns:
        A string with transpiled Trino SQL and clearly marked PL/SQL blocks.
    """
    # This regex is designed to find standalone PL/SQL blocks.
    # It looks for blocks starting with DECLARE or BEGIN, and ending with END;
    # It handles the final / on a new line, which is common in SQL tools.
    plsql_pattern = re.compile(r"(^|\s)(DECLARE|BEGIN).+?END;\s*?^/$", re.MULTILINE | re.DOTALL | re.IGNORECASE)

    # We will store the final output parts here
    final_output = []
    last_end = 0

    for match in plsql_pattern.finditer(oracle_sql):
        # Add the standard SQL part that came before this PL/SQL block
        start, end = match.span()
        non_plsql_part = oracle_sql[last_end:start].strip()
        if non_plsql_part:
            try:
                transpiled_sql = sqlglot.transpile(non_plsql_part, read='oracle', write='trino', pretty=True)
                final_output.extend(transpiled_sql)
            except Exception as e:
                final_output.append(f"-- [Gemini] Could not transpile a SQL block, please review manually: {e}\n/*\n{non_plsql_part}\n*/")

        # Add the identified PL/SQL block, clearly marked for manual conversion
        plsql_block = match.group(0).strip()
        final_output.append(
            f"-- [Gemini] The following PL/SQL block requires manual conversion.\n"
            f"-- Business logic, variables, loops, and transaction control must be rewritten.\n"
            f"/*\n{plsql_block}\n*/"
        )
        last_end = end

    # Add any remaining standard SQL that was after the last PL/SQL block
    remaining_sql = oracle_sql[last_end:].strip()
    if remaining_sql:
        try:
            transpiled_sql = sqlglot.transpile(remaining_sql, read='oracle', write='trino', pretty=True)
            final_output.extend(transpiled_sql)
        except Exception as e:
            final_output.append(f"-- [Gemini] Could not transpile a SQL block, please review manually: {e}\n/*\n{remaining_sql}\n*/")

    return "\n\n-- STATEMENT SEPARATOR --\n\n".join(final_output)

def main():
    """
    Main function to read Oracle SQL, transpile it, and write to a file.
    """
    try:
        with open("D:/google_cli/sql_comparator/oracle.txt", "r", encoding="utf-8") as f:
            oracle_sql = f.read()

        trino_sql = transpile_oracle_to_trino(oracle_sql)

        with open("D:/google_cli/sql_comparator/trino.txt", "w", encoding="utf-8") as f:
            f.write(trino_sql)

        print("Transpilation from Oracle to Trino completed successfully.")
        print("Output written to D:/google_cli/sql_comparator/trino.txt")

    except FileNotFoundError:
        print("Error: oracle.txt not found. Please make sure the file exists.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
