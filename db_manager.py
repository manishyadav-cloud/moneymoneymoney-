import duckdb
import os
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.duckdb")

def get_connection():
    return duckdb.connect(DB_PATH)

def load_csv(csv_path, table_name=None):
    """Load a CSV file into the database as a table."""
    if not os.path.exists(csv_path):
        print(f"Error: File not found: {csv_path}")
        return

    if table_name is None:
        table_name = os.path.splitext(os.path.basename(csv_path))[0]
        # Clean table name: replace spaces/special chars with underscores
        table_name = "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)

    con = get_connection()
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE "{table_name}" AS
            SELECT * FROM read_csv_auto('{csv_path.replace("'", "''")}')
        """)
        row_count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        col_count = len(con.execute(f'SELECT * FROM "{table_name}" LIMIT 0').description)
        print(f"Loaded '{csv_path}' -> table '{table_name}' ({row_count:,} rows, {col_count} columns)")
    finally:
        con.close()

def load_all_csvs(folder=None):
    """Load all CSV files from a folder into the database."""
    if folder is None:
        folder = os.path.dirname(os.path.abspath(__file__))

    csv_files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not csv_files:
        print(f"No CSV files found in {folder}")
        return

    print(f"Found {len(csv_files)} CSV file(s):")
    for f in csv_files:
        load_csv(os.path.join(folder, f))

def list_tables():
    """List all tables in the database."""
    con = get_connection()
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        if not tables:
            print("No tables in database yet.")
            return
        print(f"\nTables in database ({DB_PATH}):")
        for (name,) in tables:
            row_count = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            cols = con.execute(f'DESCRIBE "{name}"').fetchall()
            print(f"\n  {name} ({row_count:,} rows)")
            for col_name, col_type, *_ in cols:
                print(f"    - {col_name}: {col_type}")
    finally:
        con.close()

def query(sql):
    """Run a SQL query and print results."""
    con = get_connection()
    try:
        result = con.execute(sql)
        if result.description:
            print(result.df().to_string())
        else:
            print("Query executed successfully.")
    finally:
        con.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python db_manager.py load <file.csv> [table_name]  - Load a CSV")
        print("  python db_manager.py loadall [folder]              - Load all CSVs from folder")
        print("  python db_manager.py tables                        - List all tables")
        print("  python db_manager.py query 'SELECT ...'            - Run a SQL query")
        sys.exit(0)

    cmd = sys.argv[1].lower()

    if cmd == "load" and len(sys.argv) >= 3:
        table_name = sys.argv[3] if len(sys.argv) > 3 else None
        load_csv(sys.argv[2], table_name)
    elif cmd == "loadall":
        folder = sys.argv[2] if len(sys.argv) > 2 else None
        load_all_csvs(folder)
    elif cmd == "tables":
        list_tables()
    elif cmd == "query" and len(sys.argv) >= 3:
        query(sys.argv[2])
    else:
        print("Unknown command. Run without arguments for usage.")
