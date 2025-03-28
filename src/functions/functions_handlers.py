import os
import pprint
import sqlite3

def get_directory_structure(directory_path):
    """
    Returns a nested dictionary that represents the folder structure of directory_path.
    Directories are keys and their values are dictionaries of their contents.
    Files are stored in a list under the key '__files__' with their full paths.
    """

    structure = {}
    # Get a sorted list of all items in the current directory
    for item in sorted(os.listdir(directory_path)):
        item_path = os.path.join(directory_path, item)
        if os.path.isdir(item_path):
            structure[item] = get_directory_structure(item_path)
        else:
            structure.setdefault('__files__', []).append(item_path)  # Append full file path
    return structure


def find_filepath(structure, filename):
    """
    Recursively search the directory structure for the given filename.
    
    Parameters:
        structure (dict): The nested directory structure from get_directory_structure.
        filename (str): The filename to search for.
        
    Returns:
        str or None: The full filepath if found, otherwise None.
    """
    # Check if the current level has files.
    if '__files__' in structure:
        for filepath in structure['__files__']:
            if os.path.basename(filepath) == filename:
                return filepath

    # Recursively search in subdirectories.
    for key, substructure in structure.items():
        if key == '__files__':
            continue
        result = find_filepath(substructure, filename)
        if result is not None:
            return result

    return None

def print_directory_structure(directory_path):
    """
    Print the directory structure of directory_path.
    """
    pprint.pprint(get_directory_structure(directory_path))


def print_all_tables(cursor):
    """
    Print all tables in the database.
    """

    # Fetch all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        print(f"Table: {table_name}")
        
        # Fetch all rows from the table
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()

        # Fetch column names
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]

        # Print column names
        print(", ".join(column_names))

        # Print each row
        for row in rows:
            print(row)
        print("\n")


def clear_database(conn):
    """
    Clear all tables in the database.
    """

    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF;")  # Temporarily disable foreign key constraints
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table_name in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name[0]};")
    conn.commit()
    cursor.execute("PRAGMA foreign_keys = ON;")  # Re-enable foreign key constraints
    conn.close()


def apply_schema(conn, schema_path):
    """
    Apply the schema in schema_path to the SQLite database at db_path.
    """
    # Connect to the SQLite database
    cursor = conn.cursor()
    
    # Read the schema.sql file
    with open(schema_path, 'r') as file:
        schema_sql = file.read()
    
    # Execute the schema script
    cursor.executescript(schema_sql)
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    print("Schema applied successfully.")


def create_db(db_path):
    """
    Creates a database from the db_path
    """

    return sqlite3.connect(db_path)
