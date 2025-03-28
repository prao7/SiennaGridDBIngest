import sqlite3
import pandas as pd
import numpy as np
import SiennaGridDBIngest.src.functions.functions_RTS_custom as functions_RTS_custom
import SiennaGridDBIngest.src.functions.functions_handlers as functions_handlers
import SiennaGridDBIngest.src.functions.functions_5bus_custom as functions_5bus_custom


def ingest_RTS(db_path, RTS_path, schema_path):
    """
    Ingests the RTS data into the database.
    """

    # Connect to the database
    conn = functions_handlers.create_db(db_path)

    # Clear the database
    functions_handlers.clear_database(conn)

    # Apply the schema to the database
    functions_handlers.apply_schema(conn, schema_path)

    # Get the structure associated with the RTS data
    RTS_structure = functions_handlers.get_directory_structure(RTS_path)

    # Ingest the RTS data into the database
    functions_RTS_custom.process_and_ingest_RTS_data(conn, RTS_structure)

    # Close the connection
    conn.close()

def ingest_5bus(db_path, five_bus_path, schema_path):
    """
    Ingests the 5 bus data into the database.
    """
    # Connect to the database
    conn = functions_handlers.create_db(db_path)

    # Clear the database
    functions_handlers.clear_database(conn)

    # Apply the schema to the database
    functions_handlers.apply_schema(conn, schema_path)

    five_bus_structure = functions_handlers.get_directory_structure(five_bus_path)

    # Ingest the 5 bus data into the database
    functions_5bus_custom.ingest_5bus_data(conn, five_bus_structure)
    
    # Close the connection
    conn.close()


def main():
    """
    Main code logic goes here. Please input the paths to the 5 bus and RTS directories, as well as the database paths.
    """
    RTS_db_path = '/path/to/database.db'
    five_bus_db_path = '/path/to/5bus_database.db'
    RTS_directory_path = '/path/to/RTS'
    five_bus_directory_path = '/path/to/5bus'
    schema_path = '/path/to/schema'
    
    ingest_RTS(RTS_db_path, RTS_directory_path, schema_path)
    ingest_5bus(five_bus_db_path, five_bus_directory_path, schema_path)


if __name__ == '__main__':
    main()