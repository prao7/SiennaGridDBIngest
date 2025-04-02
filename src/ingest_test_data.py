import sqlite3
import pandas as pd
import numpy as np
import functions_RTS_custom as functions_RTS_custom
import functions_handlers as functions_handlers
import functions_5bus_custom as functions_5bus_custom


def ingest_RTS(db_path, RTS_path, sql_paths):
    """
    Ingests the RTS data into the database.
    """

    # Connect to the database
    conn = functions_handlers.add_sql_files_to_database(db_path, sql_paths)

    # Get the structure associated with the RTS data
    RTS_structure = functions_handlers.get_directory_structure(RTS_path)

    # Ingest the RTS data into the database
    functions_RTS_custom.process_and_ingest_RTS_data(conn, RTS_structure)

    # Close the connection
    functions_handlers.close_connection(conn)


def ingest_5bus(db_path, five_bus_path, sql_paths):
    """
    Ingests the 5 bus data into the database.
    """

    # Connect to the database
    conn = functions_handlers.add_sql_files_to_database(db_path, sql_paths)

    five_bus_structure = functions_handlers.get_directory_structure(five_bus_path)

    # Ingest the 5 bus data into the database
    functions_5bus_custom.ingest_5bus_data(conn, five_bus_structure)
    
    # Close the connection
    functions_handlers.close_connection(conn)


def main():
    """
    Main code logic goes here. Please input the paths to the 5 bus and RTS directories, as well as the database paths.
    """
    RTS_db_path = 'SiennaGridDBIngest/output_db/RTS.db'
    five_bus_db_path = 'SiennaGridDBIngest/output_db/5bus_database.db'
    RTS_directory_path = '/Users/prao/GitHub_Repos/data_interoperability/PowerSystemsInvestmentsPortfoliosTestData/RTS_inputs'
    five_bus_directory_path = '/Users/prao/GitHub_Repos/data_interoperability/PowerSystemsTestData/5-Bus'
    schema_path = '/Users/prao/GitHub_Repos/data_interoperability/SiennaGridDB/schema.sql'
    sql_paths = ["/Users/prao/GitHub_Repos/data_interoperability/SiennaGridDB/schema.sql", 
                 "/Users/prao/GitHub_Repos/data_interoperability/SiennaGridDB/triggers.sql", 
                 "/Users/prao/GitHub_Repos/data_interoperability/SiennaGridDB/views.sql"]
    
    ingest_RTS(RTS_db_path, RTS_directory_path, sql_paths)
    ingest_5bus(five_bus_db_path, five_bus_directory_path, sql_paths)


if __name__ == '__main__':
    main()