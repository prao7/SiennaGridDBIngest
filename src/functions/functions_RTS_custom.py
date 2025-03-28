import sqlite3
import pandas as pd # type: ignore
import json
import os
import h5py
import numpy as np
import SiennaGridDBIngest.src.functions.functions_handlers as functions_handlers
import SiennaGridDBIngest.src.functions.functions_schema_ingest as functions_schema_ingest


def insert_generation_RTS(conn, directory_structure):
    pass


def insert_buses_RTS(conn, directory_structure):
    pass


def insert_branches_RTS(conn, directory_structure):
    pass


def insert_loads_RTS(conn, directory_structure):
    pass


def insert_investment_options_RTS(conn, directory_structure):
    pass
 

def process_and_ingest_RTS_data(conn, directory_structure):
    """
    Process and ingest RTS data into the database.
    """

    # First, insert the buses and branches
    insert_buses_RTS(conn, directory_structure)

    insert_branches_RTS(conn, directory_structure)

    # Second, insert the generation data
    insert_generation_RTS(conn, directory_structure)

    # Third, insert the load data
    insert_loads_RTS(conn, directory_structure)

    # Finally, insert the investment options
    insert_investment_options_RTS(conn, directory_structure)