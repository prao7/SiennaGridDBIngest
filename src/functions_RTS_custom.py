import sqlite3
import pandas as pd # type: ignore
import json
import os
import h5py
import numpy as np
import functions_handlers as functions_handlers
import functions_schema_ingest as functions_schema_ingest


def insert_generation_RTS(conn, directory_structure):
    # Put the capacity data in a dataframe
    capacity_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'gen.csv'))


def get_participation_factor_RTS(bus_df):
    area_load_sum = bus_df.groupby('Area')['MW_Load'].sum()

    # Append a column to the end of the dataframe called participation factor that is row['MW Load'] / area_load_sum  
    bus_df['Participation Factor'] = bus_df.apply(lambda row: row['MW_Load'] / area_load_sum[row['Area']], axis=1)
    
    # Return the dataframe
    return bus_df


def insert_buses_RTS(conn, directory_structure):
    """
    Insert the bus data into the database.
    """
    # Get the bus data from the directory structure
    bus_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'bus_mod_updatedwithBA.csv'))

    # Get the participation factor
    bus_data_df = get_participation_factor_RTS(bus_data_df)

    for _, row in bus_data_df.iterrows():
        # Insert the data into the database
        functions_schema_ingest.insert_balancing_topologies(
                            conn,
                            row['Bus_ID'],
                            row['Bus_Name'],
                            f"Region {row['Area']}",  # using the same format as planning regions
                            row['Participation Factor'],
                            row['Bus_Type']
                        )

        # Insert lat long data into supplemental attributes
        functions_schema_ingest.insert_supplemental_attributes(conn, 'geolocation', json.dumps({'lat': row['lat'], 'lon': row['lng']}))


def insert_regions_RTS(conn, directory_structure):
    """
    Insert the region data into the database.
    """
    # Get the bus data from the directory structure
    bus_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'bus_mod_updatedwithBA.csv'))

    # Get the unique regions
    regions = bus_data_df['Area'].unique()

    # Insert the regions into the database
    for region in regions:
        # Insert the data into the database
        functions_schema_ingest.insert_planning_regions(conn, int(region), f"Region {region}")





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

    # First, insert the buses and regions
    insert_regions_RTS(conn, directory_structure)
    insert_buses_RTS(conn, directory_structure)

    # Second, insert the branches
    insert_branches_RTS(conn, directory_structure)

    # Third, insert the generation data
    insert_generation_RTS(conn, directory_structure)

    # Fourth, insert the load data
    insert_loads_RTS(conn, directory_structure)

    # Finally, insert the investment options
    insert_investment_options_RTS(conn, directory_structure)