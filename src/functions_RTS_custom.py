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
        bus_id = functions_schema_ingest.insert_balancing_topologies(
                            conn,
                            row['Bus_ID'],
                            row['Bus_Name'],
                            f"Region {row['Area']}",  # using the same format as planning regions
                            row['Participation Factor'],
                            row['Bus_Type']
                        )
        
        bus_entity_id = functions_schema_ingest.insert_entities(conn, 'balancing_topologies', bus_id)

        # Insert lat long data into supplemental attributes
        sup_at_id = functions_schema_ingest.insert_supplemental_attributes(conn, 'geolocation', json.dumps({'lat': row['lat'], 'lon': row['lng']}))

        # Insert the association into the supplemental_attributes_association table
        functions_schema_ingest.insert_supplemental_attributes_association(conn, sup_at_id, bus_entity_id)



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


def insert_interchanges_RTS(conn, directory_structure):
    pass


def insert_branches_RTS(conn, directory_structure):
    """
    Insert the transmission data into the database.
    """
    # Get the branch data from the directory structure
    branch_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'branch.csv'))

    for _, row in branch_data_df.iterrows():
        # First, create the physical arcs relationship
        from_bus_id = functions_schema_ingest.get_entity_id(conn, 'balancing_topologies', row['From Bus'])
        to_bus_id = functions_schema_ingest.get_entity_id(conn, 'balancing_topologies', row['To Bus'])

        # Add the arcs to the database
        if from_bus_id is not None and to_bus_id is not None:
            arc_id = functions_schema_ingest.insert_arcs(
                            conn,
                            from_bus_id,
                            to_bus_id)

            # Then, insert the transmission lines
            if arc_id is not None:
                # Insert the data into the database
                transmission_id = functions_schema_ingest.insert_transmission_lines(
                                    conn,
                                    row['Cont Rating'],
                                    row['STE Rating'],
                                    row['LTE Rating'],
                                    row['Length'],
                                    arc_id
                                )
                
                # Get the entity ID for the transmission line
                trans_entity_id = functions_schema_ingest.get_entity_id(conn, 'transmission_lines', transmission_id)
                
                # Insert the X, R and B data as attributes
                x_at_id = functions_schema_ingest.insert_attributes(
                                    conn,
                                    'transmission_lines',
                                    "Reactance",
                                    row['X'])

                # Insert relationship between x and attribute_association
                functions_schema_ingest.insert_attributes_associations(conn, x_at_id, trans_entity_id)

                # Insert the B data as attributes
                b_at_id = functions_schema_ingest.insert_attributes(
                                    conn,
                                    'transmission_lines',
                                    "Susceptance",
                                    row['B'])
                
                # Insert relationship between b and attribute_association
                functions_schema_ingest.insert_attributes_associations(conn, b_at_id, trans_entity_id)

                # Insert the R data as attributes
                r_at_id = functions_schema_ingest.insert_attributes(
                                    conn,
                                    'transmission_lines',
                                    "Resistance",
                                    row['R'])
                
                # Insert relationship between r and attribute_association
                functions_schema_ingest.insert_attributes_associations(conn, r_at_id, trans_entity_id)


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