import sqlite3
import pandas as pd
import json
import os
import h5py
import numpy as np
import functions_handlers as functions_handlers
import functions_schema_ingest as functions_schema_ingest


def get_participation_factor_5bus(bus_df):
    area_load_sum = bus_df.groupby('Area')['MW Load'].sum()

    # Append a column to the end of the dataframe called participation factor that is row['MW Load'] / area_load_sum  
    bus_df['Participation Factor'] = bus_df.apply(lambda row: row['MW Load'] / area_load_sum[row['Area']], axis=1)
    
    # Return the dataframe
    return bus_df


def insert_buses_5bus(conn, directory_structure):
    """
    Insert the bus data into the database.
    """
    # Get the bus data from the directory structure
    bus_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'bus.csv'))

    # Get the participation factor
    bus_data_df = get_participation_factor_5bus(bus_data_df)

    for _, row in bus_data_df.iterrows():
        # Insert the data into the database
        bus_id = functions_schema_ingest.insert_balancing_topologies(
                            conn,
                            row['Bus ID'],
                            row['Bus Name'],
                            f"Region {row['Area']}",  # using the same format as planning regions
                            row['Bus Type']
                        )
        
        # Insert the participation factor as an attribute
        if bus_id is not None:
            # Get the entity ID for the bus
            bus_entity_id = functions_schema_ingest.get_entity_id(conn, 'balancing_topologies', bus_id)

            # Insert the participation factor as an attribute
            participation_factor_at_id = functions_schema_ingest.insert_attributes(
                                            conn,
                                            'balancing_topologies',
                                            "Participation Factor",
                                            row['Participation Factor'])
            
            # Insert relationship between participation factor and attribute_association
            functions_schema_ingest.insert_attributes_associations(conn, participation_factor_at_id, bus_entity_id)


def insert_regions_5bus(conn, directory_structure):
    """
    Insert the region data into the database.
    """
    # Get the bus data from the directory structure
    bus_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'bus.csv'))

    # Get the unique regions
    regions = bus_data_df['Area'].unique()

    # Insert the regions into the database
    for region in regions:
        # Insert the data into the database
        functions_schema_ingest.insert_planning_regions(conn, int(region), f"Region {region}")


def process_interchange_data_5bus(branch_df, bus_df):
    """
    Process the interchange data and return a DataFrame.
    """
    # Get the bus area mapping
    bus_area_mapping = dict(zip(bus_df['Bus ID'], bus_df['Area']))

    # Define a list to hold rows with buses in different areas
    filtered_rows = []

    # Iterate through the rows in branch_df
    for index, row in branch_df.iterrows():
        bus_1 = row['From Bus']
        bus_2 = row['To Bus']
        
        # Check if the buses belong to different areas
        if bus_area_mapping[bus_1] != bus_area_mapping[bus_2]:
            # Append the row to the list
            filtered_rows.append(row)

    # Create a new DataFrame from the filtered rows
    filtered_branch_df = pd.DataFrame(filtered_rows, columns=branch_df.columns)

    # Reset the index of the new DataFrame
    filtered_branch_df.reset_index(drop=True, inplace=True)

    # Map the area to the filtered_branch_df
    filtered_branch_df['From Area'] = filtered_branch_df['From Bus'].map(bus_area_mapping)
    filtered_branch_df['To Area'] = filtered_branch_df['To Bus'].map(bus_area_mapping)

    # Group by 'From Area' and 'To Area' and sum the 'Cont Rating'
    grouped_df = filtered_branch_df.groupby(['From Area', 'To Area'])['Cont Rating'].sum().reset_index()

    # Rename the column for clarity
    grouped_df.rename(columns={'Cont Rating': 'Total Cont Rating'}, inplace=True)

    # Reset the index of grouped_df so that the index becomes a column
    grouped_df = grouped_df.reset_index(drop=False)

    # Create a new column named 'group_index' holding the new index
    grouped_df['group_index'] = grouped_df.index

    # Merge the group index into filtered_branch_df based on the 'From Area' and 'To Area' columns
    filtered_branch_df = filtered_branch_df.merge(
        grouped_df[['From Area', 'To Area', 'group_index']],
        on=['From Area', 'To Area'],
        how='left'
    )

    return grouped_df, filtered_branch_df


def get_transmission_line_5bus(conn, from_bus_id, to_bus_id):
    """
    Get the transmission line ID between two buses.
    """
    # Get the entity id for the buses
    from_bus_entity_id = functions_schema_ingest.get_entity_id(conn, 'balancing_topologies', from_bus_id)
    to_bus_entity_id = functions_schema_ingest.get_entity_id(conn, 'balancing_topologies', to_bus_id)

    # Get the arc ID
    arc_id = functions_schema_ingest.get_arc_id(conn, from_bus_entity_id, to_bus_entity_id)

    # Get the transmission line ID
    transmission_line_id = functions_schema_ingest.get_transmission_id_from_arc_id(conn, arc_id)

    return transmission_line_id


def insert_interchanges_5bus(conn, directory_structure):
    """
    Insert the interchanges data into the database.
    """
    # We have to process the interchanges data from the branch.csv file
    # Get the branch data from the directory structure
    branch_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'branch.csv'))

    # Get the bus data from the directory structure
    bus_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'bus.csv'))

    # Process the interchange data
    interchange_df, _ = process_interchange_data_5bus(branch_data_df, bus_data_df)

    # Add the unique interchange data to the database
    for _, row in interchange_df.iterrows():
        # Create the arc relationship between each planning region
        from_region_id = functions_schema_ingest.get_entity_id(conn, 'planning_regions', row['From Area'])
        to_region_id = functions_schema_ingest.get_entity_id(conn, 'planning_regions', row['To Area'])

        # Add the arcs to the database
        if from_region_id is not None and to_region_id is not None:
            arcs_id = functions_schema_ingest.insert_arcs(
                            conn,
                            from_region_id,
                            to_region_id)
            
            # Then, insert the transmission interchange
            if arcs_id is not None:
                # Insert the data into the database
                functions_schema_ingest.insert_transmission_interchange(
                                conn,
                                arcs_id,
                                f"{row['From Area']}_{row['To Area']}",
                                float(row['Total Cont Rating']),
                                float(row['Total Cont Rating'])
                            )
                

def insert_branches_5bus(conn, directory_structure):
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
                # TODO: Check how to get the continuous rating, ste rating and lte rating
                # For now, we will use the same value for all three
                transmission_id = functions_schema_ingest.insert_transmission_lines(
                                    conn,
                                    row['Cont Rating'],
                                    row['Cont Rating'],
                                    row['Cont Rating'],
                                    1.0,
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


def convert_to_timestamp_5bus(df, datetime_col):
    """
    Convert the year, month, day and period columns to a timestamp and insert it 
    as the first column of the DataFrame.
    
    Args:
    df (pd.DataFrame): The input DataFrame.
    datetime_col (str): Column name for the date/time.
    
    Returns:
        pd.DataFrame: The DataFrame with the datetime column converted to Python datetime objects.
    """

    # First ensure the column is in a pandas datetime (Timestamp) format
    df[datetime_col] = pd.to_datetime(df[datetime_col])

    # Convert each pandas Timestamp to a Python datetime using to_pydatetime()
    df[datetime_col] = df[datetime_col].apply(lambda x: x.to_pydatetime())

    return df


def extract_loads_5bus(df):
    """
    Given a DataFrame, return a new DataFrame containing all columns after the 'Period' column.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
    
    Returns:
        pd.DataFrame: A DataFrame with the columns after 'Period'.
    """
    # Get the index of the "Period" column
    period_index = df.columns.get_loc('values')
    # Return all columns after the "Period" column
    return df.iloc[:, period_index + 1:]
    

def insert_day_ahead_loads_5bus(conn, da_load_data_df):
    """
    Insert the day ahead load data into the database.
    """
    # Convert the year, month, day and period columns to a timestamp
    da_load_data_df = convert_to_timestamp_5bus(da_load_data_df, 'DateTime')
    
    # Extract the load data
    da_load_data_only_df = extract_loads_5bus(da_load_data_df)

    # Convert the initial timestamp to an ISO-formatted string
    initial_ts = da_load_data_df['DateTime'].iloc[0]
    if isinstance(initial_ts, pd.Timestamp):
        initial_ts = initial_ts.isoformat()
    
    # Calculate the length (cast to int if needed)
    length_val = int((da_load_data_df['DateTime'].max() - da_load_data_df['DateTime'].min()).total_seconds() / 3600)

    for col in da_load_data_only_df.columns:
        # Now call insert_time_series supplying all needed parameters
        da_time_series_id = functions_schema_ingest.insert_time_series(
            conn,
            'deterministic_forecast_time_series',           # time_series_type
            'DA Load',                                      # name
            initial_ts,                                     # initial_timestamp as a string
            3600,                                           # resolution_ms
            1,                                              # horizon
            1,                                              # interval
            length_val,                                     # length
            None,                                           # uuid (or provide a string if needed)
            '{"unit": "MW"}',                               # features
            None                                            # metadata (or provide JSON if needed)
        )

        # Get the entity ID for the region from the column header
        bus_id = functions_schema_ingest.get_bus_from_name(conn, col)

        if bus_id is None:
            raise ValueError(f"Bus ID for {col} not found in the database.")
        
        # Get the region ID from the bus ID
        region_id = functions_schema_ingest.get_entity_id(conn, 'balancing_topologies', bus_id)

        # Insert into time series associations
        functions_schema_ingest.insert_time_series_associations(conn, da_time_series_id, region_id)

        # Insert the data into the static time series
        for i, value in enumerate(da_load_data_only_df[col]):
            # Current timestamp is
            current_ts = da_load_data_df['DateTime'].iloc[i]

            # Convert the timestamp to an ISO-formatted string
            if isinstance(current_ts, pd.Timestamp):
                current_ts = current_ts.isoformat()
            
            # Insert the data into the database
            functions_schema_ingest.insert_deterministic_time_series(
                conn,
                da_time_series_id,
                current_ts,
                float(value)
            )


def insert_real_time_loads_5bus(conn, rt_load_data_df):
    """
    Insert the real time load data into the database.
    """

    # Convert the year, month, day and period columns to a timestamp
    rt_load_data_df = convert_to_timestamp_5bus(rt_load_data_df, 'DateTime')

    # Extract the load data
    rt_load_data_only_df = extract_loads_5bus(rt_load_data_df)

    # Convert the initial timestamp to an ISO-formatted string
    initial_ts = rt_load_data_df['DateTime'].iloc[0]
    if isinstance(initial_ts, pd.Timestamp):
        initial_ts = initial_ts.isoformat()
    
    # Calculate the length (cast to int if needed)
    length_val = len(rt_load_data_df["DateTime"])

    for col in rt_load_data_only_df.columns:
        rt_time_series_id = functions_schema_ingest.insert_time_series(
            conn,
            'static_time_series',         # time_series_type
            'RT Load',                    # name
            initial_ts,                   # initial_timestamp as a string
            300,                          # resolution_ms
            1,                            # horizon
            1,                            # interval
            length_val,                   # length
            None,                         # uuid (or provide a string if needed)
            '{"unit": "MW"}',             # features
            None                          # metadata (or provide JSON if needed)
        )

        # Get the entity ID for the region from the column header
        bus_id = functions_schema_ingest.get_bus_from_name(conn, col)

        if bus_id is None:
            raise ValueError(f"Bus ID for {col} not found in the database.")
        
        # Get the region ID from the bus ID
        region_id = functions_schema_ingest.get_entity_id(conn, 'balancing_topologies', bus_id)

        # Insert into time series associations
        functions_schema_ingest.insert_time_series_associations(conn, rt_time_series_id, region_id)

        # Insert the data into the static time series
        for i, value in enumerate(rt_load_data_only_df[col]):
            # Current timestamp is
            current_ts = rt_load_data_df['DateTime'].iloc[i]

            # Convert the timestamp to an ISO-formatted string
            if isinstance(current_ts, pd.Timestamp):
                current_ts = current_ts.isoformat()
            
            # Insert the data into the database
            functions_schema_ingest.insert_static_time_series(
                conn,
                rt_time_series_id,
                current_ts,
                float(value)
            )
    

def insert_loads_5bus(conn, directory_structure):
    """
    Insert the load data into the database.
    """
    # Get the DA load data from the directory structure
    da_load_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'da_load.csv'))

    # Get the RT load data from the directory structure
    rt_load_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'rt_load.csv'))

    # Insert the DA load data into the database
    insert_day_ahead_loads_5bus(conn, da_load_data_df)

    # Insert the RT load data into the database
    insert_real_time_loads_5bus(conn, rt_load_data_df)


def insert_generation_5bus(conn, directory_structure):
    pass


def insert_investment_options_5bus(conn, directory_structure):
    pass


def ingest_5bus_data(conn, directory_structure):
    """
    This is the main function on ingesting the 5 bus data into the database.
    It will call all the other functions in the correct order.
    """

    # First, insert the buses and regions
    insert_regions_5bus(conn, directory_structure)
    insert_buses_5bus(conn, directory_structure)

    # Second, insert the branches and interchanges
    insert_branches_5bus(conn, directory_structure)
    insert_interchanges_5bus(conn, directory_structure)

    # Third, insert the generation data
    insert_generation_5bus(conn, directory_structure)

    # Fourth, insert the load data
    # insert_loads_5bus(conn, directory_structure)

    # Finally, insert the investment options
    insert_investment_options_5bus(conn, directory_structure)

