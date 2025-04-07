import sqlite3
import pandas as pd # type: ignore
import json
import os
import h5py
import numpy as np
import functions_handlers as functions_handlers
import functions_schema_ingest as functions_schema_ingest


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
                            row['Bus_Type']
                        )
        
        bus_entity_id = functions_schema_ingest.insert_entities(conn, 'balancing_topologies', bus_id)

        # Insert lat long data into supplemental attributes
        sup_at_id = functions_schema_ingest.insert_supplemental_attributes(conn, 'geolocation', json.dumps({'lat': row['lat'], 'lon': row['lng']}))

        # Insert the association into the supplemental_attributes_association table
        functions_schema_ingest.insert_supplemental_attributes_association(conn, sup_at_id, bus_entity_id)

        # Insert the participation factor as an attribute
        participation_factor_at_id = functions_schema_ingest.insert_attributes(
                                            conn,
                                            'balancing_topologies',
                                            "Participation Factor",
                                            row['Participation Factor'])
        
        # Insert relationship between participation factor and attribute_association
        functions_schema_ingest.insert_attributes_associations(conn, participation_factor_at_id, bus_entity_id)            



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


def process_interchange_data_RTS(branch_df, bus_df):
    """
    Process the interchange data and return a DataFrame.
    """
    # Get the bus area mapping
    bus_area_mapping = dict(zip(bus_df['Bus_ID'], bus_df['Area']))

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


def get_transmission_line_RTS(conn, from_bus_id, to_bus_id):
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


def insert_interchanges_RTS(conn, directory_structure):
    """
    Insert the interchanges data into the database.
    """
    # We have to process the interchanges data from the branch.csv file
    # Get the branch data from the directory structure
    branch_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'branch.csv'))

    # Get the bus data from the directory structure
    bus_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'bus_mod_updatedwithBA.csv'))

    # Process the interchange data
    interchange_df, _ = process_interchange_data_RTS(branch_data_df, bus_data_df)

    # Add the unique interchange data to the database
    for _, row in interchange_df.iterrows():
        # Create the arc relationship between each planning region
        from_region_id = functions_schema_ingest.get_entity_id(conn, 'planning_regions', int(row['From Area']))
        to_region_id = functions_schema_ingest.get_entity_id(conn, 'planning_regions', int(row['To Area']))

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


def convert_to_timestamp_RTS(df, year_col, month_col, day_col, period_col, resolution_minutes=60):
    """
    Convert the year, month, day and period columns to a timestamp and insert it 
    as the first column of the DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        year_col (str): Column name for the year.
        month_col (str): Column name for the month.
        day_col (str): Column name for the day.
        period_col (str): Column name for the period.
        resolution_minutes (int): The resolution in minutes. Default is 60 (hourly).
                                   For 5 minute resolution, set resolution_minutes=5.
    
    Returns:
        pd.DataFrame: The DataFrame with a new 'Timestamp' column inserted at index 0.
    """
    # Multiply the period offset by the resolution (in minutes)
    ts = pd.to_datetime(df[[year_col, month_col, day_col]]) + pd.to_timedelta((df[period_col] - 1) * resolution_minutes, unit='m')
    df.insert(0, 'Timestamp', ts)
    return df


def extract_loads_RTS(df):
    """
    Given a DataFrame, return a new DataFrame containing all columns after the 'Period' column.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
    
    Returns:
        pd.DataFrame: A DataFrame with the columns after 'Period'.
    """
    # Get the index of the "Period" column
    period_index = df.columns.get_loc('Period')
    # Return all columns after the "Period" column
    return df.iloc[:, period_index + 1:]
    

def insert_day_ahead_loads_RTS(conn, da_load_data_df):
    """
    Insert the day ahead load data into the database.
    """
    # Convert the year, month, day and period columns to a timestamp
    da_load_data_df = convert_to_timestamp_RTS(da_load_data_df, 'Year', 'Month', 'Day', 'Period')
    
    # Extract the load data
    da_load_data_only_df = extract_loads_RTS(da_load_data_df)

    # Convert the initial timestamp to an ISO-formatted string
    initial_ts = da_load_data_df['Timestamp'].iloc[0]
    if isinstance(initial_ts, pd.Timestamp):
        initial_ts = initial_ts.isoformat()
    
    # Calculate the length (cast to int if needed)
    length_val = int((da_load_data_df['Timestamp'].max() - da_load_data_df['Timestamp'].min()).total_seconds() / 3600)

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
        region_id = functions_schema_ingest.get_entity_id(conn, 'planning_regions', int(col))

        # Insert into time series associations
        functions_schema_ingest.insert_time_series_associations(conn, da_time_series_id, region_id)

        # Insert the data into the static time series
        for i, value in enumerate(da_load_data_only_df[col]):
            # Current timestamp is
            current_ts = da_load_data_df['Timestamp'].iloc[i]

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


def insert_real_time_loads_RTS(conn, rt_load_data_df):
    """
    Insert the real time load data into the database.
    """

    # Convert the year, month, day and period columns to a timestamp
    rt_load_data_df = convert_to_timestamp_RTS(rt_load_data_df, 'Year', 'Month', 'Day', 'Period', resolution_minutes=5)

    # Extract the load data
    rt_load_data_only_df = extract_loads_RTS(rt_load_data_df)

    # Convert the initial timestamp to an ISO-formatted string
    initial_ts = rt_load_data_df['Timestamp'].iloc[0]
    if isinstance(initial_ts, pd.Timestamp):
        initial_ts = initial_ts.isoformat()
    
    # Calculate the length (cast to int if needed)
    length_val = len(rt_load_data_df["Period"])

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
        region_id = functions_schema_ingest.get_entity_id(conn, 'planning_regions', int(col))

        # Insert into time series associations
        functions_schema_ingest.insert_time_series_associations(conn, rt_time_series_id, region_id)

        # Insert the data into the static time series
        for i, value in enumerate(rt_load_data_only_df[col]):
            # Current timestamp is
            current_ts = rt_load_data_df['Timestamp'].iloc[i]

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
    

def insert_loads_RTS(conn, directory_structure):
    """
    Insert the load data into the database.
    """

    # Get the DA load data from the directory structure
    da_load_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_regional_Load.csv'))

    # Get the RT load data from the directory structure
    rt_load_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_regional_Load.csv'))

    # Add the day ahead loads to the database
    insert_day_ahead_loads_RTS(conn, da_load_data_df)

    # Add the real time loads to the database
    insert_real_time_loads_RTS(conn, rt_load_data_df)

def get_fom(fuel):
    """
    Get the fixed O&M cost for the given fuel type.
    Values taken from ATB data, ATB Advanced
    """
    fom_dict = {
        "Hydro": 92.0,
        "Solar": 16.639,
        "Wind": 26.314,
        "Nuclear": 126.0,
        "NG": 38.0,
        "Coal": 123.3,
        "Storage": 0.0,
        "Oil": 25.0,
    }
    # Return the fixed O&M cost based on the fuel type
    return fom_dict.get(fuel, 0.0)

def get_vom(fuel):
    """
    Get the variable O&M cost for the given fuel type.
    Values taken from ATB data, ATB Advanced
    """
    vom_dict = {
        "Hydro": 0.0,
        "Solar": 0.0,
        "Wind": 0.0,
        "Nuclear": 1.9,
        "NG": 2.08,
        "Coal": 14.2,
        "Storage": 0.0,
        "Oil": 6.94,
    }
    # Return the variable O&M cost based on the fuel type
    return vom_dict.get(fuel, 0.0)


def insert_fuels_RTS(conn, capacity_df):
    """
    Insert the fuel data into the database.
    """

    # Get the unique fuels
    fuels = capacity_df['Fuel'].unique()

    # Insert the fuels into the database
    for fuel in fuels:
        # Insert the data into the database
        functions_schema_ingest.insert_fuel(conn, fuel)


def get_prime_mover(prime_mover):
    """
    This method is hard coded so far. We need to figure out better enforcements on what is allowed
    and an easier transition to Sienna PrimeMovers
    """
    # Create a dictionary mapping the Unit Type to the Prime Mover
    prime_mover_mapping = {
        'PV': 'PrimeMovers.PVe',
        'STEAM': 'PrimeMovers.ST',
        'CT': 'PrimeMovers.CT',
        'CC': 'PrimeMovers.CC',
        'RTPV': 'PrimeMovers.PVe',
        'ROR': 'PrimeMovers.HY',
        'HYDRO': 'PrimeMovers.HY',
        'STORAGE': 'PrimeMovers.BA',
        'NUCLEAR': 'PrimeMovers.ST',
        'WIND': 'PrimeMovers.WT',
        'CSP': 'PrimeMovers.PVe',
    }

    # Return the prime mover based on the mapping
    return prime_mover_mapping.get(prime_mover, 'PrimeMovers.OT')


def insert_prime_mover_RTS(conn, capacity_df):
    """
    Insert the prime mover data into the database.
    """

    # Get the unique prime movers
    prime_movers = capacity_df['Unit Type'].unique()

    # Create another list with the mapped prime movers
    prime_mover_mapping = [get_prime_mover(prime_mover) for prime_mover in prime_movers]

    # Remove duplicates
    prime_movers = list(set(prime_mover_mapping))

    # Insert the prime movers into the database
    for prime_mover in prime_movers:
        # Insert the data into the database
        functions_schema_ingest.insert_prime_mover_type(conn, prime_mover)


def get_operational_cost(row):
    """
    Create the operational cost string from the data
    """
    # Get the operational cost from the row
    op_cost = {
        "variable_cost": get_vom(row['Fuel']),
        "fixed_cost": get_fom(row['Fuel']),
        "start_up_cost": row['Non Fuel Start Cost $'],
        "startup_fuel_mmbtu_per_mw": row['Start Heat Warm MBTU'],
    }
    # Convert the dictionary to a JSON string.
    return json.dumps(op_cost)


def get_heat_rate(row):
    """
    Create the heat rate string from the data
    """
    if row['HeatRate'] is not None and row['Fuel'] != 'Hydro':
        # Creating a piecewise linear blob
        piecewise_linear_data = []


        piecewise_linear_data.append({
            'from_x': row['Output_pct_0']*row['cap'],
            'to_x': row['Output_pct_1']*row['cap'] if pd.notna(row['Output_pct_1']) and pd.notna(row['cap']) else row['cap'],
            'from_y': row['HeatRate'],
            'to_y': row['HR_incr_1'] if pd.notna(row['HR_incr_1']) else row['HeatRate']
        })

        if pd.notna(row['Output_pct_2']) and pd.notna(row['HR_incr_2']):
            piecewise_linear_data.append({
                'from_x': row['Output_pct_1']*row['cap'],
                'to_x': row['Output_pct_2']*row['cap'],
                'from_y': row['HR_incr_1'],
                'to_y': row['HR_incr_2']
            })

        if pd.notna(row['Output_pct_3']) and pd.notna(row['HR_incr_3']):
            piecewise_linear_data.append({
                'from_x': row['Output_pct_2']*row['cap'],
                'to_x': row['Output_pct_3']*row['cap'],
                'from_y': row['HR_incr_2'],
                'to_y': row['HR_incr_3']
            })

        if pd.notna(row['Output_pct_4']) and pd.notna(row['HR_incr_4']):
            piecewise_linear_data.append({
                'from_x': row['Output_pct_3']*row['cap'],
                'to_x': row['Output_pct_4']*row['cap'],
                'from_y': row['HR_incr_3'],
                'to_y': row['HR_incr_4']
            })

        return json.dumps(piecewise_linear_data)
    else:
        return None


def insert_operational_data_RTS(conn, row, must_run, gen_entity_id):
    """
    Insert the operational data into the database.
    """

    # Insert the operational data values for the entity in the database
    functions_schema_ingest.insert_operational_data(
        conn,
        gen_entity_id,
        row['PMin MW'],
        must_run,
        row['Min Up Time Hr'],
        row['Min Down Time Hr'],
        row['Ramp Rate MW/Min'],
        row['Ramp Rate MW/Min'],
        get_operational_cost(row)
    )


def insert_generation_units_data_RTS(conn, row):
    """
    Insert the generation units data into the database.
    """

    # Get the generator data from the directory structure
    gen_id = functions_schema_ingest.insert_generation_units(conn, 
                                                             row['GEN UID'],
                                                             get_prime_mover(row['Unit Type']),
                                                             functions_schema_ingest.get_bus_name_from_id(conn, row['Bus ID']),
                                                             row['cap'],
                                                             row['Fuel'],
                                                             row['cap'] if row['MVAR Inj'] > row['cap'] else (row['MVAR Inj'] if row['MVAR Inj'] > 0 else 0.00000001))
    
    # Get and return the entity ID for the generator
    return functions_schema_ingest.get_entity_id(conn, 'generation_units', gen_id), gen_id


def insert_hydro_reservoir(conn, row, gen_id):
    """
    Insert the hydro reservoir data into the database.
    """

    # Insert the hydro reservoir data into the database
    hydro_res_id = functions_schema_ingest.insert_hydro_reservoir(conn, row['GEN UID'])

    # Insert the hydro connections
    functions_schema_ingest.insert_hydro_reservoir_connection(conn, gen_id, hydro_res_id)
    


def insert_day_ahead_generation(conn, da_data_df, entity_id, gen_name):
    """
    Insert the day ahead generation data into the database.
    """

    # Convert the year, month, day and period columns to a timestamp
    da_data_df = convert_to_timestamp_RTS(da_data_df, 'Year', 'Month', 'Day', 'Period')

    # Extract the generation data
    da_gen_data_only_df = extract_loads_RTS(da_data_df)[[gen_name]]

    # Convert the initial timestamp to an ISO-formatted string
    initial_ts = da_data_df['Timestamp'].iloc[0]

    if isinstance(initial_ts, pd.Timestamp):
        initial_ts = initial_ts.isoformat()
    
    # Calculate the length (cast to int if needed)
    length_val = len(da_data_df["Period"])

    # Now call insert_time_series supplying all needed parameters
    da_time_series_id = functions_schema_ingest.insert_time_series(
        conn,
        'deterministic_forecast_time_series',           # time_series_type
        'DA Generation',                                # name
        initial_ts,                                     # initial_timestamp as a string
        3600,                                           # resolution_ms
        1,                                              # horizon
        1,                                              # interval
        length_val,                                     # length
        None,                                           # uuid (or provide a string if needed)
        '{"unit": "MW"}',                               # features
        None                                            # metadata (or provide JSON if needed)
    )

    # Insert into time series associations
    functions_schema_ingest.insert_time_series_associations(conn, da_time_series_id, entity_id)

    # Insert the data into the static time series
    for i, value in da_gen_data_only_df.iterrows():
        # Current timestamp is
        current_ts = da_data_df['Timestamp'].iloc[i]

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
        

        
def insert_real_time_generation(conn, rt_data_df, entity_id, gen_name):
    """
    Insert the real time generation data into the database.
    """

    # Convert the year, month, day and period columns to a timestamp
    rt_data_df = convert_to_timestamp_RTS(rt_data_df, 'Year', 'Month', 'Day', 'Period', resolution_minutes=5)

    # Extract the generation data
    rt_gen_data_only_df = extract_loads_RTS(rt_data_df)[[gen_name]]

    # Convert the initial timestamp to an ISO-formatted string
    initial_ts = rt_data_df['Timestamp'].iloc[0]

    if isinstance(initial_ts, pd.Timestamp):
        initial_ts = initial_ts.isoformat()
    
    # Calculate the length (cast to int if needed)
    length_val = len(rt_data_df["Period"])

    rt_time_series_id = functions_schema_ingest.insert_time_series(
        conn,
        'static_time_series',         # time_series_type
        'RT Generation',               # name
        initial_ts,                   # initial_timestamp as a string
        300,                          # resolution_ms
        1,                            # horizon
        1,                            # interval
        length_val,                   # length
        None,                         # uuid (or provide a string if needed)
        '{"unit": "MW"}',             # features
        None                          # metadata (or provide JSON if needed)
    )

    # Insert into time series associations
    functions_schema_ingest.insert_time_series_associations(conn, rt_time_series_id, entity_id)
    # Insert the data into the static time series
    for i, value in rt_gen_data_only_df.iterrows():
        # Current timestamp is
        current_ts = rt_data_df['Timestamp'].iloc[i]

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
        


def insert_hydro_data_RTS(conn, row, directory_structure):
    """
    Insert the hydro data into the database.
    """
    # Get the DA hydro data from the directory structure
    da_hydro_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_hydro.csv'))

    # Get the RT hydro data from the directory structure
    rt_hydro_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_hydro.csv'))

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id, _ = insert_generation_units_data_RTS(conn, row)

    # Insert the day ahead generation data into the database
    insert_day_ahead_generation(conn, da_hydro_data_df, gen_entity_id, row['GEN UID'])

    # Insert the real time generation data into the database
    insert_real_time_generation(conn, rt_hydro_data_df, gen_entity_id, row['GEN UID'])

    # Insert the hydro reservoir data into the database
    insert_hydro_reservoir(conn, row, gen_entity_id)
    

def insert_pv_data_RTS(conn, row, directory_structure):
    """
    Insert the PV data into the database.
    """
    # Get the DA PV data from the directory structure
    da_pv_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_pv.csv'))

    # Get the RT PV data from the directory structure
    rt_pv_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_pv.csv'))

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id, _ = insert_generation_units_data_RTS(conn, row)

    # Insert the day ahead generation data into the database
    insert_day_ahead_generation(conn, da_pv_data_df, gen_entity_id, row['GEN UID'])

    # Insert the real time generation data into the database
    insert_real_time_generation(conn, rt_pv_data_df, gen_entity_id, row['GEN UID'])


def insert_csp_data_RTS(conn, row, directory_structure):
    """
    Insert the CSP data into the database.
    """
    # Get the DA CSP data from the directory structure
    da_csp_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_Natural_Inflow.csv'))

    # Get the RT CSP data from the directory structure
    rt_csp_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_Natural_Inflow.csv'))

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id, _ = insert_generation_units_data_RTS(conn, row)

    # Insert the day ahead generation data into the database
    insert_day_ahead_generation(conn, da_csp_data_df, gen_entity_id, row['GEN UID'])

    # Insert the real time generation data into the database
    insert_real_time_generation(conn, rt_csp_data_df, gen_entity_id, row['GEN UID'])


def insert_rtpv_data_RTS(conn, row, directory_structure):
    """
    Insert the RTPV data into the database.
    """
    # Get the DA RTPV data from the directory structure
    da_rtpv_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_rtpv.csv'))

    # Get the RT RTPV data from the directory structure
    rt_rtpv_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_rtpv.csv'))
    
    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id, _ = insert_generation_units_data_RTS(conn, row)

    # Insert the day ahead generation data into the database
    insert_day_ahead_generation(conn, da_rtpv_data_df, gen_entity_id, row['GEN UID'])

    # Insert the real time generation data into the database
    insert_real_time_generation(conn, rt_rtpv_data_df, gen_entity_id, row['GEN UID'])


def insert_wind_data_RTS(conn, row, directory_structure):
    """
    Insert the wind data into the database.
    """
    # Get the DA wind data from the directory structure
    da_wind_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_wind.csv'))

    # Get the RT wind data from the directory structure
    rt_wind_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_wind.csv'))

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id, _ = insert_generation_units_data_RTS(conn, row)

    # Insert the day ahead generation data into the database
    insert_day_ahead_generation(conn, da_wind_data_df, gen_entity_id, row['GEN UID'])

    # Insert the real time generation data into the database
    insert_real_time_generation(conn, rt_wind_data_df, gen_entity_id, row['GEN UID'])


def insert_thermal_data_RTS(conn, row):
    """
    Insert the thermal data into the database.
    """

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id, _ = insert_generation_units_data_RTS(conn, row)

    if row['Fuel'] == 'NG' or row['Fuel'] == 'Oil':
        # Insert the operational data into the database
        insert_operational_data_RTS(conn, row, False, gen_entity_id)
    else:
        # Insert the operational data into the database
        insert_operational_data_RTS(conn, row, True, gen_entity_id)
    
    # Insert the heat rate as an attribute
    heat_rate_at_id = functions_schema_ingest.insert_attributes(
                                            conn,
                                            'generation_units',
                                            "Heat Rate",
                                            get_heat_rate(row))
    
    # Insert relationship between heat rate and attribute_association
    functions_schema_ingest.insert_attributes_associations(conn, heat_rate_at_id, gen_entity_id)

    # Insert the outage rate as an attribute
    outage_rate_at_id = functions_schema_ingest.insert_attributes(
                                            conn,
                                            'generation_units',
                                            "Outage Rate",
                                            row['FOR'])
    
    # Insert relationship between outage rate and attribute_association
    functions_schema_ingest.insert_attributes_associations(conn, outage_rate_at_id, gen_entity_id)

    # Insert the mttr as an attribute
    mttr_at_id = functions_schema_ingest.insert_attributes(
                                            conn,
                                            'generation_units',
                                            "MTTR",
                                            row['MTTR Hr'])
    
    # Insert relationship between mttr and attribute_association
    functions_schema_ingest.insert_attributes_associations(conn, mttr_at_id, gen_entity_id)

    # Insert CO2 Emissions
    co2_emissions_at_id = functions_schema_ingest.insert_supplemental_attributes(
                                            conn,
                                            "CO2 Emissions",
                                            row['Emissions CO2 Lbs/MMBTU'])
    
    # Insert relationship between CO2 emissions and attribute_association
    functions_schema_ingest.insert_supplemental_attributes_association(conn, co2_emissions_at_id, gen_entity_id)
    

def insert_storage_data_RTS(conn, storage_row, gen_row, directory_structure):
    """
    Insert the storage data into the database.
    """

    # Insert the storage unit inside the database
    storage_id = functions_schema_ingest.insert_storage_units(
        conn,
        storage_row['GEN UID'],
        get_prime_mover(gen_row['Unit Type']),
        storage_row['Max Volume GWh']*1e3,
        functions_schema_ingest.get_bus_name_from_id(conn, gen_row['Bus ID']),
        gen_row['cap'],
        gen_row['cap'] if gen_row['MVAR Inj'] > gen_row['cap'] else (gen_row['MVAR Inj'] if gen_row['MVAR Inj'] > 0 else 0.00000001),
        np.sqrt(((float(gen_row['Storage Roundtrip Efficiency'])/100) + 0.00001)),
        np.sqrt((float((gen_row['Storage Roundtrip Efficiency'])/100) + 0.00001))
    )
    
    # If the unit is hydro, insert the time series
    if gen_row['Fuel'] == 'Hydro':
        # Get the DA storage data from the directory structure
        da_storage_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_hydro.csv'))

        # Get the RT storage data from the directory structure
        rt_storage_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_hydro.csv'))

        # Insert the day ahead generation data into the database
        insert_day_ahead_generation(conn, da_storage_data_df, storage_id, gen_row['GEN UID'])

        # Insert the real time generation data into the database
        insert_real_time_generation(conn, rt_storage_data_df, storage_id, gen_row['GEN UID'])

    elif gen_row['Fuel'] == 'Solar':
        # Get the DA storage data from the directory structure
        da_storage_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_Natural_Inflow.csv'))

        # Get the RT storage data from the directory structure
        rt_storage_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_Natural_Inflow.csv'))

        # Insert the day ahead generation data into the database
        insert_day_ahead_generation(conn, da_storage_data_df, storage_id, gen_row['GEN UID'])

        # Insert the real time generation data into the database
        insert_real_time_generation(conn, rt_storage_data_df, storage_id, gen_row['GEN UID'])


def insert_generation_RTS(conn, directory_structure):
    """
    Insert the generation data into the database.
    """

    # Put the capacity data in a dataframe
    capacity_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'ReEDS_generator_database_final_RTS-GMLC_updated_nodal.csv'))

    # Get the storage dataframe
    storage_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'storage.csv'))

    # Get the set of storage GEN UIDs
    storage_gen_uids = set(storage_df['GEN UID'])

    # Insert the prime movers into the database
    insert_prime_mover_RTS(conn, capacity_df)

    # Insert the fuels into the database
    insert_fuels_RTS(conn, capacity_df)

    for _, row in capacity_df.iterrows():
        # Check if the generator is a storage unit
        if row['GEN UID'] in storage_gen_uids:

            # Extract the storage row from the dataframe
            storage_row = storage_df[storage_df['GEN UID'] == row['GEN UID']].iloc[0]

            # Insert the storage data into the database
            insert_storage_data_RTS(conn, storage_row, row, directory_structure)
        else:
            if row["Fuel"] == "Wind" or row["Fuel"] == "Solar" or row["Fuel"] == "Hydro":
                if row["Fuel"] == "Hydro":
                    insert_hydro_data_RTS(conn, row, directory_structure)
                elif row["Fuel"] == "Solar":
                    if row["Unit Type"] == "PV":
                        insert_pv_data_RTS(conn, row, directory_structure)
                    elif row["Unit Type"] == "CSP":
                        insert_csp_data_RTS(conn, row, directory_structure)
                    elif row["Unit Type"] == "RTPV":
                        insert_rtpv_data_RTS(conn, row, directory_structure)
                    pass
                elif row["Fuel"] == "Wind":
                    insert_wind_data_RTS(conn, row, directory_structure)
            else:
                # If it's a thermal units, non-VRE then no need for time series.
                # Insert the thermal data into the database
                insert_thermal_data_RTS(conn, row)
            

"""
Processing and inserting supply curves from the reV supply curves
Curves are aggregated by region and technology
"""
def create_sorted_dfs_by_region_class(file_path, ba_file_path):
    # Read the CSV files into DataFrames
    df = pd.read_csv(file_path)
    ba_df = pd.read_csv(ba_file_path)
    
    # Create a dictionary to store unique BA values and associated buses
    ba_dict = {ba: ba_df[ba_df['BA'] == ba]['Bus_ID'].values for ba in ba_df['BA'].unique()}
    
    # Create a dictionary to store DataFrames
    dfs_dict = {}
    
    # Group by region and class and process each group
    grouped_df = df.groupby(['region', 'class'])
    
    for (region, cls), group in grouped_df:
        if region in ba_dict:
            region_class_key = f"{region}_{cls}"
            # Sort the DataFrame by capacity in ascending order
            sorted_df = group.sort_values(by='capacity')
            # Store the sorted DataFrame in the dictionary
            dfs_dict[region_class_key] = sorted_df
    
    return dfs_dict, ba_dict


def process_and_insert_supply_curves(dfs_dict, ba_dict, prime_mover, fuel, conn, directory_structure):
    """
    Supply technologies are aggregated at the bus level. What this method does is take in the reV supply curve, aggregate by 
    bus, and enter the supply technologies, supply curves, reinforcement curves and time series for the given insert.
    """

    # Extracting the time series 
    if fuel == 'Solar':
        timeseries_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'pv_availability.csv'))
    else:
        timeseries_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'wind_availability.csv'))
    
    prime_mover = get_prime_mover(prime_mover)

    # First, create the supply technology class
    supply_curve_id = functions_schema_ingest.insert_supply_technologies(conn, 
                                                                         prime_mover,
                                                                         fuel,
                                                                         None,
                                                                         None,
                                                                         'Reference')

    supply_curve_entity_id = functions_schema_ingest.get_entity_id(conn, 'supply_technologies', supply_curve_id)

    # Define the output directory for testing
    # output_directory = '/Users/prao/GitHub_Repos/SiennaInvest/test_supply_curves'
    # os.makedirs(output_directory, exist_ok=True)

    for region_class_key, df in dfs_dict.items():
        region = region_class_key.split('_')[0]
        if region in ba_dict:
            buses = ba_dict[region]
            
            
            for bus in buses:
                # technology_class = df['class'].iloc[0]

                # Data to insert into attributes table
                entity_type = 'supply_technologies'
                name = f"supply curve for {prime_mover} and {fuel} at {bus}"
                
                df = df.sort_values(by='capacity', ascending=True)
                # Process DataFrame and create piecewise linear JSON blobs
                piecewise_linear_data = []
                from_x, from_y = 0, 0
                for idx, row in df.iterrows():
                    to_x = row['capacity']
                    to_y = row['supply_curve_cost_per_mw']
                    piecewise_linear_data.append({
                        'from_x': from_x,
                        'to_x': to_x,
                        'from_y': from_y,
                        'to_y': to_y
                    })
                    from_x, from_y = to_x, to_y

                # Save the first piecewise linear data to CSV
                # piecewise_linear_df = pd.DataFrame(piecewise_linear_data)
                # piecewise_linear_df.to_csv(
                #     os.path.join(output_directory, f"piecewise_linear_{prime_mover}_{bus}_supply_curve.csv"),
                #     index=False
                # )

                # Insert piecewise linear data
                piecewise_linear_blob = json.dumps(piecewise_linear_data)
                piecewise_linear_blob = bytes(piecewise_linear_blob, 'utf-8')

                # Insert the piecewise linear into the attributes
                supply_curve_at_id = functions_schema_ingest.insert_attributes(conn,
                                                          entity_type,
                                                          name,
                                                          piecewise_linear_blob)
                

                # Insert the attribute association
                functions_schema_ingest.insert_attributes_associations(conn,
                                                                       supply_curve_at_id,
                                                                       supply_curve_entity_id)


                # cursor.execute(insert_attribute_sql, (gen_entity_id, entity_type, data_type, name, piecewise_linear_blob))
                

                # Process DataFrame for the second piecewise linear insert
                reinforcement_name = f"reinforcement curve for {prime_mover} and {fuel} at {bus}"
                piecewise_linear_data_2 = []
                from_x, from_y = 0, 0
                for idx, row in df.iterrows():
                    to_x = row['dist_km']
                    to_y = row['reinforcement_dist_km']
                    piecewise_linear_data_2.append({
                        'from_x': from_x,
                        'to_x': to_x,
                        'from_y': from_y,
                        'to_y': to_y
                    })
                    from_x, from_y = to_x, to_y

                # Save the second piecewise linear data to CSV
                # piecewise_linear_df_2 = pd.DataFrame(piecewise_linear_data_2)
                # piecewise_linear_df_2.to_csv(
                #     os.path.join(output_directory, f"piecewise_linear_{prime_mover}_{bus}_reinforcement_curve.csv"),
                #     index=False
                # )


                # Insert second piecewise linear data
                piecewise_linear_blob_2 = json.dumps(piecewise_linear_data_2)
                piecewise_linear_blob_2 = bytes(piecewise_linear_blob_2, 'utf-8')
                
                # Insert the piecewise linear into the attributes
                reinforcement_curve_at_id = functions_schema_ingest.insert_attributes(conn,
                                                          entity_type,
                                                          reinforcement_name,
                                                          piecewise_linear_blob_2)
                

                # Insert the attribute association
                functions_schema_ingest.insert_attributes_associations(conn,
                                                                       reinforcement_curve_at_id,
                                                                       supply_curve_entity_id)
                
                initial_ts = timeseries_df['Timestamps'].iloc[0]
                if isinstance(initial_ts, pd.Timestamp):
                    initial_ts = initial_ts.isoformat()
                    
                # Calculate the length (cast to int if needed)
                length_val = len(timeseries_df["Timestamps"])

                # Now, insert the time series
                time_series_id = functions_schema_ingest.insert_time_series(conn,
                                                                            'static_time_series',           # time_series_type
                                                                            f'PV Time Series for {bus}',    # name
                                                                            initial_ts,                     # initial_timestamp as a string
                                                                            300,                            # resolution_ms
                                                                            1,                              # horizon
                                                                            1,                              # interval
                                                                            length_val,                     # length
                                                                            None,                           # uuid (or provide a string if needed)
                                                                            '{"unit": "MW"}',               # features
                                                                            None                            # metadata (or provide JSON if needed)
                                                                            )
                
                # Insert into time series associations
                functions_schema_ingest.insert_time_series_associations(conn, time_series_id, supply_curve_entity_id)

                col = str(bus)

                for i, row in timeseries_df.iterrows():
                    # Current timestamp is
                    current_ts = timeseries_df['Timestamps'].iloc[i]

                    # Convert the timestamp to an ISO-formatted string
                    if isinstance(current_ts, pd.Timestamp):
                        current_ts = current_ts.isoformat()

                    # Insert the data into the database
                    functions_schema_ingest.insert_static_time_series(
                        conn,
                        time_series_id,
                        current_ts,
                        float(row[col])
                    )
                                                                    

def insert_investment_options_RTS(conn, directory_structure):
    """
    This function inserts the investment options for the RTS system.
    """

    file_path_solar = functions_handlers.find_filepath(directory_structure, 'upv_supply_curve-reference_ba.csv')
    file_path_wind = functions_handlers.find_filepath(directory_structure, 'wind-ons_supply_curve-reference_ba.csv')
    ba_file_path = functions_handlers.find_filepath(directory_structure, 'bus_mod_updatedwithBA.csv')

    # Create the dictionary of sorted DataFrames
    dfs_dict_solar, ba_dict = create_sorted_dfs_by_region_class(file_path_solar, ba_file_path)
    dfs_dict_wind, ba_dict_wind = create_sorted_dfs_by_region_class(file_path_wind, ba_file_path)

    process_and_insert_supply_curves(dfs_dict_solar, ba_dict, 'PV', 'Solar', conn, directory_structure)

    # Loading the piecewise linear data for wind
    process_and_insert_supply_curves(dfs_dict_wind, ba_dict_wind, 'Wind', 'Wind', conn, directory_structure)



def process_and_ingest_RTS_data(conn, directory_structure):
    """
    Process and ingest RTS data into the database.
    """

    # First, insert the buses and regions
    insert_regions_RTS(conn, directory_structure)
    insert_buses_RTS(conn, directory_structure)

    # Second, insert the branches
    insert_branches_RTS(conn, directory_structure)
    insert_interchanges_RTS(conn, directory_structure)

    # Third, insert the generation data
    insert_generation_RTS(conn, directory_structure)

    # Fourth, insert the load data
    insert_loads_RTS(conn, directory_structure)

    # Finally, insert the investment options
    insert_investment_options_RTS(conn, directory_structure)