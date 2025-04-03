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
    return functions_schema_ingest.get_entity_id(conn, 'generation_units', gen_id)


def insert_hydro_data_RTS(conn, row, directory_structure):
    """
    Insert the hydro data into the database.
    """
    # Get the DA hydro data from the directory structure
    da_hydro_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_hydro.csv'))

    # Get the RT hydro data from the directory structure
    rt_hydro_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_hydro.csv'))

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id = insert_generation_units_data_RTS(conn, row)
    

def insert_pv_data_RTS(conn, row, directory_structure):
    """
    Insert the PV data into the database.
    """
    # Get the DA PV data from the directory structure
    da_pv_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_pv.csv'))

    # Get the RT PV data from the directory structure
    rt_pv_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_pv.csv'))

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id = insert_generation_units_data_RTS(conn, row)


def insert_csp_data_RTS(conn, row, directory_structure):
    """
    Insert the CSP data into the database.
    """
    # Get the DA CSP data from the directory structure
    da_csp_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_Natural_Inflow.csv'))

    # Get the RT CSP data from the directory structure
    rt_csp_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_Natural_Inflow.csv'))

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id = insert_generation_units_data_RTS(conn, row)


def insert_rtpv_data_RTS(conn, row, directory_structure):
    """
    Insert the RTPV data into the database.
    """
    # Get the DA RTPV data from the directory structure
    da_rtpv_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_rtpv.csv'))

    # Get the RT RTPV data from the directory structure
    rt_rtpv_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_rtpv.csv'))
    
    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id = insert_generation_units_data_RTS(conn, row)


def insert_wind_data_RTS(conn, row, directory_structure):
    """
    Insert the wind data into the database.
    """
    # Get the DA wind data from the directory structure
    da_wind_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'DAY_AHEAD_wind.csv'))

    # Get the RT wind data from the directory structure
    rt_wind_data_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'REAL_TIME_wind.csv'))

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id = insert_generation_units_data_RTS(conn, row)


def insert_thermal_data_RTS(conn, row):
    """
    Insert the thermal data into the database.
    """

    # Get the entity ID for the generator and insert the generator into the generation_units table
    gen_entity_id = insert_generation_units_data_RTS(conn, row)

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


def insert_generation_RTS(conn, directory_structure):
    """
    Insert the generation data into the database.
    """

    # Put the capacity data in a dataframe
    capacity_df = pd.read_csv(functions_handlers.find_filepath(directory_structure, 'ReEDS_generator_database_final_RTS-GMLC_updated_nodal.csv'))

    # Insert the prime movers into the database
    insert_prime_mover_RTS(conn, capacity_df)

    # Insert the fuels into the database
    insert_fuels_RTS(conn, capacity_df)

    for _, row in capacity_df.iterrows():
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
    insert_interchanges_RTS(conn, directory_structure)

    # Third, insert the generation data
    insert_generation_RTS(conn, directory_structure)

    # Fourth, insert the load data
    # insert_loads_RTS(conn, directory_structure)

    # Finally, insert the investment options
    insert_investment_options_RTS(conn, directory_structure)