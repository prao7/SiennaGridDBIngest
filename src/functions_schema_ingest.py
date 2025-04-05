import sqlite3


def insert_entities(conn, entity_type, entity_id):
    """
    Inserts a row into the entities table.
    """

    sql = """
    INSERT INTO entities (entity_type, entity_id)
    VALUES (?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (entity_type, entity_id))
    conn.commit()
    return cur.lastrowid


def insert_planning_regions(conn, id, name, description=None):
    """
    Inserts a row into the planning_regions table.
    """

    sql = """
    INSERT INTO planning_regions (id, name, description)
    VALUES (?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (id, name, description))
    conn.commit()
    return cur.lastrowid


def insert_balancing_topologies(conn, bus_id, name, area=None, description=None):
    """
    Inserts a row into the balancing_topologies table.
    """

    sql = """
    INSERT INTO balancing_topologies (id, name, area, description)
    VALUES (?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (bus_id, name, area, description))
    conn.commit()
    return cur.lastrowid


def insert_arcs(conn, from_to, to_from):
    """
    Inserts a row into the arcs table.
    """

    sql = """
    INSERT INTO arcs (from_to, to_from)
    VALUES (?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (from_to, to_from))
    conn.commit()
    return cur.lastrowid


def insert_transmission_lines(conn, continuous_rating, ste_rating, lte_rating, line_length, arc_id=None):
    """
    Inserts a row into the transmission_lines table.
    """

    sql = """
    INSERT INTO transmission_lines (arc_id, continuous_rating, ste_rating, lte_rating, line_length)
    VALUES (?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (arc_id, continuous_rating, ste_rating, lte_rating, line_length))
    conn.commit()
    return cur.lastrowid


def insert_transmission_interchange(conn, arc_id, max_flow_from, max_flow_to):
    """
    Inserts a row into the transmission_interchange table.
    """

    sql = """
    INSERT INTO transmission_interchanges (arc_id, max_flow_from, max_flow_to)
    VALUES (?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (arc_id, max_flow_from, max_flow_to))
    conn.commit()
    return cur.lastrowid


def insert_transmission_interchange(conn, arc_id, name, max_flow_from, max_flow_to):
    """
    Inserts a row into the transmission_interchanges table.
    """
    sql = """
    INSERT INTO transmission_interchanges (arc_id, name, max_flow_from, max_flow_to)
    VALUES (?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (arc_id, name, max_flow_from, max_flow_to))
    conn.commit()
    return cur.lastrowid


def insert_generation_units(conn, name, prime_mover, balancing_topology, base_power, fuel=None, rating=1.0):
    """
    Inserts a row into the generation_units table based on the new design.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        name (str): The unique name of the generation unit.
        prime_mover (str): The prime mover type (must match an entry in prime_mover_types).
        balancing_topology (str): The balancing topology (must match an entry in balancing_topologies).
        base_power (float): The base power of the unit (must be > 0 and >= rating).
        fuel (str, optional): The fuel type (must match an entry in fuels, if provided).
        rating (float, optional): The rating of the unit (> 0). Defaults to 1.0.
        
    Returns:
        int: The ID of the newly inserted generation unit.
    """
    sql = """
    INSERT INTO generation_units 
      (name, prime_mover, fuel, balancing_topology, rating, base_power)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (name, prime_mover, fuel, balancing_topology, rating, base_power))
    conn.commit()
    return cur.lastrowid


def insert_storage_units(conn, name, prime_mover, max_capacity, balancing_topology,
                         base_power, rating=1, efficiency_up=1.0, efficiency_down=1.0):
    """
    Inserts a row into the storage_units table.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        name (str): The unique name of the storage unit.
        prime_mover (str): The prime mover type (must match an entry in prime_mover_types).
        max_capacity (float): The energy capacity (> 0).
        balancing_topology (str): The balancing topology (must match an entry in balancing_topologies).
        base_power (float): The base power of the storage unit (> 0 and >= rating).
        rating (float, optional): The rating of the unit (> 0). Defaults to 1.
        efficiency_up (float, optional): The charging efficiency (0 < value <= 1.0). Defaults to 1.0.
        efficiency_down (float, optional): The discharging efficiency (0 < value <= 1.0). Defaults to 1.0.
        
    Returns:
        int: The ID of the newly inserted storage unit.
    """
    sql = """
    INSERT INTO storage_units 
      (name, prime_mover, max_capacity, balancing_topology, efficiency_up, efficiency_down, rating, base_power)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (name, prime_mover, max_capacity, balancing_topology, efficiency_up, efficiency_down, rating, base_power))
    conn.commit()
    return cur.lastrowid


def insert_hydro_reservoir(conn, name):
    """
    Inserts a row into the hydro_reservoir table.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        name (str): The name of the reservoir.
        
    Returns:
        int: The ID of the newly inserted reservoir.
    """
    sql = """
    INSERT INTO hydro_reservoir (name)
    VALUES (?)
    """
    cur = conn.cursor()
    cur.execute(sql, (name,))
    conn.commit()
    return cur.lastrowid


def insert_hydro_reservoir_connection(conn, turbine_id, reservoir_id):
    """
    Inserts a row into the hydro_reservoir_connections table.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        turbine_id (int): The id of the generation unit (turbine).
        reservoir_id (int): The id of the hydro reservoir.
        
    Returns:
        int: The ID of the newly inserted row.
    """
    sql = """
    INSERT INTO hydro_reservoir_connections (turbine_id, reservoir_id)
    VALUES (?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (turbine_id, reservoir_id))
    conn.commit()
    return cur.lastrowid



def insert_supply_technologies(conn, prime_mover, fuel=None, area=None, balancing_topology=None, scenario=None):
    """
    Inserts a row into the supply_technologies table.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        prime_mover (str): The prime mover type (must match an entry in prime_mover_types).
        fuel (str, optional): The fuel type (must match an entry in fuels, if provided).
        area (str, optional): The area (must match an entry in planning_regions, if provided).
        balancing_topology (str, optional): The balancing topology (must match an entry in balancing_topologies, if provided).
        scenario (str, optional): The scenario name.
        
    Returns:
        int: The ID of the newly inserted supply_technologies row.
    """
    sql = """
    INSERT INTO supply_technologies 
        (prime_mover, fuel, area, balancing_topology, scenario)
    VALUES (?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (prime_mover, fuel, area, balancing_topology, scenario))
    conn.commit()
    return cur.lastrowid


def insert_transport_technologies(conn, arc_id=None, scenario=None):
    """
    Inserts a row into the transport_technologies table.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        arc_id (int, optional): The associated arc ID (references arcs(id)). Defaults to None.
        scenario (str, optional): The scenario name. Defaults to None.
    
    Returns:
        int: The ID of the newly inserted transport_technologies row.
    """
    sql = """
    INSERT INTO transport_technologies (arc_id, scenario)
    VALUES (?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (arc_id, scenario))
    conn.commit()
    return cur.lastrowid


def insert_operational_data(conn, entity_id, active_power_limit_min, must_run, uptime, downtime, ramp_up, ramp_down, operational_cost=None):
    """
    Inserts a row into the operational_data table.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        entity_id (int): The ID of the associated entity.
        active_power_limit_min (float): The minimum active power limit (>= 0).
        must_run (bool): Flag indicating if the unit must run.
        uptime (float): Uptime value (>= 0).
        downtime (float): Downtime value (>= 0).
        ramp_up (float): The ramp-up rate.
        ramp_down (float): The ramp-down rate.
        operational_cost (str or None): Optional JSON string representing the operational cost details.
    
    Returns:
        int: The ID of the newly inserted operational_data row.
    """
    sql = """
    INSERT INTO operational_data 
      (entity_id, active_power_limit_min, must_run, uptime, downtime, ramp_up, ramp_down, operational_cost)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (entity_id, active_power_limit_min, must_run, uptime, downtime, ramp_up, ramp_down, operational_cost))
    conn.commit()
    return cur.lastrowid


def insert_prime_mover_type(conn, name, description=None):
    """
    Inserts a row into the prime_mover_types table.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        name (str): The prime mover type (e.g., 'CT').
        description (str): Description of the prime mover type.
        
    Returns:
        int: The ID of the newly inserted row.
    """
    
    sql = """
    INSERT INTO prime_mover_types (name, description)
    VALUES (?, ?)
    """
    
    cur = conn.cursor()
    cur.execute(sql, (name, description))
    conn.commit()
    
    return cur.lastrowid


def insert_fuel(conn, name, description=None):
    """
    Inserts a row into the fuels table.
    
    Args:
        conn (sqlite3.Connection): The database connection.
        name (str): The fuel name (e.g., 'NG').
        description (str): Description of the fuel.
        
    Returns:
        int: The ID of the newly inserted row.
    """
    
    sql = """
    INSERT INTO fuels (name, description)
    VALUES (?, ?)
    """
    
    cur = conn.cursor()
    cur.execute(sql, (name, description))
    conn.commit()
    
    return cur.lastrowid


def insert_attributes(conn, type, name, value):
    """
    Inserts a row into the attributes table.
    """

    sql = """
    INSERT INTO attributes (type, name, value)
    VALUES (?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (type, name, value))
    conn.commit()
    return cur.lastrowid


def insert_attributes_associations(conn, attribute_id, entity_id):
    """
    Inserts a row into the attributes_associations table.
    """

    sql = """
    INSERT INTO attributes_associations (attribute_id, entity_id)
    VALUES (?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (attribute_id, entity_id))
    conn.commit()
    return cur.lastrowid


def insert_supplemental_attributes(conn, type, value):
    """
    Inserts a row into the supplemental_attributes table.
    """

    sql = """
    INSERT INTO supplemental_attributes (type, value)
    VALUES (?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (type, value))
    conn.commit()
    return cur.lastrowid


def insert_supplemental_attributes_association(conn, attribute_id, entity_id):
    """
    Inserts a row into the supplemental_attributes_association table.
    """

    sql = """
    INSERT INTO supplemental_attributes_association (attribute_id, entity_id)
    VALUES (?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (attribute_id, entity_id))
    conn.commit()
    return cur.lastrowid


def insert_time_series(conn, time_series_type, name, initial_timestamp, resolution_ms, horizon, interval, length,
                       uuid=None, features=None, metadata=None):
    """
    Inserts a row into the time_series table.
    """

    sql = """
    INSERT INTO time_series 
      (time_series_type, name, initial_timestamp, resolution_ms, horizon, interval, length, uuid, features, metadata)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (time_series_type, name, initial_timestamp, resolution_ms, horizon, interval, length, uuid, features, metadata))
    conn.commit()
    return cur.lastrowid


def insert_time_series_associations(conn, time_series_id, owner_id):
    """
    Inserts a row into the time_series_associations table.
    """

    sql = """
    INSERT INTO time_series_associations (time_series_id, owner_id)
    VALUES (?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (time_series_id, owner_id))
    conn.commit()
    return cur.lastrowid


def insert_static_time_series(conn, time_series_id, timestamp, value, uuid=None):
    """
    Inserts a row into the static_time_series table.
    """

    sql = """
    INSERT INTO static_time_series (time_series_id, uuid, timestamp, value)
    VALUES (?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (time_series_id, uuid, timestamp, value))
    conn.commit()
    return cur.lastrowid


def insert_deterministic_time_series(conn, time_series_id, timestamp, value, uuid=None):
    """
    Inserts a row into the deterministic_time_series table.
    """

    sql = """
    INSERT INTO deterministic_forecast_time_series (time_series_id, uuid, timestamp, value)
    VALUES (?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (time_series_id, uuid, timestamp, value))
    conn.commit()
    return cur.lastrowid


def insert_probabilistic_time_series(conn, time_series_id, timestamp, value, uuid=None):
    """
    Inserts a row into the probabilistic_time_series table.
    """
    sql = """
    INSERT INTO probabilistic_forecast_time_series (time_series_id, uuid, timestamp, value)
    VALUES (?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (time_series_id, uuid, timestamp, value))
    conn.commit()
    return cur.lastrowid


def get_entity_id(conn, entity_type, entity_id):
    """
    Fetches the entity ID from the database.
    """

    sql = """
    SELECT id FROM entities WHERE entity_type = ? AND entity_id = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (entity_type, entity_id))
    result = cur.fetchone()

    if result:
        return result[0]
    else:
        return None
    
def get_arc_id(conn, from_to, to_from):
    """
    Fetches the arc ID from the database.
    """

    sql = """
    SELECT id FROM arcs WHERE from_to = ? AND to_from = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (from_to, to_from))
    result = cur.fetchone()

    if result:
        return result[0]
    else:
        return None


def get_transmission_id_from_arc_id(conn, arc_id):
    """
    Fetches the transmission ID from the database using arc ID.
    """

    sql = """
    SELECT id FROM transmission_lines WHERE arc_id = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (arc_id,))
    result = cur.fetchone()

    if result:
        return result[0]
    else:
        return None
    

def get_bus_from_name(conn, bus_name):
    """
    Fetches the bus ID from the database using bus name.
    """
    sql = """
    SELECT id FROM balancing_topologies WHERE name = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (bus_name,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    else:
        return None


def get_bus_name_from_id(conn, bus_id):
    """
    Fetches the bus name from the database using bus ID.
    """
    sql = """
    SELECT name FROM balancing_topologies WHERE id = ?
    """
    cur = conn.cursor()
    cur.execute(sql, (bus_id,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    else:
        return None