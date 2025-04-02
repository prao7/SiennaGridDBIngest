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


def insert_prime_movers(conn, prime_mover, fuel=None, description=None):
    """
    Inserts a row into the prime_movers table.
    """

    sql = """
    INSERT INTO prime_movers (prime_mover, fuel, description)
    VALUES (?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (prime_mover, fuel, description))
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


def insert_generation_units(conn, name, prime_mover, balancing_topology, start_year, base_power, fuel=None, rating=1.0):
    """
    Inserts a row into the generation_units table.
    """

    sql = """
    INSERT INTO generation_units 
      (name, prime_mover, fuel, balancing_topology, start_year, rating, base_power)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (name, prime_mover, fuel, balancing_topology, start_year, rating, base_power))
    conn.commit()
    return cur.lastrowid


def insert_storage_units(conn, name, prime_mover, max_capacity, balancing_topology, start_year, base_power, 
                           rating=1, charging_efficiency=1.0, discharge_efficiency=1.0):
    """
    Inserts a row into the storage_units table.
    """

    sql = """
    INSERT INTO storage_units
      (name, prime_mover, max_capacity, balancing_topology, charging_efficiency, discharge_efficiency, start_year, rating, base_power)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (name, prime_mover, max_capacity, balancing_topology, charging_efficiency,
                      discharge_efficiency, start_year, rating, base_power))
    conn.commit()
    return cur.lastrowid


def insert_supply_technologies(conn, prime_mover, vom_cost, fom_cost, fuel=None, area=None, balancing_topology=None, scenario=None):
    """
    Inserts a row into the supply_technologies table.
    """

    sql = """
    INSERT INTO supply_technologies 
      (prime_mover, fuel, area, balancing_topology, vom_cost, fom_cost, scenario)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (prime_mover, fuel, area, balancing_topology, vom_cost, fom_cost, scenario))
    conn.commit()
    return cur.lastrowid


def insert_operational_data(conn, entity_id, fom_cost, vom_cost, startup_cost, min_stable_level,
                            mttr, startup_fuel_mmbtu_per_mw, uptime, downtime, operational_cost=None):
    """
    Inserts a row into the operational_data table.
    """

    sql = """
    INSERT INTO operational_data 
      (entity_id, fom_cost, vom_cost, startup_cost, min_stable_level, mttr, startup_fuel_mmbtu_per_mw, uptime, downtime, operational_cost)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    cur.execute(sql, (entity_id, fom_cost, vom_cost, startup_cost, min_stable_level,
                      mttr, startup_fuel_mmbtu_per_mw, uptime, downtime, operational_cost))
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
