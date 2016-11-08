/*
 * CSU BGP CORE TABLES
 */
CREATE KEYSPACE csu_bgp_core WITH replication = {'class': 'NetworkTopologyStrategy', 'csu_dc': '2'} AND durable_writes = true;

CREATE TYPE csu_bgp_core.prefix (ip_address inet, mask int);

CREATE TABLE csu_bgp_core.update_messages_by_time (
    time_bucket timestamp,
    timestamp timeuuid,
    collector_ip_address inet,
    collector_mac_address blob, //REMOVE
    collector_port int, //REMOVE
    peer_ip_address inet,
    as_path list< bigint >,
    next_hop inet,
    advertised_prefixes list< frozen< prefix> >,
    withdrawn_prefixes list< frozen< prefix> >,
    PRIMARY KEY (time_bucket, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'};

CREATE TABLE csu_bgp_core.organization_by_as_number (
    as_number bigint,
    measure_time timeuuid,
    organization_name text,
    organization_abbreviation text,
    local_registry text,
    regional_registry text,
    website text,
    city text,
    state text,
    country text,
    source text,
    PRIMARY KEY (as_number, measure_time)
) WITH CLUSTERING ORDER BY (measure_time DESC);

/*
 * CSU BGP DERIVED TABLES
 */
CREATE KEYSPACE csu_bgp_derived WITH replication = {'class': 'NetworkTopologyStrategy', 'csu_dc': '2'}  AND durable_writes = true;

CREATE TYPE csu_bgp_derived.prefix (ip_address inet, mask int);

CREATE TABLE csu_bgp_derived.as_number_by_prefix_range (
    time_bucket timestamp,
    prefix_ip_address inet,
    prefix_mask int,
    timestamp timeuuid,
    is_advertisement bool, //ADDED
    as_number bigint,
    PRIMARY KEY (time_bucket, prefix_ip_address, prefix_mask, timestamp)
) WITH CLUSTERING ORDER BY (prefix_ip_address ASC, prefix_mask ASC, timestamp DESC);

CREATE TABLE csu_bgp_derived.as_number_by_prefix (
    prefix_ip_address inet,
    prefix_mask int,
    measure_date timestamp,
    as_number bigint,
    prefixes map< frozen<prefix>, int >,
    PRIMARY KEY ((prefix_ip_address, prefix_mask), measure_date, as_number)
) WITH CLUSTERING ORDER BY (measure_date DESC, as_number ASC);

CREATE TABLE csu_bgp_derived.prefix_by_as_number (
    as_number bigint,
    measure_date timestamp,
    prefix_ip_address inet,
    prefix_mask int,
    update_message_count int,
    PRIMARY KEY (as_number, measure_date, prefix_ip_address, prefix_mask)
) WITH CLUSTERING ORDER BY (measure_date DESC, prefix_ip_address ASC, prefix_mask ASC);

CREATE TABLE csu_bgp_derived.prefix_hijacks ( //NEW TABLE
    time_bucket timestamp,
    timestamp timeuuid,
    monitor_ip_address inet,
    monitor_mask inet,
    module_id string,
    PRIMARY KEY (time_bucket, moduel_id, timeuuid)
) WITH CLUSTERING ORDER BY (timestamp DESC);

/*
 * CSU GEO LOCATION CORE TABLES
 */
CREATE KEYSPACE csu_location_core WITH replication = {'class': 'NetworkTopologyStrategy', 'csu_dc': '2'} AND durable_writes = true;

CREATE TABLE csu_location_core.location_by_as_number (
    as_number bigint,
    measure_date timeuuid,
    country_code text,
    state_code,
    city text,
    latitude float,
    longitude float,
    source text,
    PRIMARY KEY (as_number, measure_date)
) WITH CLUSTERING ORDER BY (measure_date DESC);

CREATE TABLE csu_location_core.location_by_ip_address (
    ip_address inet,
    measure_date timeuuid,
    country_code text,
    state_code text,
    city text,
    latitude float,
    longitude float,
    source text,
    PRIMARY KEY (ip_address, measure_date)
) WITH CLUSTERING ORDER BY (measure_date DESC);

CREATE TABLE csu_location_core.location_by_prefix (
    prefix_ip_address inet,
    prefix_mask int,
    measure_date timeuuid,
    country_code text,
    state_code text,
    city text,
    latitude float,
    longitude float,
    source text,
    PRIMARY KEY ((prefix_ip_address, prefix_mask), measure_date)
) WITH CLUSTERING ORDER BY (measure_date DESC);
