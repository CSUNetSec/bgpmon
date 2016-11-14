/*
 * CSU BGP CORE TABLES
 */
CREATE KEYSPACE csu_bgp_core WITH replication = {'class': 'NetworkTopologyStrategy', 'csu_dc': '2'} AND durable_writes = true;

CREATE TYPE csu_bgp_core.prefix (ip_address inet, mask int);

CREATE TABLE csu_bgp_core.update_messages_by_time (
    time_bucket timestamp,
    timestamp timeuuid,
    collector_ip_address inet,
    peer_ip_address inet,
    as_path list< bigint >,
    next_hop inet,
    advertised_prefixes list< frozen< prefix> >,
    withdrawn_prefixes list< frozen< prefix> >,
    protomsg blob,
    PRIMARY KEY (time_bucket, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'};

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
    as_number bigint,
    is_withdrawal boolean,
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

CREATE TABLE csu_bgp_derived.advertised_prefix_by_as_number (
    time_bucket timestamp,
    as_number bigint,
    prefix_ip_address inet,
    prefix_mask int,
    advertised_count counter,
    PRIMARY KEY (time_bucket, as_number, prefix_ip_address, prefix_mask)
) WITH CLUSTERING ORDER BY (as_number ASC, prefix_ip_address ASC, prefix_mask ASC);

CREATE TABLE csu_bgp_derived.withdrawn_prefix_by_time (
    time_bucket timestamp,
    prefix_ip_address inet,
    prefix_mask int,
    withdrawn_count counter,
    PRIMARY KEY (time_bucket, prefix_ip_address, prefix_mask)
) WITH CLUSTERING ORDER BY (prefix_ip_address ASC, prefix_mask ASC);

CREATE TABLE csu_bgp_derived.prefix_hijacks (
    time_bucket timestamp,
    module_id string,
    timestamp timeuuid,
    advertised_ip_address inet,
    advertised_mask int,
    monitor_ip_address inet,
    monitor_mask inet,
    PRIMARY KEY (time_bucket, module_id, timestamp, advertised_ip_address, advertised_mask, monitor_ip_address, monitor_mask)
) WITH CLUSTERING ORDER BY (module_id ASC, timestamp DESC);

/*
 * CSU BGP CONFIG TABLES
 */
CREATE KEYSPACE csu_bgp_config WITH replication = {'class': 'NetworkTopologyStrategy', 'csu_dc': '2'}  AND durable_writes = true;

CREATE TABLE csu_bgp_config.monitor_prefixes (
    module_id text,
    timestamp timeuuid,
    ip_address inet,
    mask int,
    as_number bigint,
    enabled boolean,
    PRIMARY KEY (module_id, timestamp)
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
