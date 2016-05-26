/*
 * CSU BGP CORE TABLES
 */
CREATE KEYSPACE csu_bgp_core WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '2'} AND durable_writes = true;

CREATE TYPE csu_bgp_core.prefix (ip_address inet, mask int);

CREATE TABLE csu_bgp_core.update_messages_by_time (
    time_bucket timestamp,
    timestamp timeuuid,
    protobuf blob,
    PRIMARY KEY (time_bucket, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'};

/*CREATE TABLE csu_bgp_core.update_messages_by_time (
	time_bucket timestamp,
	timestamp timeuuid,
	announced_prefixes list< frozen< prefix> >,
	withdrawn_prefixes list< frozen< prefix> >,
	collector_ip_address inet,
	peer_ip_address inet,	
	as_path list< bigint >,
	bytes blob,
	source text,
	source_set text,
	message_number bigint,
	PRIMARY KEY (time_bucket, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
	AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'};

CREATE INDEX ON csu_bgp_core.update_messages_by_time(as_path); 

CREATE MATERIALIZED VIEW csu_bgp_core.update_messages_by_collector_view AS
	SELECT time_bucket, timestamp, announced_prefixes, withdrawn_prefixes, collector_ip_address, peer_ip_address, as_path, bytes FROM csu_bgp_core.update_messages_by_time
	WHERE collector_ip_address IS NOT NULL AND time_bucket IS NOT NULL AND timestamp IS NOT NULL
	PRIMARY KEY ((collector_ip_address, time_bucket), timestamp)
	WITH CLUSTERING ORDER BY (timestamp DESC)
	AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'};

CREATE MATERIALIZED VIEW csu_bgp_core.update_messages_by_peer_view AS
	SELECT time_bucket, timestamp, announced_prefixes, withdrawn_prefixes, collector_ip_address, peer_ip_address, as_path, bytes FROM csu_bgp_core.update_messages_by_time
	WHERE peer_ip_address IS NOT NULL AND time_bucket IS NOT NULL AND timestamp IS NOT NULL
	PRIMARY KEY ((peer_ip_address, time_bucket), timestamp)
	WITH CLUSTERING ORDER BY (timestamp DESC)
	AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'}; */

-- CREATE INDEX ON csu_bgp_core.update_message_by_time(announed_prefixes);
-- CREATE INDEX ON csu_bgp_core.update_message_by_time(withdrawn_prefixes);

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
CREATE KEYSPACE csu_bgp_derived WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '2'}  AND durable_writes = true;

CREATE TYPE csu_bgp_derived.prefix (ip_address inet, mask int);

CREATE TABLE csu_bgp_derived.as_number_by_prefix_range (
        time_bucket timestamp,
        prefix_ip_address inet,
        prefix_mask int,
        timestamp timeuuid,
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





/*
 * CSU GEO LOCATION CORE TABLES
 */
CREATE KEYSPACE csu_location_core WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '2'} AND durable_writes = true;

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
