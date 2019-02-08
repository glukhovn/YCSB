CREATE DATABASE ycsb;
CREATE USER ycsb PASSWORD 'ycsb';
GRANT ALL ON DATABASE ycsb to ycsb;

\connect ycsb ycsb

CREATE TABLE soe_customers(data jsonb);

COPY soe_customers FROM '/path/to/customers.json';

UPDATE soe_customers
SET data = data || jsonb_build_object(
	'YCSB_KEY', data->'_id',
	'linked_devices', (SELECT jsonb_agg(arr->'arr') FROM jsonb_array_elements(data->'linked_devices') arr)
--	'linked_devices', jsonb_path_query_array(data, '$.linked_devices[*].arr')  SQL/JSON variant
);
