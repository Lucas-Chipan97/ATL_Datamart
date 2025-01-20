CREATE EXTENSION IF NOT EXISTS dblink;

-- Connecter à la base de données source via dblink
SELECT dblink_connect(
    'data_warehouse_conn',
    'host=data-warehouse port=5432 dbname=data_warehouse user=postgres password=admin'
);

-- Remplir la dimension Vendor
INSERT INTO snowflake.dim_vendor (vendor_id, vendor_name)
SELECT DISTINCT COALESCE(vendorid, -1), 'Unknown Vendor'
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT vendorid FROM mon_schema.yellow_tripdata'
) AS t(vendorid INT);

-- Remplir la dimension Rate Code
INSERT INTO snowflake.dim_rate_code (rate_code_id, rate_code_description)
SELECT DISTINCT COALESCE(ratecodeid, -1), 'Unknown Rate Code'
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT ratecodeid FROM mon_schema.yellow_tripdata'
) AS t(ratecodeid INT);

-- Remplir la dimension Location
INSERT INTO snowflake.dim_location (location_id, borough, zone, latitude, longitude)
SELECT DISTINCT COALESCE(pulocationid, -1), 'Unknown Borough', 'Unknown Zone', NULL::DOUBLE PRECISION, NULL::DOUBLE PRECISION
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT pulocationid FROM mon_schema.yellow_tripdata'
) AS t(pulocationid INT)
UNION
SELECT DISTINCT COALESCE(dolocationid, -1), 'Unknown Borough', 'Unknown Zone', NULL::DOUBLE PRECISION, NULL::DOUBLE PRECISION
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT dolocationid FROM mon_schema.yellow_tripdata'
) AS t(dolocationid INT);

-- Remplir la dimension Payment Type
INSERT INTO snowflake.dim_payment (payment_type_id, payment_description)
SELECT DISTINCT COALESCE(payment_type, -1), 'Unknown Payment'
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT payment_type FROM mon_schema.yellow_tripdata'
) AS t(payment_type INT);

-- Remplir la dimension Time
INSERT INTO snowflake.dim_time (date, year, month, day, hour, minute)
SELECT DISTINCT
    DATE(tpep_pickup_datetime),
    EXTRACT(YEAR FROM tpep_pickup_datetime),
    EXTRACT(MONTH FROM tpep_pickup_datetime),
    EXTRACT(DAY FROM tpep_pickup_datetime),
    EXTRACT(HOUR FROM tpep_pickup_datetime),
    EXTRACT(MINUTE FROM tpep_pickup_datetime)
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT tpep_pickup_datetime FROM mon_schema.yellow_tripdata'
) AS t(tpep_pickup_datetime TIMESTAMP);

-- Remplir la table factuelle
INSERT INTO snowflake.rides_fact (
    pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
    vendor_id, rate_code_id, pickup_location_id, dropoff_location_id, payment_type_id
)
SELECT 
    tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
    COALESCE(vendorid, -1), -- Utilise -1 pour les valeurs NULL
    COALESCE(ratecodeid, -1), -- Utilise -1 pour les valeurs NULL
    COALESCE(pulocationid, -1), -- Utilise -1 pour les valeurs NULL
    COALESCE(dolocationid, -1), -- Utilise -1 pour les valeurs NULL
    COALESCE(payment_type, -1) -- Utilise -1 pour les valeurs NULL
FROM dblink(
    'data_warehouse_conn',
    'SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
            fare_amount, extra, mta_tax, tip_amount, tolls_amount,
            improvement_surcharge, total_amount, congestion_surcharge, airport_fee,
            vendorid, ratecodeid, pulocationid, dolocationid, payment_type
     FROM mon_schema.yellow_tripdata'
) AS t(
    tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count INT, trip_distance FLOAT,
    fare_amount FLOAT, extra FLOAT, mta_tax FLOAT, tip_amount FLOAT, tolls_amount FLOAT,
    improvement_surcharge FLOAT, total_amount FLOAT, congestion_surcharge FLOAT, airport_fee FLOAT,
    vendorid INT, ratecodeid INT, pulocationid INT, dolocationid INT, payment_type INT
);
