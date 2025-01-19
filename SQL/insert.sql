SELECT dblink_connect('data_warehouse_conn', 'host=data-warehouse port=5432 dbname=data_warehouse user=postgres password=admin');        

-- Insertion dans la table dim_country
INSERT INTO snowflake.dim_country (country_id, country_name)
SELECT DISTINCT 
    COALESCE(country_id, 0), 
    COALESCE(country_name, '')
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT country_id, country_name FROM yellow_tripdata')
AS t(country_id INT, country_name VARCHAR(255));

-- Insertion dans la table dim_region
INSERT INTO snowflake.dim_region (region_id, region_name, country_id)
SELECT DISTINCT 
    COALESCE(region_id, 0), 
    COALESCE(region_name, ''), 
    COALESCE(country_id, 0)
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT region_id, region_name, country_id FROM yellow_tripdata')
AS t(region_id INT, region_name VARCHAR(255), country_id INT);

-- Insertion dans la table dim_location
INSERT INTO snowflake.dim_location (location_id, location_name, region_id)
SELECT DISTINCT 
    COALESCE(location_id, 0), 
    COALESCE(location_name, ''), 
    COALESCE(region_id, 0)
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT location_id, location_name, region_id FROM yellow_tripdata')
AS t(location_id INT, location_name VARCHAR(255), region_id INT);

-- Insertion dans la table dim_vendor
INSERT INTO snowflake.dim_vendor (VendorID, vendor_name)
SELECT DISTINCT 
    COALESCE(VendorID, 0), 
    COALESCE(vendor_name, '')
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT VendorID, vendor_name FROM yellow_tripdata')
AS t(VendorID INT, vendor_name VARCHAR(255));

-- Insertion dans la table dim_ratecode
INSERT INTO snowflake.dim_ratecode (RatecodeID, rate_description)
SELECT DISTINCT 
    COALESCE(RatecodeID, 0), 
    COALESCE(rate_description, '')
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT RatecodeID, rate_description FROM yellow_tripdata')
AS t(RatecodeID INT, rate_description VARCHAR(255));

-- Insertion dans la table dim_payment_type
INSERT INTO snowflake.dim_payment_type (payment_type, payment_method)
SELECT DISTINCT 
    COALESCE(payment_type, 0), 
    COALESCE(payment_method, '')
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT payment_type, payment_method FROM yellow_tripdata')
AS t(payment_type INT, payment_method VARCHAR(255));

-- Insertion dans la table dim_quarter

INSERT INTO snowflake.dim_quarter (quarter_id, quarter_name, year)
SELECT DISTINCT 
    COALESCE(quarter_id, 0), 
    COALESCE(quarter_name, ''), 
    COALESCE(year, 0)
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT quarter_id, quarter_name, year FROM yellow_tripdata')
AS t(quarter_id INT, quarter_name VARCHAR(255), year INT);



-- Insertion dans la table dim_month
INSERT INTO snowflake.dim_month (month_id, month_name, quarter_id)
SELECT DISTINCT 
    COALESCE(month_id, 0), 
    COALESCE(month_name, ''), 
    COALESCE(quarter_id, 0)
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT month_id, month_name, quarter_id FROM yellow_tripdata')
AS t(month_id INT, month_name VARCHAR(255), quarter_id INT);


-- Insertion dans la table dim_time
INSERT INTO snowflake.dim_time (time_key, date, day_of_week, hour, month_id)
SELECT DISTINCT 
    COALESCE(time_key, 0), 
    COALESCE(date, '1970-01-01'::DATE), -- Valeur par défaut de la date si elle est manquante
    COALESCE(day_of_week, 0), 
    COALESCE(hour, 0), 
    COALESCE(month_id, 0)
FROM dblink('data_warehouse_conn', 'SELECT DISTINCT time_key, date, day_of_week, hour, month_id FROM yellow_tripdata')
AS t(time_key INT, date DATE, day_of_week INT, hour INT, month_id INT);

-- Insertion dans la table fact_tripdata
INSERT INTO snowflake.fact_tripdata (
    vendor_id,
    pickup_time_key,
    dropoff_time_key,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pulocation_id,
    dolocation_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge
)
SELECT 
    COALESCE(vendor_id, 0),
    COALESCE(pickup_time_key, 0),
    COALESCE(dropoff_time_key, 0),
    COALESCE(passenger_count, 0),
    COALESCE(trip_distance, 0),
    COALESCE(ratecode_id, 0),
    COALESCE(store_and_fwd_flag, ''),
    COALESCE(pulocation_id, 0),
    COALESCE(dolocation_id, 0),
    COALESCE(payment_type, 0),
    COALESCE(fare_amount, 0),
    COALESCE(extra, 0),
    COALESCE(mta_tax, 0),
    COALESCE(tip_amount, 0),
    COALESCE(tolls_amount, 0),
    COALESCE(improvement_surcharge, 0),
    COALESCE(total_amount, 0),
    COALESCE(congestion_surcharge, 0)
FROM dblink('data_warehouse_conn', 
    'SELECT vendor_id, pickup_time_key, dropoff_time_key, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pulocation_id, dolocation_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge FROM yellow_tripdata')
AS t(
    vendor_id INT,
    pickup_time_key INT,
    dropoff_time_key INT,
    passenger_count INT,
    trip_distance NUMERIC,
    ratecode_id INT,
    store_and_fwd_flag CHAR(1),
    pulocation_id INT,
    dolocation_id INT,
    payment_type INT,
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    total_amount NUMERIC,
    congestion_surcharge NUMERIC
);
