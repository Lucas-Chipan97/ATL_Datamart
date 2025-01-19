
create extension if not exists dblink;
SELECT dblink_connect('data_warehouse_conn', 'host=data-warehouse port=5432 dbname=data_warehouse user=postgres password=admin');        



-- Insérer les données dans la dimension Vendor
INSERT INTO snowflake.dim_vendor (VendorID, vendor_name)
SELECT DISTINCT 
    COALESCE(VendorID, -1), -- Utilisation de -1 pour les valeurs NULL
    'Unknown Vendor' -- Description par défaut
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT mon_schema.yellow_tripdata.VendorID FROM mon_schema.yellow_tripdata'
) AS t(VendorID INT);

-- Insérer les données dans la dimension Rate Code
INSERT INTO snowflake.dim_ratecode (RatecodeID, rate_description)
SELECT DISTINCT 
    COALESCE(RatecodeID, -1), -- Utilisation de -1 pour les valeurs NULL
    'Unknown Rate Code' -- Description par défaut
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT mon_schema.yellow_tripdata.RatecodeID FROM mon_schema.yellow_tripdata'
) AS t(RatecodeID INT);

-- Insérer les données dans la dimension Location (emplacements de prise en charge et de dépose)
INSERT INTO snowflake.dim_location (location_id, location_name, region_id)
SELECT DISTINCT 
    COALESCE(PULocationID, -1), 
    'Unknown Location', 
    NULL -- Il est possible d’ajouter une région si des données sont disponibles
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT yellow_tripdata.PULocationID FROM mon_schema.yellow_tripdata'
) AS t(PULocationID INT)
UNION
SELECT DISTINCT 
    COALESCE(DOLocationID, -1), 
    'Unknown Location', 
    NULL -- Il est possible d’ajouter une région si des données sont disponibles
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT yellow_tripdata.DOLocationID FROM mon_schema.yellow_tripdata'
) AS t(DOLocationID INT);

-- Insérer les données dans la dimension Payment Type (type de paiement)
INSERT INTO snowflake.dim_payment_type (payment_type, payment_method)
SELECT DISTINCT 
    COALESCE(payment_type, -1), -- Utilisation de -1 pour les valeurs NULL
    'Unknown Payment' -- Description par défaut
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT yellow_tripdata.payment_type FROM mon_schema.yellow_tripdata'
) AS t(payment_type INT);

-- Insérer les données dans la dimension Time (table temporelle)
INSERT INTO snowflake.dim_time (time_key, date, day_of_week, hour, month_id)
SELECT DISTINCT
    EXTRACT(EPOCH FROM tpep_pickup_datetime)::INT, -- Conversion de la date/heure en clé de temps
    DATE(tpep_pickup_datetime), -- Date seulement
    EXTRACT(DAY FROM tpep_pickup_datetime) % 7 + 1, -- Jour de la semaine (1-7)
    EXTRACT(HOUR FROM tpep_pickup_datetime), -- Heure de la journée
    COALESCE(MONTH_ID, -1) -- Utilisation de -1 si mois non défini
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT yellow_tripdata.tpep_pickup_datetime FROM mon_schema.yellow_tripdata'
) AS t(tpep_pickup_datetime TIMESTAMP);

-- Insérer les données dans la dimension Quarter (Trimestre)
INSERT INTO snowflake.dim_quarter (quarter_id, quarter_name, year)
SELECT DISTINCT
    EXTRACT(QUARTER FROM tpep_pickup_datetime) AS quarter_id, 
    'Q' || EXTRACT(QUARTER FROM tpep_pickup_datetime) AS quarter_name,
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS year
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT yellow_tripdata.tpep_pickup_datetime FROM mon_schema.yellow_tripdata'
) AS t(tpep_pickup_datetime TIMESTAMP);

-- Insérer les données dans la dimension Month (Mois)
INSERT INTO snowflake.dim_month (month_id, month_name, quarter_id)
SELECT DISTINCT
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS month_id,
    TO_CHAR(tpep_pickup_datetime, 'FMMonth') AS month_name, -- Format pour le mois (ex: "January")
    EXTRACT(QUARTER FROM tpep_pickup_datetime) AS quarter_id
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT yellow_tripdata.tpep_pickup_datetime FROM mon_schema.yellow_tripdata'
) AS t(tpep_pickup_datetime TIMESTAMP);

-- Insérer les données dans la table des montants (dim_amount)
INSERT INTO snowflake.dim_amount (
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, total_amount, congestion_surcharge, airport_fee
)
SELECT 
    fare_amount, 
    extra, 
    mta_tax, 
    tip_amount, 
    tolls_amount,
    improvement_surcharge, 
    total_amount, 
    congestion_surcharge, 
    airport_fee
FROM dblink(
    'data_warehouse_conn',
    'SELECT yellow_tripdata.fare_amount, yellow_tripdata.extra, yellow_tripdata.mta_tax, yellow_tripdata.tip_amount, 
    yellow_tripdata.tolls_amount, yellow_tripdata.improvement_surcharge, yellow_tripdata.total_amount, 
    yellow_tripdata.congestion_surcharge, yellow_tripdata.airport_fee
     FROM mon_schema.yellow_tripdata'
) AS t(
    fare_amount NUMERIC(10, 2), 
    extra NUMERIC(10, 2), 
    mta_tax NUMERIC(10, 2), 
    tip_amount NUMERIC(10, 2), 
    tolls_amount NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2), 
    total_amount NUMERIC(10, 2), 
    congestion_surcharge NUMERIC(10, 2), 
    airport_fee NUMERIC(10, 2)
);

-- Insérer les données dans la table de faits (fact_tripdata)
INSERT INTO snowflake.fact_tripdata (
    trip_id, vendor_id, pickup_time_key, dropoff_time_key, passenger_count, 
    trip_distance, ratecode_id, store_and_fwd_flag, pulocation_id, dolocation_id, 
    payment_type, amount_id
)
SELECT 
    id, -- ID unique du trajet
    COALESCE(VendorID, -1), -- ID fournisseur, avec valeur par défaut
    EXTRACT(EPOCH FROM tpep_pickup_datetime)::INT, -- Clé de temps pour le début
    EXTRACT(EPOCH FROM tpep_dropoff_datetime)::INT, -- Clé de temps pour la fin
    passenger_count, 
    trip_distance,
    COALESCE(RatecodeID, -1), -- Code tarifaire, avec valeur par défaut
    COALESCE(store_and_fwd_flag, 'N'), -- Flag de transfert de données (par défaut 'N')
    COALESCE(PULocationID, -1), -- Location de prise en charge, avec valeur par défaut
    COALESCE(DOLocationID, -1), -- Location de dépose, avec valeur par défaut
    COALESCE(payment_type, -1), -- Type de paiement, avec valeur par défaut
    COALESCE(amount_id, -1) -- Montant, avec valeur par défaut
FROM dblink(
    'data_warehouse_conn',
    'SELECT id, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
            RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type
     FROM mon_schema.yellow_tripdata'
) AS t(
    id INT, 
    VendorID INT, 
    tpep_pickup_datetime TIMESTAMP, 
    tpep_dropoff_datetime TIMESTAMP, 
    passenger_count INT, 
    trip_distance NUMERIC, 
    RatecodeID INT, 
    store_and_fwd_flag CHAR(1), 
    PULocationID INT, 
    DOLocationID INT, 
    payment_type INT
);
