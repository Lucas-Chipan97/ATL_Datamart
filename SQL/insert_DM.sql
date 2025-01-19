-- Activer l'extension dblink si elle n'est pas déjà activée
CREATE EXTENSION IF NOT EXISTS dblink;

-- Connecter à la base de données source via dblink
SELECT dblink_connect(
    'data_warehouse_conn',
    'host=data-warehouse port=5432 dbname=data_warehouse user=postgres password=admin'
);

-- =======================================================
-- Étape 1 : Remplir les dimensions avec des valeurs par défaut ou manquantes
-- =======================================================

-- Remplir dim_region avec une valeur par défaut pour éviter les violations de clé étrangère
INSERT INTO snowflake.dim_region (region_id, region_name)
VALUES (-1, 'Unknown Region')
ON CONFLICT (region_id) DO NOTHING;

-- Remplir dim_month avec les mois manquants pour éviter les violations de clé étrangère
INSERT INTO snowflake.dim_month (month_id, month_name, quarter_id)
SELECT month_id, TO_CHAR(TO_DATE(month_id::text, 'MM'), 'FMMonth'), EXTRACT(QUARTER FROM TO_DATE(month_id::text, 'MM'))
FROM (
    SELECT DISTINCT EXTRACT(MONTH FROM TIMESTAMP '2024-01-01' + (s || ' days')::INTERVAL) AS month_id
    FROM generate_series(0, 364) s
) months
WHERE month_id NOT IN (SELECT month_id FROM dim_month);

-- Remplir dim_time avec les clés temporelles manquantes
INSERT INTO snowflake.dim_time (time_key, date, year, month, day, hour, minute)
VALUES 
    (1672533130, '2023-12-31', 2023, 12, 31, 14, 30),
    (1672533190, '2023-12-31', 2023, 12, 31, 14, 31) -- Ajoutez d'autres clés si nécessaire
ON CONFLICT (time_key) DO NOTHING;

-- Remplir dim_location avec les données manquantes ou défauts depuis un fichier CSV

CREATE TEMP TABLE temp_location (
    location_id INT,
    borough TEXT,
    zone TEXT,
    service_zone TEXT
);
COPY temp_location(location_id, borough, zone, service_zone)
FROM '/tmp/taxi_zone_lookup.csv'
DELIMITER ',' CSV HEADER;

INSERT INTO snowflake.dim_location (location_id, borough, zone, service_zone)
SELECT location_id, borough, zone, service_zone
FROM temp_location
ON CONFLICT (location_id) DO UPDATE SET
    borough = EXCLUDED.borough,
    zone = EXCLUDED.zone,
    service_zone = EXCLUDED.service_zone;

-- =======================================================
-- Étape 2 : Remplir les dimensions principales depuis la source
-- =======================================================

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
    COALESCE(PULocationID, -1), -- Si aucune location ID, utilisez -1
    'Unknown Location', 
    -1 -- Indique qu'aucune région n'est définie
FROM dblink(
    'data_warehouse_conn',
    'SELECT DISTINCT yellow_tripdata.PULocationID FROM mon_schema.yellow_tripdata'
) AS t(PULocationID INT)
UNION
SELECT DISTINCT 
    COALESCE(DOLocationID, -1), 
    'Unknown Location', 
    -1 -- Même logique pour les régions inconnues
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

-- =======================================================
-- Étape 3 : Remplir la table de faits avec des clés cohérentes
-- =======================================================

-- Insérer les données dans la table de faits
INSERT INTO snowflake.fact_tripdata (
    trip_id, vendor_id, pickup_time_key, dropoff_time_key, passenger_count, 
    trip_distance, ratecode_id, store_and_fwd_flag, pulocation_id, dolocation_id, 
    payment_type, amount_id
)
SELECT 
    id, -- Identifiant unique du trajet
    COALESCE(VendorID, -1), -- ID du fournisseur (valeur par défaut : -1)
    EXTRACT(EPOCH FROM tpep_pickup_datetime)::INT, -- Clé temporelle pour le début
    EXTRACT(EPOCH FROM tpep_dropoff_datetime)::INT, -- Clé temporelle pour la fin
    passenger_count, -- Nombre de passagers
    trip_distance, -- Distance parcourue
    COALESCE(RatecodeID, -1), -- Code tarifaire
    COALESCE(store_and_fwd_flag, 'N'), -- Indicateur de stockage/transfert (défaut : 'N')
    COALESCE(PULocationID, -1), -- Emplacement de prise en charge
    COALESCE(DOLocationID, -1), -- Emplacement de dépose
    COALESCE(payment_type, -1), -- Type de paiement (défaut : -1)
    -1 -- ID du montant inconnu
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
