drop schema snowflake cascade;
create schema snowflake;

-- Table des fournisseurs
CREATE TABLE IF NOT EXISTS snowflake.dim_vendor (
    VendorID BIGINT PRIMARY KEY,
    vendor_name VARCHAR(255) -- Exemple de colonne supplémentaire
);


-- Table des pays
CREATE TABLE IF NOT EXISTS snowflake.dim_country (
    country_id BIGINT PRIMARY KEY,
    country_name VARCHAR(255)
);


-- Table des régions
CREATE TABLE IF NOT EXISTS snowflake.dim_region (
    region_id BIGINT PRIMARY KEY,
    region_name VARCHAR(255),
    country_id BIGINT REFERENCES snowflake.dim_country(country_id) -- Référence vers les pays
);


-- Table des localisations
CREATE TABLE IF NOT EXISTS snowflake.dim_location (
    location_id BIGINT PRIMARY KEY,
    location_name VARCHAR(255),
    region_id BIGINT REFERENCES snowflake.dim_region(region_id) -- Référence vers les régions
);


-- Table des codes de tarif
CREATE TABLE IF NOT EXISTS snowflake.dim_ratecode (
    RatecodeID BIGINT PRIMARY KEY,
    rate_description VARCHAR(255)
);

-- Table des types de paiement
CREATE TABLE IF NOT EXISTS snowflake.dim_payment_type (
    payment_type BIGINT PRIMARY KEY,
    payment_method VARCHAR(255) -- Exemple : "Carte", "Espèces"
);

-- Table des trimestres
CREATE TABLE IF NOT EXISTS snowflake.dim_quarter (
    quarter_id BIGINT PRIMARY KEY,
    quarter_name VARCHAR(255),
    year INT
);

-- Table des mois
CREATE TABLE IF NOT EXISTS snowflake.dim_month (
    month_id BIGINT PRIMARY KEY,
    month_name VARCHAR(255),
    quarter_id BIGINT REFERENCES snowflake.dim_quarter(quarter_id) -- Référence vers les trimestres
);

-- Table de temps
CREATE TABLE IF NOT EXISTS snowflake.dim_time (
    time_key INT PRIMARY KEY,
    date DATE, -- Ajout pour stocker la date exacte
    day_of_week INT, -- Jour de la semaine (1-7)
    hour INT, -- Heure de la journée
    month_id BIGINT REFERENCES snowflake.dim_month(month_id) -- Référence vers les mois
);

-- Table des montants
CREATE TABLE IF NOT EXISTS snowflake.dim_amount (
    amount_id BIGSERIAL PRIMARY KEY, -- Identifiant unique pour chaque enregistrement
    fare_amount NUMERIC(10, 2) NOT NULL, -- Montant de la course
    extra NUMERIC(10, 2) NOT NULL, -- Suppléments
    mta_tax NUMERIC(10, 2) NOT NULL, -- Taxe MTA
    tip_amount NUMERIC(10, 2) NOT NULL, -- Pourboire
    tolls_amount NUMERIC(10, 2) NOT NULL, -- Montant des péages
    improvement_surcharge NUMERIC(10, 2) NOT NULL, -- Supplément d'amélioration
    total_amount NUMERIC(10, 2) NOT NULL, -- Montant total
    congestion_surcharge NUMERIC(10, 2) NOT NULL, -- Supplément pour congestion
    airport_fee NUMERIC(10, 2) NOT NULL -- Taxe aéroportuaire
);


-- Table de faits (données de trajets)
CREATE TABLE IF NOT EXISTS snowflake.fact_tripdata (
    trip_id BIGSERIAL PRIMARY KEY,
    vendor_id BIGINT REFERENCES snowflake.dim_vendor(VendorID),
    pickup_time_key INT REFERENCES snowflake.dim_time(time_key),
    dropoff_time_key INT REFERENCES snowflake.dim_time(time_key),
    passenger_count BIGINT,
    trip_distance FLOAT,
    ratecode_id BIGINT REFERENCES snowflake.dim_ratecode(RatecodeID),
    store_and_fwd_flag VARCHAR(10),
    pulocation_id BIGINT REFERENCES snowflake.dim_location(location_id),
    dolocation_id BIGINT REFERENCES snowflake.dim_location(location_id),
    payment_type BIGINT REFERENCES snowflake.dim_payment_type(payment_type),
    amount_id BIGINT REFERENCES snowflake.dim_amount(amount_id) -- Référence à la table des montants
);
