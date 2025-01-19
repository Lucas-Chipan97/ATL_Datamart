drop schema mon_shema cascade;

create schema if not exists mon_schema;

CREATE TABLE mon_schema.yellow_tripdata (
    id SERIAL PRIMARY KEY, -- Clé primaire auto-incrémentée
    VendorID INT NOT NULL, -- ID du fournisseur de services


    tpep_pickup_datetime TIMESTAMP NOT NULL, -- Date et heure de prise en charge
    tpep_dropoff_datetime TIMESTAMP NOT NULL, -- Date et heure de dépose
    passenger_count INT NOT NULL, -- Nombre de passagers
    trip_distance NUMERIC(10, 2) NOT NULL, -- Distance du trajet
    RatecodeID INT NOT NULL, -- Code tarifaire
    /*
    */
    store_and_fwd_flag CHAR(1), -- Indicateur de transfert de données en magasin ('Y' ou 'N')
    /*
    */
    PULocationID INT NOT NULL, -- ID de l'emplacement de prise en charge
    DOLocationID INT NOT NULL, -- ID de l'emplacement de dépose
    /*
    */
    payment_type INT NOT NULL, -- Type de paiement
    /*
    */
    fare_amount NUMERIC(10, 2) NOT NULL, -- Montant de la course
    extra NUMERIC(10, 2) NOT NULL, -- Suppléments
    mta_tax NUMERIC(10, 2) NOT NULL, -- Taxe MTA
    tip_amount NUMERIC(10, 2) NOT NULL, -- Pourboire
    tolls_amount NUMERIC(10, 2) NOT NULL, -- Montant des péages
    improvement_surcharge NUMERIC(10, 2) NOT NULL, -- Supplément d'amélioration
    total_amount NUMERIC(10, 2) NOT NULL, -- Montant total
    congestion_surcharge NUMERIC(10, 2) NOT NULL, -- Supplément pour congestion
    airport_fee  NUMERIC(10, 2) NOT NULL
    /*
    */
);

