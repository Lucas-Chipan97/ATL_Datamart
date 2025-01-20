-- Mise à jour de la description des codes de tarifs
UPDATE snowflake.dim_rate_code
SET rate_code_description = 
    CASE rate_code_id
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
    END
WHERE rate_code_description = 'Unknown Rate Code';

-- Insertion de nouvelles données dans la table des timestamps (avec gestion des conflits)
INSERT INTO snowflake.dim_time (date, year, month, day, hour, minute)
VALUES
    ('2024-12-10', 2024,12,10,14,30),
    ('2024-12-11', 2024,12,11,9,15),
    ('2024-12-12', 2024,12,12,16,45)
ON CONFLICT (time_id) DO NOTHING;  -- Ignorer si l'enregistrement existe déjà

-- Insertion de nouveaux fournisseurs (avec mise à jour en cas de conflit)
INSERT INTO snowflake.dim_vendor (vendor_id, vendor_name)
VALUES
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc.')
ON CONFLICT (vendor_id) 
DO UPDATE SET vendor_name = EXCLUDED.vendor_name;  -- Mettre à jour le nom si le fournisseur existe déjà

-- Mise à jour de la description des types de paiement
UPDATE snowflake.dim_payment
SET payment_description = 
    CASE payment_type_id
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'  -- Garder 'Unknown' pour certaines valeurs
        WHEN 6 THEN 'Voided trip'
        ELSE 'Unknown Payment'  -- Par défaut, pour toute autre valeur inattendue
    END
WHERE payment_description = 'Unknown Payment';



-- Insertion des données de la table temporaire dans la table principale "dim_location".
-- En cas de conflit sur la colonne "location_id" (clé primaire), les données existantes sont mises à jour.
/*
*
*
*
*/
INSERT INTO snowflake.dim_location (location_id, borough, zone, service_zone)
SELECT location_id, borough, zone, service_zone
FROM snowflake.location
ON CONFLICT (location_id) DO UPDATE
SET
    borough = EXCLUDED.borough,
    zone = EXCLUDED.zone,
    service_zone = EXCLUDED.service_zone;