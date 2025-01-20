CREATE TEMP TABLE temp_location (
                                        location_id INT,
                                        borough TEXT,
                                        zone TEXT,
                                        service_zone TEXT
                                        );

\COPY temp_location(location_id, borough, zone, service_zone)
FROM '/tmp/taxi_zone_lookup.csv'
DELIMITER ',' CSV HEADER;
COPY 265

INSERT INTO dim_location(location_id, borough, zone, service_zone)
SELECT location_id, borough, zone, service_zone
FROM temp_location
ON CONFLICT (location_id)
DO UPDATE SET 
    borough = EXCLUDED.borough,
    zone = EXCLUDED.zone,
    service_zone = EXCLUDED.service_zone;
INSERT @ 245