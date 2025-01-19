import os
import pandas as pd
from minio import Minio
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Paramètres Minio
bucket_name = "daminio"
file_name = "yellow_tripdata_2023-01.parquet"
minio_client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

# Paramètres PostgreSQL
db_params = {
    'dbname': 'data_warehouse',
    'user': 'postgres',
    'password': 'admin',
    'host': 'localhost',
    'port': '15432'
}

# Télécharger le fichier depuis Minio
def download_file_from_minio():
    try:
        print(f"Téléchargement du fichier '{file_name}' depuis Minio...")
        minio_client.fget_object(bucket_name, file_name, file_name)
        print(f"Fichier '{file_name}' téléchargé avec succès.")
        return file_name
    except Exception as e:
        print(f"Erreur lors du téléchargement : {e}")
        return None

# Connexion PostgreSQL
def connect_to_postgres():
    try:
        return psycopg2.connect(
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host'],
            port=db_params['port']
        )
    except Exception as e:
        print(f"Erreur de connexion à PostgreSQL : {e}")
        return None

# Insérer les données dans PostgreSQL (limitées à 1000 lignes)
def insert_data_to_postgres(file_path, schema_name, table_name):
    conn = None
    cur = None
    try:
        # Lire le fichier Parquet
        print("Chargement du fichier Parquet...")
        df = pd.read_parquet(file_path, engine='pyarrow')
        print(f"Fichier Parquet chargé avec succès ({len(df)} lignes).")

        # Limiter les données aux 1000 premières lignes
        df = df.head(1000)
        print(f"Limitation à {len(df)} lignes pour l'insertion.")

        # Connexion à PostgreSQL
        conn = connect_to_postgres()
        if conn is None:
            return
        cur = conn.cursor()

        # Préparer la requête d'insertion
        insert_query = sql.SQL("""
            INSERT INTO {}.{} (
                VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
                PULocationID, DOLocationID, payment_type, fare_amount, extra,
                mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                total_amount, congestion_surcharge, airport_fee
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name))

        # Insérer les lignes une par une
        for _, row in df.iterrows():
            cur.execute(insert_query, (
                row['VendorID'], row['tpep_pickup_datetime'], row['tpep_dropoff_datetime'],
                row['passenger_count'], row['trip_distance'], row['RatecodeID'], row['store_and_fwd_flag'],
                row['PULocationID'], row['DOLocationID'], row['payment_type'], row['fare_amount'], row['extra'],
                row['mta_tax'], row['tip_amount'], row['tolls_amount'], row['improvement_surcharge'],
                row['total_amount'], row['congestion_surcharge'], row['airport_fee']
            ))

        conn.commit()
        print(f"Données insérées avec succès dans la table '{schema_name}.{table_name}'.")

    except Exception as e:
        print(f"Erreur lors de l'insertion des données : {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

        # Supprimer le fichier local après insertion
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Fichier local '{file_path}' supprimé.")

# Exécution principale
def main():
    # Étape 1 : Télécharger le fichier depuis Minio
    downloaded_file = download_file_from_minio()
    if downloaded_file:
        # Étape 2 : Insérer les données dans PostgreSQL (limité à 1000 lignes)
        schema_name = "mon_schema"  # À personnaliser
        table_name = "yellow_tripdata"  # À personnaliser
        insert_data_to_postgres(downloaded_file, schema_name, table_name)
    else:
        print("Aucun fichier à traiter.")

if __name__ == "__main__":
    main()
