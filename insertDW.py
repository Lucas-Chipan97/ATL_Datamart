import psycopg2
import pandas as pd

# Fonction de connexion à PostgreSQL avec encodage UTF-8
def connect_to_db():
    try:
        return psycopg2.connect(
            dbname="data_warehouse",  # Remplacez par le nom de votre base
            user="postgres",         # Remplacez par votre utilisateur
            password="admin",        # Remplacez par votre mot de passe
            host="localhost",        # Hôte de la base
            port="15432"             # Port PostgreSQL
        )
    except Exception as e:
        print("Erreur de connexion à la base de données :", e)
        exit()

# Fonction pour charger le fichier Parquet et insérer les données (limitées aux 100 premières lignes)
def load_parquet_to_db(file_path):
    conn = None
    cur = None
    try:
        # Chargement du fichier Parquet avec pandas
        print("Chargement du fichier Parquet...")
        df = pd.read_parquet(file_path, engine='pyarrow')
        print("Fichier Parquet chargé avec succès !")

        # Limiter les données aux 100 premières lignes
        df = df.head(1000)
        print(f"Limité à {len(df)} lignes pour l'insertion.")

        # Connexion à la base
        conn = connect_to_db()
        cur = conn.cursor()

        # Vérification des valeurs maximales et minimales dans les colonnes numériques
        print("Valeurs maximales dans les colonnes numériques :")
        print(df[['VendorID', 'RatecodeID', 'passenger_count', 'PULocationID', 'DOLocationID', 'payment_type']].max())

        print("Valeurs minimales dans les colonnes numériques :")
        print(df[['VendorID', 'RatecodeID', 'passenger_count', 'PULocationID', 'DOLocationID', 'payment_type']].min())

        # Insertion des données dans la table
        insert_query = """
            INSERT INTO mon_schema.yellow_tripdata (
                VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
                PULocationID, DOLocationID, payment_type, fare_amount, extra,
                mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                total_amount, congestion_surcharge, airport_fee
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        for _, row in df.iterrows():
            cur.execute(insert_query, (
                row['VendorID'], row['tpep_pickup_datetime'], row['tpep_dropoff_datetime'],
                row['passenger_count'], row['trip_distance'], row['RatecodeID'], row['store_and_fwd_flag'],
                row['PULocationID'], row['DOLocationID'], row['payment_type'], row['fare_amount'], row['extra'],
                row['mta_tax'], row['tip_amount'], row['tolls_amount'], row['improvement_surcharge'],
                row['total_amount'], row['congestion_surcharge'], row['airport_fee']
            ))

        conn.commit()
        print("Données insérées avec succès dans la table !")

    except Exception as e:
        print("Une erreur s'est produite lors de l'insertion des données :", e)

    finally:
        # Fermeture sécurisée
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    # Chemin vers le fichier Parquet
    parquet_file_path = r"C:/Users/lucas/OneDrive/Desktop/EPSI/Architecture_des_données/TP1/Part 1/yellow_tripdata_2023-01.parquet"

    # Charger et insérer les données (limitées aux 100 premières lignes) dans la table
    load_parquet_to_db(parquet_file_path)
