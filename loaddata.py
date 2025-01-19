import os
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


def main():
    load_data_to_postgres()

# Charger les variables d'environnement
load_dotenv()

# Paramètres Minio
bucket_name = "daminio"
expected_files = ["yellow_tripdata_2023-01.parquet"]
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
    'port': 15432
}

# Connexion PostgreSQL
def create_postgres_engine():
    return create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

# Télécharger le fichier depuis Minio
def download_from_minio():
    for file_name in expected_files:
        try:
            if minio_client.stat_object(bucket_name, file_name):
                print(f"Le fichier {file_name} existe dans le bucket.")
                minio_client.fget_object(bucket_name, file_name, file_name)
                print(f"Le fichier {file_name} a été téléchargé avec succès.")
                return file_name
        except Exception as e:
            print(f"Erreur lors de l'accès au fichier {file_name}: {e}")
            return None

# Mappe les types Pandas vers les types PostgreSQL
def map_dtype_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "TEXT"

# Créer dynamiquement une table PostgreSQL à partir d'un DataFrame
def create_table_from_dataframe(engine, schema_name, table_name, df):
    columns = [
        f"{col} {map_dtype_to_postgres(dtype)}"
        for col, dtype in zip(df.columns, df.dtypes)
    ]
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {', '.join(columns)}
        );
    """
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name};"))
        conn.execute(text(create_table_query))
        print(f"Table '{schema_name}.{table_name}' créée avec succès.")

# Insérer des données dans la table PostgreSQL
def insert_data_to_table(engine, schema_name, table_name, df):
    try:
        df.to_sql(table_name, engine, schema=schema_name, if_exists="append", index=False)
        print(f"Données insérées avec succès dans la table '{schema_name}.{table_name}'.")
    except Exception as e:
        print(f"Erreur lors de l'insertion des données : {e}")

# Charger les données dans PostgreSQL
def load_data_to_postgres(file_name):
    if file_name:
        # Lire le fichier Parquet
        df = pd.read_parquet(file_name)
        print(f"Lecture du fichier {file_name} réussie. Nombre de lignes : {len(df)}")

        # Connexion PostgreSQL
        engine = create_postgres_engine()
        schema_name = "espi"
        table_name = "taxis_data"

        # Créer la table et insérer les données
        create_table_from_dataframe(engine, schema_name, table_name, df)
        insert_data_to_table(engine, schema_name, table_name, df)

        # Nettoyage
        os.remove(file_name)
        print(f"Fichier {file_name} supprimé après importation.")
    else:
        print("Aucun fichier trouvé pour l'importation.")

# Exécution principale
def main():
    file_name = download_from_minio()
    if file_name:
        load_data_to_postgres(file_name)
    else:
        print("Aucun fichier trouvé pour l'importation.")

if __name__ == "__main__":
    main()
