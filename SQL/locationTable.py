import psycopg2
import csv
import os

# Paramètres de connexion à la base de données PostgreSQL
conn_params = {
    "host": "localhost",  # Remplacez par l'adresse de votre serveur PostgreSQL
    "dbname": "datamart",  # Remplacez par le nom de votre base de données
    "user": "postgres",  # Remplacez par votre nom d'utilisateur PostgreSQL
    "password": "admin",  # Remplacez par votre mot de passe PostgreSQL
    "port": 15435  # Le port par défaut de PostgreSQL
}

# Chemin du fichier CSV à importer
csv_file_path = "C:/Users/lucas/OneDrive/Desktop/EPSI/Architecture_des_données/TP1/Part 1/SQL/taxi_zone_lookup.csv"

# Fonction pour vérifier si le fichier existe
def check_file_exists(file_path):
    return os.path.isfile(file_path)

# Fonction pour insérer les données dans la table temp_location
def insert_data_into_temp_location(file_path):
    # Vérifier si le fichier existe
    if not check_file_exists(file_path):
        print("Fichier non trouvé.")
        return
    
    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    # Création de la table temporaire si elle n'existe pas
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS location (
            location_id INT,
            borough TEXT,
            zone TEXT,
            service_zone TEXT
        );
    """)

    # Ouvrir le fichier CSV et insérer les données ligne par ligne
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Ignorer la ligne d'en-tête
        for row in reader:
            cur.execute("""
                INSERT INTO location (location_id, borough, zone, service_zone)
                VALUES (%s, %s, %s, %s)
            """, (row[0], row[1], row[2], row[3]))

    # Commit des changements
    conn.commit()

    # Fermeture de la connexion
    cur.close()
    conn.close()

    print("Données insérées dans temp_location avec succès.")

# Appeler la fonction pour insérer les données
insert_data_into_temp_location(csv_file_path)
