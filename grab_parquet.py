from minio import Minio
import urllib.request
import os
import re
import sys

def main():
    grab_data()
    grab_data_last_date()
    write_data_minio()
    verify_files_in_minio()


def grab_data() -> None:
    """
    Grab the data from New York Yellow Taxi for January 2023 to August 2023.

    Downloads Parquet files from the specified date range.
    Files are saved into C:/Users/lucas/OneDrive/Desktop/EPSI/Architecture_des_données/TP1/Part 1/.
    """
    base_url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    download_dir = "C:/Users/lucas/OneDrive/Desktop/EPSI/Architecture_des_données/TP1/Part 1"
    os.makedirs(download_dir, exist_ok=True)

    try:
        # Fetch the webpage content
        with urllib.request.urlopen(base_url) as response:
            html_content = response.read().decode('utf-8')  # Decode to string for regex

        # Regex to find all 'a' tags with href attribute
        link_pattern = re.compile(r'<a\s+(?:[^>]*?\s+)?href="([^"]+)"')
        all_links = link_pattern.findall(html_content)

        # Regex to match the specific parquet files for 2023-01 to 2023-08
        file_pattern = re.compile(r'yellow_tripdata_2023-(0[1-8])\.parquet$')

        # Generate complete URLs for matching links
        parquet_links = [
            urllib.parse.urljoin(base_url, link)
            for link in all_links if file_pattern.search(link)
        ]

        if not parquet_links:
            print("No matching Parquet files found.")
            return

        # Download each Parquet file
        for link in parquet_links:
            file_name = os.path.basename(link)
            file_path = os.path.join(download_dir, file_name)

            print(f"Downloading {file_name}...")
            urllib.request.urlretrieve(link, file_path)
            print(f"Saved to {file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")

def grab_data_last_date() -> None:
    """
    Grab the latest Parquet file available on the website.

    Downloads the file to the specified directory.
    """
    base_url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    download_dir = "C:/Users/lucas/OneDrive/Desktop/EPSI/Architecture_des_données/TP1/Part 1"
    os.makedirs(download_dir, exist_ok=True)

    try:
        # Fetch the webpage content
        with urllib.request.urlopen(base_url) as response:
            html_content = response.read().decode('utf-8')  # Decode to string for regex

        # Regex to find all 'a' tags with href attribute
        link_pattern = re.compile(r'<a\s+(?:[^>]*?\s+)?href="([^"]+)"')
        all_links = link_pattern.findall(html_content)

        # Regex to match all Parquet files
        file_pattern = re.compile(r'yellow_tripdata_202\d-\d{2}\.parquet$')

        # Filter and sort Parquet files by date (descending order)
        parquet_files = sorted(
            (urllib.parse.urljoin(base_url, link) for link in all_links if file_pattern.search(link)),
            key=lambda x: file_pattern.search(x).group(),
            reverse=True
        )

        if not parquet_files:
            print("No Parquet files found.")
            return

        # Get the latest file
        latest_file = parquet_files[0]
        file_name = os.path.basename(latest_file)
        file_path = os.path.join(download_dir, file_name)

        print(f"Downloading the latest file: {file_name}...")
        urllib.request.urlretrieve(latest_file, file_path)
        print(f"Saved to {file_path}")

    except Exception as e:
        print(f"An error occurred: {e}")


def write_data_minio():
    """
    Upload a Parquet file to MinIO and ensure the bucket exists.
    The file path and bucket name are hardcoded inside the function.
    """
    # Configuration
    file_path = "C:/Users/lucas/OneDrive/Desktop/EPSI/Architecture_des_données/TP1/Part 1/yellow_tripdata_2023-01.parquet"  # Replace with the actual file path
    bucket = "daminio"         
    # Bucket name in MinIO

    # Initialize MinIO client
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    # Ensure the bucket exists
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created.")
    else:
        print(f"Bucket '{bucket}' already exists.")

    # Upload the file
    if os.path.exists(file_path):
        file_name = os.path.basename(file_path)
        client.fput_object(bucket, file_name, file_path)
        print(f"File '{file_name}' uploaded to bucket '{bucket}'.")
    else:
        print(f"File '{file_path}' does not exist.")

def verify_files_in_minio():
    """
    Verify that specific files are present in a MinIO bucket.
    """
    bucket = "daminio"
    expected_files = ["yellow_tripdata_2023-01.parquet"]  # Replace with the list of files to verify

    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    if not client.bucket_exists(bucket):
        print(f"Bucket '{bucket}' does not exist.")
        return

    # List all objects in the bucket
    objects = [obj.object_name for obj in client.list_objects(bucket)]

    # Check for expected files
    for file in expected_files:
        if file in objects:
            print(f"File '{file}' is present in bucket '{bucket}'.")
        else:
            print(f"File '{file}' is missing from bucket '{bucket}'.")
if __name__ == "__main__":
    main()


