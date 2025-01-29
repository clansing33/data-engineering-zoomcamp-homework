import os
import subprocess

def download_file(url, output_path):
    """
    Downloads a file from the given URL if it doesn't already exist.
    
    Args:
        url (str): The URL of the file to download.
        output_path (str): The path where the file will be saved.
    """
    if not os.path.exists(output_path):
        print(f"File not found: {output_path}. Downloading...")
        try:
            subprocess.run(["wget", "-O", output_path, url], check=True)
            print(f"File downloaded successfully: {output_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error downloading file: {e}")
    else:
        print(f"File already exists: {output_path}. Skipping download.")


csv_green_trip = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"
path_green_trip = "green_tripdata_2019-10.csv.gz"

download_file(csv_green_trip, path_green_trip)

csv_zone_lookup = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
path_zone_lookup = "taxi_zone_lookup.csv"

download_file(csv_zone_lookup, path_zone_lookup)