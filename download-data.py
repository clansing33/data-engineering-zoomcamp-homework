import urllib.request

def download_tripdata(color: str, year: str) -> None:
    for i in range(1, 13):
        month = ("0" + str(i))[-2:]
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"
        local_file_name = f"{color}_tripdata_{year}-{month}.csv.gz"

        urllib.request.urlretrieve(url, local_file_name)


# download_tripdata("green", "2019")
# download_tripdata("green", "2020")
# download_tripdata("yellow", "2020")
download_tripdata("fhv", "2019")