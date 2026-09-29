import os
import urllib.request

LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
OUTPUT_PATH = r"D:\COLLEGE\SEM 7\CNS\PROJECT\DATASET\taxi_zone_lookup.csv"

def download_zone_lookup():
    print(f"Downloading Taxi Zone Lookup CSV from: {LOOKUP_URL}")
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    if os.path.exists(OUTPUT_PATH):
        print(f"File already exists at: {OUTPUT_PATH} ({os.path.getsize(OUTPUT_PATH)} bytes)")
        return
        
    try:
        urllib.request.urlretrieve(LOOKUP_URL, OUTPUT_PATH)
        print(f"Downloaded successfully to: {OUTPUT_PATH} ({os.path.getsize(OUTPUT_PATH)} bytes)")
    except Exception as e:
        print(f"Error downloading zone lookup CSV: {e}")

if __name__ == "__main__":
    download_zone_lookup()
