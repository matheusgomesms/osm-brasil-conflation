import os
import requests
import sys

# Import functions from the scripts folder
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
from clean import process_clean
from conflate import run_conflation

# --- CONFIGURATION ---
DATA_URL = "https://dados.fortaleza.ce.gov.br/dataset/c529ba45-27ae-4d3a-8f70-76019a87edba/resource/4e4a18a1-86cc-48a1-b631-3ef636b455ee/download/dadosabertos_semaforosctafor.geojson"

# Setup Folders
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, 'data', 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'output')

# File Paths
RAW_FILE = os.path.join(INPUT_DIR, 'semaforos_raw.geojson')
CLEAN_FILE = os.path.join(OUTPUT_DIR, 'clean_traffic_lights.geojson')

def download_data(url, save_path):
    print(f"Downloading data from {url}...")
    try:
        r = requests.get(url, verify=False) # verify=False because gov sites often have SSL issues
        r.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(r.content)
        print("Download successful.")
    except Exception as e:
        print(f"Error downloading: {e}")
        sys.exit(1)

def main():
    # 0. Create directories if they don't exist
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Download
    download_data(DATA_URL, RAW_FILE)

    # 2. Clean
    print("--- 1. Running Cleaning Script ---")
    process_clean(RAW_FILE, CLEAN_FILE)

    # 3. Conflate
    print("--- 2. Running Conflation Script ---")
    run_conflation(CLEAN_FILE, OUTPUT_DIR)

    print("--- DONE ---")

if __name__ == "__main__":
    main()