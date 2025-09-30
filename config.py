"""
Project Configuration Settings
"""
import os

# URLs
WIKIPEDIA_URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_PATH = os.path.join(DATA_DIR, "exchange_rate.csv")
OUTPUT_CSV_PATH = os.path.join(DATA_DIR, "Largest_banks_data.csv")
DB_PATH = os.path.join(DATA_DIR, "Banks.db")
LOG_PATH = os.path.join(BASE_DIR, "code_log.txt")

# Database settings
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"

# Table attributes
TABLE_ATTRIBUTES = ["Name", "MC_USD_Billion"]

# Exchange rates (fallback if CSV is not available)
DEFAULT_EXCHANGE_RATES = {
    'EUR': 0.93,
    'GBP': 0.8,
    'INR': 82.95
}

# Logging settings
LOG_FORMAT = '%Y-%b-%d-%H:%M:%S'
