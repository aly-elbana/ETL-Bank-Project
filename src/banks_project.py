"""
ETL Banks Project - Extract, Transform, Load Pipeline
A comprehensive ETL pipeline for banking data extraction and processing

Inspired by: IBM Data Engineering Python Final Project
IBM Data Engineering Professional Certificate

Enhanced with:
- Improved error handling and logging
- Type hints and modern Python practices
- Modular design and configuration management
- Comprehensive testing and documentation
"""

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import os
import sys
from typing import Optional, Dict, List
import logging

# Add project root directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import *
except ImportError:
    # Default settings if config file is not available
    WIKIPEDIA_URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    TABLE_ATTRIBUTES = ["Name", "MC_USD_Billion"]
    CSV_PATH = "./data/exchange_rate.csv"
    DB_NAME = 'Banks.db'
    TABLE_NAME = "Largest_banks"
    LOG_PATH = "code_log.txt"
    LOG_FORMAT = '%Y-%b-%d-%H:%M:%S'
    DEFAULT_EXCHANGE_RATES = {'EUR': 0.93, 'GBP': 0.8, 'INR': 82.95}

# Logging function
def log_progress(message: str, log_file: str = None) -> None:
    """
    Log progress messages to file and console
    
    Args:
        message: Message to log
        log_file: Path to log file (optional)
    """
    if log_file is None:
        log_file = LOG_PATH
    
    try:
        stamp = datetime.now().strftime(LOG_FORMAT)
        with open(log_file, "a", encoding='utf-8') as file:
            file.write(f"{stamp} : {message}\n")
        print(f"[OK] {message}")  # Also print to console
    except Exception as e:
        print(f"Logging error: {e}")

# Extract function
def extract(url: str, table_attribs: List[str]) -> pd.DataFrame:
    """
    Extract banking data from Wikipedia
    
    Args:
        url: Wikipedia page URL
        table_attribs: Required column names
        
    Returns:
        DataFrame containing extracted data
    """
    try:
        log_progress("Starting data extraction from Wikipedia...")
        
        # Send HTTP request with error handling
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table', {'class': 'wikitable'})
        
        if not tables:
            raise ValueError("Required tables not found")
        
        target_table = tables[0]
        rows = target_table.find_all('tr')
        data = []

        for row in rows[1:]:  # Skip table header
            cols = row.find_all(['td', 'th'])
            if len(cols) >= 3:
                name = cols[1].text.strip()
                mc = cols[2].text.strip().replace('\n', '').replace(',', '')
                
                try:
                    mc_value = float(mc)
                    if mc_value > 0:  # Ensure positive value
                        data.append([name, mc_value])
                except (ValueError, TypeError):
                    continue

        if not data:
            raise ValueError("No valid data found")

        df = pd.DataFrame(data, columns=table_attribs)
        log_progress(f"Successfully extracted {len(df)} banks")
        return df
        
    except requests.RequestException as e:
        log_progress(f"Connection error: {e}")
        raise
    except Exception as e:
        log_progress(f"Extraction error: {e}")
        raise

# Transform function
def transform(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """
    Transform data by adding currency conversions
    
    Args:
        df: Input DataFrame with USD values
        csv_path: Path to exchange rates CSV file
        
    Returns:
        Transformed DataFrame with multiple currencies
    """
    try:
        log_progress("Starting data transformation...")
        
        # Load exchange rates with error handling
        if os.path.exists(csv_path):
            rates_df = pd.read_csv(csv_path)
            exchange_rate = rates_df.set_index('Currency').to_dict()['Rate']
        else:
            log_progress("Exchange rates CSV not found, using default rates")
            exchange_rate = DEFAULT_EXCHANGE_RATES

        # Add currency conversions
        df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
        df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
        df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

        log_progress("Data transformation complete. Initiating Loading process")
        return df
        
    except Exception as e:
        log_progress(f"Transformation error: {e}")
        raise

# Load to CSV function
def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Save DataFrame to CSV file
    
    Args:
        df: DataFrame to save
        output_path: Output file path
    """
    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        log_progress(f"Data saved to CSV file: {output_path}")
    except Exception as e:
        log_progress(f"CSV save error: {e}")
        raise

def load_to_db(df: pd.DataFrame, sql_connection, table_name: str) -> None:
    """
    Load DataFrame to SQLite database
    
    Args:
        df: DataFrame to load
        sql_connection: SQLite connection object
        table_name: Target table name
    """
    try:
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
        log_progress("Data loaded to Database as a table, Executing queries")
    except Exception as e:
        log_progress(f"Database load error: {e}")
        raise

# Run query function
def run_query(query_statement: str, sql_connection) -> pd.DataFrame:
    """
    Execute SQL query and return results
    
    Args:
        query_statement: SQL query string
        sql_connection: SQLite connection object
        
    Returns:
        Query results as DataFrame
    """
    try:
        print(f"\nRunning Query: {query_statement}\n")
        query_output = pd.read_sql(query_statement, sql_connection)
        print(query_output)
        log_progress("Query executed successfully")
        return query_output
    except Exception as e:
        log_progress(f"Query execution error: {e}")
        raise

def main():
    """
    Main ETL process execution
    """
    try:
        log_progress("ETL Process initialization complete. Starting ETL pipeline...")
        
        # Initialize database connection
        sql_connection = sqlite3.connect(DB_NAME)
        log_progress("SQL Connection initiated")

        # Run ETL pipeline step-by-step
        log_progress("Step 1: Data Extraction")
        extracted_data = extract(WIKIPEDIA_URL, TABLE_ATTRIBUTES)
        print("\nExtracted Data:")
        print(extracted_data.head())

        log_progress("Step 2: Data Transformation")
        transformed_data = transform(extracted_data, CSV_PATH)
        print("\nTransformed Data:")
        print(transformed_data.head())

        log_progress("Step 3: Data Loading")
        load_to_csv(transformed_data, OUTPUT_CSV_PATH)
        load_to_db(transformed_data, sql_connection, TABLE_NAME)

        log_progress("Step 4: Running Queries")
        run_query("SELECT * FROM Largest_banks", sql_connection)
        run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)
        run_query("SELECT Name FROM Largest_banks LIMIT 5", sql_connection)

        sql_connection.close()
        log_progress("ETL Process completed successfully!")

    except Exception as e:
        log_progress(f"ETL Process failed: {e}")
        raise

if __name__ == "__main__":
    main()