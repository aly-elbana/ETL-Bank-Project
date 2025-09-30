# ETL Banks Project

A comprehensive Extract, Transform, Load (ETL) pipeline for banking data extraction and processing from Wikipedia.

> **Note**: This project was inspired by the IBM Data Engineering Python Final Project for the IBM Data Engineering Professional Certificate. It has been enhanced with additional features, improved error handling, and modern Python best practices.

## 🚀 Features

- **Data Extraction**: Scrapes banking data from Wikipedia's list of largest banks
- **Data Transformation**: Converts market capitalization to multiple currencies (USD, GBP, EUR, INR)
- **Data Loading**: Saves data to both CSV files and SQLite database
- **Error Handling**: Robust error handling and logging throughout the pipeline
- **Type Hints**: Full type annotations for better code maintainability
- **Modular Design**: Clean separation of concerns with configurable settings

## 📁 Project Structure

```
├── src/
│   └── banks_project.py      # Main ETL pipeline
├── data/
│   ├── exchange_rate.csv    # Exchange rates data
│   ├── Largest_banks_data.csv # Output CSV file
│   └── Banks.db            # SQLite database
├── config.py               # Configuration settings
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore rules
├── code_log.txt          # Execution logs
└── README.md             # This file
```

## 🛠️ Installation

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd etl-banks-project
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the ETL pipeline**
   ```bash
   python src/banks_project.py
   ```

## 📊 Data Sources

- **Wikipedia**: List of largest banks by market capitalization
- **Exchange Rates**: Currency conversion rates for GBP, EUR, and INR

## 🔧 Configuration

The project uses a centralized configuration system in `config.py`:

- **URLs**: Wikipedia data source
- **File Paths**: Input/output file locations
- **Database Settings**: SQLite database configuration
- **Exchange Rates**: Default currency rates

## 📈 ETL Process

### 1. Extract

- Scrapes banking data from Wikipedia
- Extracts bank names and market capitalization in USD
- Handles network errors and data validation

### 2. Transform

- Loads exchange rates from CSV file
- Converts USD values to GBP, EUR, and INR
- Applies data validation and error handling

### 3. Load

- Saves transformed data to CSV file
- Loads data into SQLite database
- Executes sample queries for verification

## 🗄️ Database Schema

The `Largest_banks` table contains:

- `Name`: Bank name
- `MC_USD_Billion`: Market cap in USD (billions)
- `MC_GBP_Billion`: Market cap in GBP (billions)
- `MC_EUR_Billion`: Market cap in EUR (billions)
- `MC_INR_Billion`: Market cap in INR (billions)

## 📝 Logging

The project includes comprehensive logging:

- Progress tracking for each ETL step
- Error logging with detailed messages
- Console output for real-time monitoring
- File-based logging in `code_log.txt`

## 🧪 Sample Queries

The pipeline automatically runs these verification queries:

```sql
-- View all data
SELECT * FROM Largest_banks;

-- Average market cap in GBP
SELECT AVG(MC_GBP_Billion) FROM Largest_banks;

-- Top 5 banks by name
SELECT Name FROM Largest_banks LIMIT 5;
```

## 🔍 Error Handling

The pipeline includes robust error handling for:

- Network connectivity issues
- Data parsing errors
- File I/O operations
- Database operations
- Missing configuration files

## 📋 Requirements

- Python 3.7+
- pandas >= 1.5.0
- numpy >= 1.21.0
- requests >= 2.28.0
- beautifulsoup4 >= 4.11.0
- lxml >= 4.9.0

## 🆘 Support

If you encounter any issues or have questions, please:

1. Check the logs in `code_log.txt`
2. Review the error messages in the console
3. Ensure all dependencies are installed
4. Verify the data sources are accessible
