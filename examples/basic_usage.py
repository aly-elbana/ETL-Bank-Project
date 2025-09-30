"""
Basic usage example for ETL Banks Project
"""

import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from src.banks_project import extract, transform, load_to_csv, log_progress
    import pandas as pd
    
    def example_usage():
        """Demonstrate basic ETL operations"""
        print("ETL Banks Project - Basic Usage Example")
        print("=" * 50)
        
        # Example with sample data
        sample_data = pd.DataFrame({
            'Name': ['Example Bank A', 'Example Bank B'],
            'MC_USD_Billion': [150.5, 275.3]
        })
        
        print("Sample Data:")
        print(sample_data)
        print()
        
        # Transform sample data
        try:
            transformed = transform(sample_data, "./data/exchange_rate.csv")
            print("Transformed Data:")
            print(transformed)
        except Exception as e:
            print(f"Transform error: {e}")
            print("Using default exchange rates...")
            # Use default rates if CSV not available
            transformed = sample_data.copy()
            transformed['MC_GBP_Billion'] = [x * 0.8 for x in sample_data['MC_USD_Billion']]
            transformed['MC_EUR_Billion'] = [x * 0.93 for x in sample_data['MC_USD_Billion']]
            transformed['MC_INR_Billion'] = [x * 82.95 for x in sample_data['MC_USD_Billion']]
            print("Transformed Data (with default rates):")
            print(transformed)
        
        print("\nExample completed successfully!")

    if __name__ == "__main__":
        example_usage()

except ImportError as e:
    print(f"Import error: {e}")
    print("Please run from the project root directory")
