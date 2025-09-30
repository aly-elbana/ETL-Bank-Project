"""
ETL Banks Project Runner
Simple script to run the ETL pipeline
"""

import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from banks_project import main
    print("Starting ETL Banks Project...")
    print("=" * 50)
    main()
    print("=" * 50)
    print("ETL Process completed successfully!")
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure all dependencies are installed:")
    print("pip install -r requirements.txt")
except Exception as e:
    print(f"ETL Process failed: {e}")
    sys.exit(1)
