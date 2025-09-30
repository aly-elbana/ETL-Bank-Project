"""
Basic tests for the ETL Banks Project
"""

import unittest
import pandas as pd
import os
import sys

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from banks_project import extract, transform, log_progress
except ImportError:
    print("Warning: Could not import banks_project module")

class TestETLBanksProject(unittest.TestCase):
    """Test cases for ETL Banks Project"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.sample_data = pd.DataFrame({
            'Name': ['Test Bank 1', 'Test Bank 2'],
            'MC_USD_Billion': [100.0, 200.0]
        })
    
    def test_log_progress(self):
        """Test logging functionality"""
        try:
            log_progress("Test log message")
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Logging failed: {e}")
    
    def test_sample_data_structure(self):
        """Test sample data structure"""
        self.assertEqual(len(self.sample_data), 2)
        self.assertIn('Name', self.sample_data.columns)
        self.assertIn('MC_USD_Billion', self.sample_data.columns)
    
    def test_data_types(self):
        """Test data types in sample data"""
        self.assertTrue(pd.api.types.is_numeric_dtype(self.sample_data['MC_USD_Billion']))
        self.assertTrue(pd.api.types.is_object_dtype(self.sample_data['Name']))

if __name__ == '__main__':
    unittest.main()
