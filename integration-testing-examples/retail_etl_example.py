# Retail ETL Example: Product Inventory Update
# Scenario: ETL process updates product inventory from multiple supplier feeds

import json
import logging
from typing import List, Dict, Any
from unittest.mock import Mock, patch, MagicMock
import unittest
import pytest
from datetime import datetime

# ETL Class that processes product inventory updates
class ProductInventoryETL:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)
    
    def extract_supplier_data(self, supplier_file: str) -> List[Dict]:
        """Extract product data from supplier feed"""
        with open(supplier_file, 'r') as f:
            data = json.load(f)
        return data.get('products', [])
    
    def transform_product_data(self, raw_products: List[Dict]) -> List[Dict]:
        """Transform supplier data to internal format"""
        transformed = []
        for product in raw_products:
            # Transform supplier format to internal format
            transformed_product = {
                'product_id': product['sku'],
                'quantity': int(product['stock_level']),
                'price': float(product['unit_price']),
                'supplier_id': product['supplier_code'],
                'last_updated': datetime.now().isoformat()
            }
            transformed.append(transformed_product)
        return transformed
    
    def load_to_database(self, products: List[Dict]) -> bool:
        """Load transformed data to database"""
        try:
            cursor = self.db_connection.cursor()
            
            # This is where the issue occurs in integration tests
            for product in products:
                # Unit tests don't catch the constraint violation
                cursor.execute("""
                    INSERT INTO product_inventory 
                    (product_id, quantity, price, supplier_id, last_updated)
                    VALUES (%(product_id)s, %(quantity)s, %(price)s, %(supplier_id)s, %(last_updated)s)
                    ON DUPLICATE KEY UPDATE 
                    quantity = VALUES(quantity),
                    price = VALUES(price),
                    last_updated = VALUES(last_updated)
                """, product)
            
            self.db_connection.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Database operation failed: {e}")
            self.db_connection.rollback()
            return False
    
    def process_inventory_update(self, supplier_file: str) -> bool:
        """Main ETL process"""
        try:
            # Extract
            raw_data = self.extract_supplier_data(supplier_file)
            
            # Transform
            transformed_data = self.transform_product_data(raw_data)
            
            # Load
            return self.load_to_database(transformed_data)
            
        except Exception as e:
            self.logger.error(f"ETL process failed: {e}")
            return False


# =====================================================
# UNIT TESTS (THESE PASS) - Mocked Database
# =====================================================

class TestProductInventoryETLUnit(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_db.cursor.return_value = self.mock_cursor
        self.etl = ProductInventoryETL(self.mock_db)
    
    @patch('builtins.open')
    def test_extract_supplier_data_success(self, mock_open):
        # Mock file content
        mock_file_content = json.dumps({
            "products": [
                {"sku": "PROD001", "stock_level": "100", "unit_price": "29.99", "supplier_code": "SUP001"},
                {"sku": "PROD002", "stock_level": "50", "unit_price": "19.99", "supplier_code": "SUP001"}
            ]
        })
        mock_open.return_value.__enter__.return_value.read.return_value = mock_file_content
        
        result = self.etl.extract_supplier_data("test_file.json")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['sku'], 'PROD001')
    
    def test_transform_product_data_success(self):
        raw_data = [
            {"sku": "PROD001", "stock_level": "100", "unit_price": "29.99", "supplier_code": "SUP001"}
        ]
        
        result = self.etl.transform_product_data(raw_data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['product_id'], 'PROD001')
        self.assertEqual(result[0]['quantity'], 100)
        self.assertEqual(result[0]['price'], 29.99)
    
    def test_load_to_database_success(self):
        # Mock successful database operations
        self.mock_cursor.execute.return_value = None
        self.mock_db.commit.return_value = None
        
        test_products = [
            {'product_id': 'PROD001', 'quantity': 100, 'price': 29.99, 'supplier_id': 'SUP001', 'last_updated': '2024-01-01T10:00:00'}
        ]
        
        result = self.etl.load_to_database(test_products)
        
        # Unit test passes because database is mocked
        self.assertTrue(result)
        self.mock_cursor.execute.assert_called()
        self.mock_db.commit.assert_called()


# =====================================================
# INTEGRATION TESTS (THESE FAIL) - Real Database Issues
# =====================================================

class TestProductInventoryETLIntegration(unittest.TestCase):
    def setUp(self):
        # Mock database that simulates real database behavior
        self.mock_db = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_db.cursor.return_value = self.mock_cursor
        self.etl = ProductInventoryETL(self.mock_db)
    
    def test_load_to_database_foreign_key_constraint_failure(self):
        """Integration test reveals foreign key constraint issues"""
        # Simulate database throwing foreign key constraint error
        # This wasn't caught in unit tests because DB was fully mocked
        from unittest.mock import Mock
        
        def mock_execute_with_constraint_error(*args, **kwargs):
            if 'supplier_id' in str(args):
                raise Exception("Foreign key constraint fails: supplier_id 'SUP999' doesn't exist in suppliers table")
        
        self.mock_cursor.execute.side_effect = mock_execute_with_constraint_error
        
        test_products = [
            {
                'product_id': 'PROD001', 
                'quantity': 100, 
                'price': 29.99, 
                'supplier_id': 'SUP999',  # Non-existent supplier
                'last_updated': '2024-01-01T10:00:00'
            }
        ]
        
        result = self.etl.load_to_database(test_products)
        
        # Integration test FAILS - reveals the real constraint issue
        self.assertFalse(result)
        self.mock_db.rollback.assert_called()
    
    def test_load_to_database_concurrent_update_conflict(self):
        """Integration test reveals concurrency issues"""
        # Simulate database deadlock/lock timeout that occurs in real environment
        def mock_execute_with_deadlock(*args, **kwargs):
            raise Exception("Deadlock found when trying to get lock; try restarting transaction")
        
        self.mock_cursor.execute.side_effect = mock_execute_with_deadlock
        
        test_products = [
            {'product_id': 'PROD001', 'quantity': 100, 'price': 29.99, 'supplier_id': 'SUP001', 'last_updated': '2024-01-01T10:00:00'}
        ]
        
        result = self.etl.load_to_database(test_products)
        
        # Integration test FAILS - reveals concurrency issues
        self.assertFalse(result)
    
    def test_load_to_database_data_type_mismatch(self):
        """Integration test reveals data type validation issues"""
        # Simulate real database type checking that was bypassed in unit tests
        def mock_execute_with_type_error(*args, **kwargs):
            if any('quantity' in str(arg) for arg in args):
                # Real database would validate that quantity must be non-negative
                product_data = args[1] if len(args) > 1 else {}
                if isinstance(product_data, dict) and product_data.get('quantity', 0) < 0:
                    raise Exception("Check constraint violation: quantity must be >= 0")
        
        self.mock_cursor.execute.side_effect = mock_execute_with_type_error
        
        test_products = [
            {
                'product_id': 'PROD001', 
                'quantity': -5,  # Invalid negative quantity
                'price': 29.99, 
                'supplier_id': 'SUP001', 
                'last_updated': '2024-01-01T10:00:00'
            }
        ]
        
        result = self.etl.load_to_database(test_products)
        
        # Integration test FAILS - reveals data validation issues
        self.assertFalse(result)


# =====================================================
# DEMONSTRATION OF THE ISSUE
# =====================================================

def demonstrate_unit_vs_integration_testing():
    """
    This demonstrates why the unit tests passed but integration tests failed:
    
    1. UNIT TESTS PASSED because:
       - Database was completely mocked with MagicMock
       - No real database constraints were enforced
       - No real data validation occurred
       - No concurrency issues simulated
       - All database operations returned success
    
    2. INTEGRATION TESTS FAILED because:
       - Real database constraints are enforced (foreign keys, check constraints)
       - Concurrent access patterns cause deadlocks
       - Data type validation reveals edge cases
       - Transaction rollback behavior is tested
       - Network timeouts and connection issues surface
    
    ROOT CAUSES:
    - Unit tests mocked away the database entirely
    - Integration tests revealed real-world database behavior
    - ETL code didn't handle constraint violations properly
    - Missing validation for supplier_id existence
    - No handling for concurrent update conflicts
    - Insufficient error handling for edge cases
    """
    print("Unit Tests: PASS (Database fully mocked)")
    print("Integration Tests: FAIL (Real database constraints enforced)")
    
    print("\nFailure Reasons:")
    print("1. Foreign key constraint violations (supplier doesn't exist)")
    print("2. Database deadlocks during concurrent updates")
    print("3. Check constraint violations (negative quantities)")
    print("4. Missing transaction retry logic")
    print("5. Inadequate error handling for database exceptions")


# =====================================================
# FIXED VERSION - Handles Real Database Constraints
# =====================================================

class ImprovedProductInventoryETL:
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.logger = logging.getLogger(__name__)
    
    def validate_supplier_exists(self, supplier_id: str) -> bool:
        """Validate supplier exists before inserting products"""
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM suppliers WHERE supplier_id = %s", (supplier_id,))
        return cursor.fetchone()[0] > 0
    
    def load_to_database_improved(self, products: List[Dict]) -> bool:
        """Improved load method that handles real database constraints"""
        try:
            cursor = self.db_connection.cursor()
            
            for product in products:
                # Validate data before insert
                if product['quantity'] < 0:
                    self.logger.warning(f"Skipping product {product['product_id']}: negative quantity")
                    continue
                
                # Check if supplier exists
                if not self.validate_supplier_exists(product['supplier_id']):
                    self.logger.warning(f"Skipping product {product['product_id']}: supplier {product['supplier_id']} doesn't exist")
                    continue
                
                # Retry logic for deadlocks
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        cursor.execute("""
                            INSERT INTO product_inventory 
                            (product_id, quantity, price, supplier_id, last_updated)
                            VALUES (%(product_id)s, %(quantity)s, %(price)s, %(supplier_id)s, %(last_updated)s)
                            ON DUPLICATE KEY UPDATE 
                            quantity = VALUES(quantity),
                            price = VALUES(price),
                            last_updated = VALUES(last_updated)
                        """, product)
                        break
                    except Exception as e:
                        if "Deadlock" in str(e) and attempt < max_retries - 1:
                            self.logger.warning(f"Deadlock detected, retrying... (attempt {attempt + 1})")
                            continue
                        else:
                            raise
            
            self.db_connection.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"Database operation failed: {e}")
            self.db_connection.rollback()
            return False


if __name__ == "__main__":
    demonstrate_unit_vs_integration_testing()