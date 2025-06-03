import sys
import os

# Add the src/ directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from data_quality import check_data_completeness, validate_data_types

# In Databricks cell 6: Data Quality Tests
def test_check_data_completeness():
    # Test complete data
    complete_data = {"name": "John", "email": "john@test.com", "age": 30}
    required_fields = ["name", "email", "age"]
    is_complete, missing = check_data_completeness(complete_data, required_fields)
    assert is_complete == True
    assert missing == []
    
    # Test incomplete data
    incomplete_data = {"name": "John", "email": "", "age": 30}
    is_complete, missing = check_data_completeness(incomplete_data, required_fields)
    assert is_complete == False
    assert "email" in missing

def test_validate_data_types():
    # Test correct types
    data = {"name": "John", "age": 30, "salary": 50000.5}
    types = {"name": str, "age": int, "salary": float}
    is_valid, errors = validate_data_types(data, types)
    assert is_valid == True
    assert errors == []
    
    # Test incorrect types  
    wrong_data = {"name": "John", "age": "thirty", "salary": 50000.5}
    is_valid, errors = validate_data_types(wrong_data, types)
    assert is_valid == False
    assert len(errors) > 0