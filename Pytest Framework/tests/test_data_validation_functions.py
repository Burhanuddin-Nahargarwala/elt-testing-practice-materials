import sys
import os

# Add the src/ directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from data_validation_functions import validate_email, clean_phone_number, validate_age

# In Databricks cell 2: Write tests
def test_validate_email():
    # Test valid email
    assert validate_email("user@example.com") == True
    
    # Test invalid emails
    assert validate_email("invalid-email") == False
    assert validate_email("user@") == False
    assert validate_email("@example.com") == False

def test_clean_phone_number():
    # Test phone cleaning
    assert clean_phone_number("(123) 456-7890") == "1234567890"
    assert clean_phone_number("123.456.7890") == "1234567890"
    assert clean_phone_number("123abc456def") == "123456"

def test_validate_age():
    # Test valid ages
    assert validate_age(25) == True
    assert validate_age(0) == True
    assert validate_age(150) == True
    
    # Test invalid ages
    assert validate_age(-1) == False
    assert validate_age(151) == False