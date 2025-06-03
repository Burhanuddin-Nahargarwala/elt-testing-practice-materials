# In Databricks cell 1: Define utility functions
def validate_email(email):
    """Validate email format"""
    return "@" in email and "." in email

def clean_phone_number(phone):
    """Clean phone number by removing non-digits"""
    return ''.join(filter(str.isdigit, phone))

def validate_age(age):
    """Validate age is reasonable"""
    return 0 <= age <= 100