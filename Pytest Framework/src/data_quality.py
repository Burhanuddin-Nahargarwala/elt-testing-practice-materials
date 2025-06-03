# In Databricks cell 5: Data Quality Functions
def check_data_completeness(data_dict, required_fields):
    """Check if all required fields are present and not empty"""
    missing_fields = []
    for field in required_fields:
        if field not in data_dict or not data_dict[field]:
            missing_fields.append(field)
    return len(missing_fields) == 0, missing_fields

def validate_data_types(data_dict, field_types):
    """Validate data types for specified fields"""
    type_errors = []
    for field, expected_type in field_types.items():
        if field in data_dict:
            if not isinstance(data_dict[field], expected_type):
                type_errors.append(f"{field}: expected {expected_type.__name__}, got {type(data_dict[field]).__name__}")
    return len(type_errors) == 0, type_errors