# AI-Assisted Learning for ETL/ELT Testing

This repository contains materials for ETL/ELT testing, including Python basics, exception handling, file operations, and JSON processing. Sessions are conducted in Databricks notebooks and stored here for your reference.

## Getting Help with Python Concepts

As you work through the notebooks, you may have questions about Python syntax, data structures, or programming concepts. **AI assistants like Claude, ChatGPT, or Gemini can be excellent resources for these questions!**

Below are examples of how AI tools can help clarify Python concepts relevant to our ETL/ELT testing program. Feel free to ask similar questions when you encounter challenges.

---

## Example Conversations with AI Assistants

### Conversation 1: Understanding Functions, Parameters and Arguments

**User:**
I'm confused about functions in Python. What's the difference between parameters and arguments? And how do I write a function that accepts multiple inputs?

**Claude:**
I'd be happy to clarify functions in Python!

**Parameters vs. Arguments:**
- **Parameters** are the variables listed in the function definition
- **Arguments** are the actual values passed to the function when it's called

Think of parameters as placeholders and arguments as the real data. Here's a simple example:

```python
# Definition: 'name' is a parameter
def greet(name):
    return f"Hello, {name}!"

# Call: 'Alice' is an argument
greeting = greet("Alice")
```

**Writing Functions with Multiple Inputs:**
You can define as many parameters as needed by separating them with commas:

```python
def process_data(source_file, destination_file, transform_type="basic"):
    # source_file and destination_file are required parameters
    # transform_type is an optional parameter with a default value
    print(f"Processing {source_file} to {destination_file} using {transform_type} transformation")
    # Function logic would go here
```

This function can be called in multiple ways:
```python
# All arguments explicitly provided
process_data("raw_data.csv", "processed_data.csv", "advanced")

# Using the default for transform_type
process_data("raw_data.csv", "processed_data.csv")
```

Does that help clarify the concept?

**User:**
Yes, that's helpful! Can you give me an example of a function specifically related to ETL testing? Maybe something that validates data?

**Claude:**
Absolutely! Here's an example of a Python function for data validation in an ETL/ELT testing context:

```python
def validate_customer_data(customer_df, required_columns=None, allow_nulls=False):
    """
    Validates a customer dataframe for ETL testing
    
    Parameters:
    - customer_df: Pandas DataFrame containing customer data
    - required_columns: List of column names that must exist
    - allow_nulls: Boolean indicating if null values are acceptable
    
    Returns:
    - Dictionary with validation results and error messages
    """
    validation_results = {
        "valid": True,
        "errors": []
    }
    
    # If no required columns specified, use these defaults
    if required_columns is None:
        required_columns = ["customer_id", "name", "email", "signup_date"]
    
    # Check for required columns
    missing_columns = [col for col in required_columns if col not in customer_df.columns]
    if missing_columns:
        validation_results["valid"] = False
        validation_results["errors"].append(f"Missing required columns: {missing_columns}")
    
    # Check for null values if not allowed
    if not allow_nulls:
        null_counts = customer_df[required_columns].isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0]
        
        if not columns_with_nulls.empty:
            validation_results["valid"] = False
            for col, count in columns_with_nulls.items():
                validation_results["errors"].append(f"Column '{col}' contains {count} null values")
    
    # Validate email format (simple check)
    if "email" in customer_df.columns:
        invalid_emails = customer_df[~customer_df["email"].str.contains("@", na=False)]
        if len(invalid_emails) > 0:
            validation_results["valid"] = False
            validation_results["errors"].append(f"Found {len(invalid_emails)} records with invalid email format")
    
    return validation_results
```

Usage example:
```python
# In your ETL test script
import pandas as pd

# Load test data
test_data = pd.DataFrame({
    "customer_id": [1001, 1002, 1003, None],
    "name": ["John Doe", "Jane Smith", "Bob Johnson", "Alice Brown"],
    "email": ["john@example.com", "jane@example.com", "invalid-email", "alice@example.com"],
    "signup_date": ["2023-01-15", "2023-02-20", "2023-03-10", "2023-04-05"]
})

# Validate the data
validation_result = validate_customer_data(test_data)

# Check validation results in your test
if not validation_result["valid"]:
    print("Data validation failed:")
    for error in validation_result["errors"]:
        print(f"- {error}")
else:
    print("Data validation passed!")
```

This function demonstrates:
1. Multiple parameters (both required and optional with defaults)
2. Parameter types including a DataFrame and primitive types
3. Return value as a structured dictionary
4. Practical data validation logic you might use in ETL testing

The function checks for:
- Required columns
- Null values (when not allowed)
- Basic email format validation

You can expand this with more validation rules specific to your ETL/ELT pipeline requirements!

---

### Conversation 2: JSON Processing and Validation

**User:**
I'm working on JSON validation for our ETL tests. How can I parse a JSON file in Python, validate its structure, and handle errors if the JSON is malformed?

**Claude:**
Let me walk you through JSON processing and validation for ETL testing in Python!

**Parsing a JSON File:**
Python's built-in `json` module makes this straightforward:

```python
import json

def load_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return {"success": True, "data": data}
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"Invalid JSON format: {str(e)}"}
    except FileNotFoundError:
        return {"success": False, "error": f"File not found: {file_path}"}
    except Exception as e:
        return {"success": False, "error": f"Error loading JSON: {str(e)}"}
```

**Validating JSON Structure:**
For structure validation, you can create a function to check for required fields and data types:

```python
def validate_json_structure(json_data, schema):
    """
    Validates JSON data against a simple schema
    
    Parameters:
    - json_data: Parsed JSON data (dict or list)
    - schema: Dictionary defining expected structure
             Format: {"field_name": {"type": type, "required": bool}}
    
    Returns:
    - Dictionary with validation results
    """
    results = {"valid": True, "errors": []}
    
    # For list of objects, validate each item
    if isinstance(json_data, list):
        for i, item in enumerate(json_data):
            item_result = validate_json_structure(item, schema)
            if not item_result["valid"]:
                results["valid"] = False
                results["errors"].append(f"Item at index {i} has errors: {item_result['errors']}")
        return results
    
    # Validate required fields exist
    for field, rules in schema.items():
        if rules.get("required", False) and field not in json_data:
            results["valid"] = False
            results["errors"].append(f"Required field '{field}' is missing")
    
    # Validate field types
    for field, value in json_data.items():
        if field in schema and "type" in schema[field]:
            expected_type = schema[field]["type"]
            
            # Handle special case for lists with item type specified
            if expected_type == list and "item_type" in schema[field]:
                if not isinstance(value, list):
                    results["valid"] = False
                    results["errors"].append(f"Field '{field}' should be a list, got {type(value).__name__}")
                else:
                    item_type = schema[field]["item_type"]
                    for i, item in enumerate(value):
                        if not isinstance(item, item_type):
                            results["valid"] = False
                            results["errors"].append(
                                f"Item at index {i} in field '{field}' should be {item_type.__name__}, "
                                f"got {type(item).__name__}"
                            )
            # Regular type check
            elif not isinstance(value, expected_type):
                results["valid"] = False
                results["errors"].append(
                    f"Field '{field}' should be {expected_type.__name__}, got {type(value).__name__}"
                )
    
    return results
```

**Complete Example for ETL Testing:**
Here's how you would use these functions in an ETL test:

```python
import unittest

class TestProductDataETL(unittest.TestCase):
    
    def test_product_json_structure(self):
        # Define expected schema
        product_schema = {
            "product_id": {"type": str, "required": True},
            "name": {"type": str, "required": True},
            "price": {"type": (int, float), "required": True},
            "in_stock": {"type": bool, "required": True},
            "categories": {"type": list, "required": False, "item_type": str},
            "metadata": {"type": dict, "required": False}
        }
        
        # Load JSON file
        result = load_json_file("test_data/products.json")
        self.assertTrue(result["success"], f"Failed to load JSON: {result.get('error')}")
        
        # Validate structure
        products = result["data"]
        validation = validate_json_structure(products, product_schema)
        
        # Assert validation passes
        self.assertTrue(validation["valid"], f"JSON validation failed: {validation['errors']}")
        
        # Additional ETL-specific tests could go here...

if __name__ == "__main__":
    unittest.main()
```

**Handling Malformed JSON:**
The `load_json_file` function already handles JSON parsing errors through exception handling. For production ETL pipelines, you might:

1. Log detailed error information
2. Send alerts for critical failures
3. Track statistics on failure rates
4. Implement retry mechanisms with backoff

This approach gives you a robust framework for JSON validation in your ETL/ELT testing process!

**User:**
Great example! One more question - what's the best way to compare two JSON objects to verify transformation results in our ETL pipeline?

**Claude:**
That's an excellent question for ETL testing! When verifying transformation results, you often need to compare the original JSON with the transformed output. Here's how to effectively compare JSON objects in your ETL tests:

```python
def compare_json_objects(source, target, ignore_fields=None, tolerance=0.0001):
    """
    Compares two JSON objects for ETL transformation verification
    
    Parameters:
    - source: Original JSON object (pre-transformation)
    - target: Transformed JSON object (post-transformation)
    - ignore_fields: List of field paths to ignore (e.g., ["metadata.timestamp"])
    - tolerance: Floating point comparison tolerance
    
    Returns:
    - Dictionary with comparison results
    """
    if ignore_fields is None:
        ignore_fields = []
        
    results = {
        "identical": True,
        "differences": []
    }
    
    def _should_ignore(path):
        return any(path.startswith(field) for field in ignore_fields)
    
    def _compare_values(s_val, t_val, path):
        # Handle different types
        if type(s_val) != type(t_val):
            # Special case: numeric types comparison
            if isinstance(s_val, (int, float)) and isinstance(t_val, (int, float)):
                if abs(float(s_val) - float(t_val)) > tolerance:
                    return False
            else:
                return False
        # Handle dictionaries
        elif isinstance(s_val, dict):
            return _compare_objects(s_val, t_val, path)
        # Handle lists
        elif isinstance(s_val, list):
            return _compare_arrays(s_val, t_val, path)
        # Handle numeric values with tolerance
        elif isinstance(s_val, (int, float)):
            return abs(float(s_val) - float(t_val)) <= tolerance
        # Handle other scalar values
        else:
            return s_val == t_val
        return True
            
    def _compare_objects(s_obj, t_obj, path=""):
        is_identical = True
        
        # Check fields in source
        for key, s_val in s_obj.items():
            current_path = f"{path}.{key}" if path else key
            
            # Skip ignored fields
            if _should_ignore(current_path):
                continue
                
            # Check if key exists in target
            if key not in t_obj:
                results["differences"].append({
                    "path": current_path,
                    "type": "missing_field",
                    "source_value": s_val
                })
                is_identical = False
                continue
                
            # Compare values
            t_val = t_obj[key]
            if not _compare_values(s_val, t_val, current_path):
                results["differences"].append({
                    "path": current_path,
                    "type": "value_mismatch",
                    "source_value": s_val,
                    "target_value": t_val
                })
                is_identical = False
        
        # Check for extra fields in target
        for key in t_obj:
            current_path = f"{path}.{key}" if path else key
            if key not in s_obj and not _should_ignore(current_path):
                results["differences"].append({
                    "path": current_path,
                    "type": "extra_field",
                    "target_value": t_obj[key]
                })
                is_identical = False
                
        return is_identical
    
    def _compare_arrays(s_arr, t_arr, path):
        is_identical = True
        
        # Compare array lengths
        if len(s_arr) != len(t_arr):
            results["differences"].append({
                "path": path,
                "type": "length_mismatch",
                "source_length": len(s_arr),
                "target_length": len(t_arr)
            })
            is_identical = False
            
        # Compare items
        for i, (s_item, t_item) in enumerate(zip(s_arr, t_arr)):
            current_path = f"{path}[{i}]"
            if not _compare_values(s_item, t_item, current_path):
                results["differences"].append({
                    "path": current_path,
                    "type": "value_mismatch",
                    "source_value": s_item,
                    "target_value": t_item
                })
                is_identical = False
                
        return is_identical
    
    # Start comparison
    results["identical"] = _compare_objects(source, target)
    return results
```

**Example Usage in ETL Tests:**

```python
def test_customer_data_transformation():
    # Load source data
    source_result = load_json_file("test_data/source_customers.json")
    assert source_result["success"], f"Failed to load source JSON: {source_result.get('error')}"
    source_data = source_result["data"]
    
    # Load transformed data (output from your ETL process)
    target_result = load_json_file("test_data/transformed_customers.json")
    assert target_result["success"], f"Failed to load target JSON: {target_result.get('error')}"
    target_data = target_result["data"]
    
    # Fields to ignore in comparison (e.g., timestamps, processing metadata)
    ignore_fields = [
        "metadata.processed_timestamp",
        "metadata.etl_job_id"
    ]
    
    # Compare the JSON objects
    comparison = compare_json_objects(
        source_data, 
        target_data,
        ignore_fields=ignore_fields,
        tolerance=0.01  # For floating point comparisons (e.g., currency values)
    )
    
    # Generate detailed report if differences found
    if not comparison["identical"]:
        print(f"Found {len(comparison['differences'])} differences between source and target:")
        for diff in comparison["differences"]:
            print(f"- {diff['path']}: {diff['type']}")
            if "source_value" in diff:
                print(f"  Source: {diff['source_value']}")
            if "target_value" in diff:
                print(f"  Target: {diff['target_value']}")
    
    # Assert no unexpected differences
    assert comparison["identical"], "Transformation produced unexpected differences"
```

**Key Benefits of This Approach:**

1. **Field-by-Field Comparison**: Detailed comparison of nested structures
2. **Flexible Tolerance**: For comparing numeric values (important for financial calculations)
3. **Path Reporting**: Clear indication of where differences occur in nested structures
4. **Ignore Capability**: Skip expected differences like timestamps
5. **Type Awareness**: Handle type changes that might be part of your transformation

**Testing Specific Transformations:**

You can also extend this to test specific transformation rules:

```python
def test_customer_address_normalization():
    # Load before/after data
    source = {"address": "123 Main st.", "city": "new york", "state": "NY"}
    target = {"address": "123 Main Street", "city": "New York", "state": "NY"}
    
    # Test specific fields with custom comparators
    assert target["address"] == source["address"].replace("st.", "Street")
    assert target["city"] == source["city"].title()
    assert target["state"] == source["state"]  # Should remain unchanged
```

This comparison approach handles both wholesale validation and specific transformation rule testing, giving you comprehensive coverage for your ETL/ELT pipeline testing!

---

### Conversation 3: Exception Handling in ETL Pipelines

**User:**
In our ETL project, I need to understand exception handling better. How should I implement robust exception handling in Python for our data pipeline? What specific exceptions should I catch?

**Claude:**
# Exception Handling in ETL/ELT Pipelines

Exception handling is critical in ETL/ELT pipelines to ensure reliability, proper error reporting, and graceful failure. Here's how to implement robust exception handling for your data pipeline:

## Basic Exception Handling Structure

```python
def etl_process_data(source_path, target_path):
    try:
        # Main ETL logic here
        data = extract_data(source_path)
        transformed_data = transform_data(data)
        load_data(transformed_data, target_path)
        return {"success": True, "records_processed": len(data)}
    
    except Exception as e:
        # Log the error with traceback
        import traceback
        error_details = traceback.format_exc()
        log_error(f"ETL process failed: {str(e)}\n{error_details}")
        
        # Return structured error info
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }
```

## Specific Exceptions to Handle in ETL Pipelines

For ETL processing, you should catch and handle these specific exceptions:

```python
def extract_data(source_path):
    try:
        # Determine file type (CSV, JSON, etc.)
        file_extension = source_path.split('.')[-1].lower()
        
        if file_extension == 'csv':
            return pd.read_csv(source_path)
        elif file_extension == 'json':
            return pd.read_json(source_path)
        elif file_extension in ['xls', 'xlsx']:
            return pd.read_excel(source_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
            
    except FileNotFoundError:
        # Source data missing
        raise FileNotFoundError(f"Source file not found: {source_path}")
        
    except pd.errors.EmptyDataError:
        # File exists but is empty
        raise ValueError(f"Source file is empty: {source_path}")
        
    except pd.errors.ParserError:
        # Malformed data
        raise ValueError(f"Source file contains malformed data: {source_path}")
        
    except PermissionError:
        # Access issues
        raise PermissionError(f"Insufficient permissions to read: {source_path}")
        
    except MemoryError:
        # File too large
        raise MemoryError(f"File too large to process in memory: {source_path}")
```

## Multi-level Exception Handling

For complex ETL pipelines, implement multi-level exception handling:

```python
def run_etl_pipeline(config):
    pipeline_result = {
        "success": True,
        "steps_completed": [],
        "errors": []
    }
    
    # 1. Extract phase
    try:
        data = extract_data(config["source_path"])
        pipeline_result["steps_completed"].append("extract")
    except Exception as e:
        pipeline_result["success"] = False
        pipeline_result["errors"].append({
            "phase": "extract", 
            "error": str(e),
            "error_type": type(e).__name__
        })
        return pipeline_result  # Early exit if extract fails
    
    # 2. Transform phase
    try:
        transformed_data = transform_data(data, config["transformations"])
        pipeline_result["steps_completed"].append("transform")
    except KeyError as e:
        # Missing column
        pipeline_result["success"] = False
        pipeline_result["errors"].append({
            "phase": "transform", 
            "error": f"Missing required column: {str(e)}",
            "error_type": "KeyError"
        })
        return pipeline_result
    except ValueError as e:
        # Data type or validation error
        pipeline_result["success"] = False
        pipeline_result["errors"].append({
            "phase": "transform", 
            "error": f"Validation error: {str(e)}",
            "error_type": "ValueError"
        })
        return pipeline_result
    except Exception as e:
        # Generic transform error
        pipeline_result["success"] = False
        pipeline_result["errors"].append({
            "phase": "transform", 
            "error": str(e),
            "error_type": type(e).__name__
        })
        return pipeline_result
    
    # 3. Load phase
    try:
        load_data(transformed_data, config["target_path"])
        pipeline_result["steps_completed"].append("load")
    except Exception as e:
        pipeline_result["success"] = False
        pipeline_result["errors"].append({
            "phase": "load", 
            "error": str(e),
            "error_type": type(e).__name__
        })
    
    return pipeline_result
```

## Custom ETL Exceptions

Define custom exceptions for better error handling:

```python
# Custom exception classes
class ETLError(Exception):
    """Base class for all ETL exceptions"""
    pass

class ExtractError(ETLError):
    """Raised when data extraction fails"""
    pass

class TransformError(ETLError):
    """Raised when data transformation fails"""
    pass

class LoadError(ETLError):
    """Raised when data loading fails"""
    pass

class ValidationError(ETLError):
    """Raised when data validation fails"""
    def __init__(self, message, failed_records=None):
        super().__init__(message)
        self.failed_records = failed_records or []
```

## Example Implementation with Custom Exceptions

```python
def validate_customer_data(df):
    """Validates customer data before loading"""
    invalid_records = []
    
    # Check for missing email
    missing_email = df[df['email'].isna()]
    if not missing_email.empty:
        invalid_records.extend(missing_email.index.tolist())
    
    # Check for duplicate customer IDs
    duplicate_ids = df[df.duplicated('customer_id', keep=False)]
    if not duplicate_ids.empty:
        invalid_records.extend(duplicate_ids.index.tolist())
    
    if invalid_records:
        unique_invalid = list(set(invalid_records))
        raise ValidationError(
            f"Found {len(unique_invalid)} invalid records",
            failed_records=df.iloc[unique_invalid].to_dict('records')
        )

def transform_customer_data(df):
    try:
        # Normalize email addresses
        df['email'] = df['email'].str.lower()
        
        # Validate before returning
        validate_customer_data(df)
        
        return df
    except ValidationError as e:
        # Re-raise with additional context
        raise TransformError(f"Customer data validation failed: {str(e)}") from e
    except Exception as e:
        raise TransformError(f"Failed to transform customer data: {str(e)}") from e
```

**User:**
Thanks, that's really helpful! How can I handle retries and implement a backoff strategy for transient errors in our ETL process, especially for database or API connections?

**Claude:**
# Implementing Retry Logic and Backoff Strategies in ETL Pipelines

You've asked an excellent question! Handling transient errors with retry logic and backoff strategies is crucial for robust ETL pipelines, especially when working with external systems like databases or APIs. Here's a comprehensive approach:

## Basic Retry Decorator with Exponential Backoff

```python
import time
import random
import logging
from functools import wraps

def retry_with_backoff(max_retries=3, initial_delay=1, backoff_factor=2, jitter=0.1, 
                       exceptions=(Exception,)):
    """
    Retry decorator with exponential backoff
    
    Parameters:
    - max_retries: Maximum number of retry attempts
    - initial_delay: Initial delay in seconds
    - backoff_factor: Multiplier for delay after each retry
    - jitter: Random factor to avoid thundering herd problem
    - exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            delay = initial_delay
            
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retry_count += 1
                    
                    if retry_count > max_retries:
                        logging.error(f"Max retries ({max_retries}) exceeded. Last error: {str(e)}")
                        raise
                    
                    # Calculate jittered delay
                    jitter_amount = random.uniform(-jitter, jitter)
                    jittered_delay = delay * (1 + jitter_amount)
                    
                    logging.warning(
                        f"Retry {retry_count}/{max_retries} for {func.__name__} "
                        f"after {jittered_delay:.2f}s due to: {str(e)}"
                    )
                    
                    # Wait before retrying
                    time.sleep(jittered_delay)
                    
                    # Increase delay for next iteration
                    delay *= backoff_factor
                    
        return wrapper
    return decorator
```

## Applying the Retry Decorator to ETL Components

### Database Connection Retries

```python
# Define database-specific retryable exceptions
import psycopg2
import sqlalchemy.exc

DB_TRANSIENT_ERRORS = (
    psycopg2.OperationalError,  # Connection-related issues
    psycopg2.InterfaceError,    # Connection issues
    sqlalchemy.exc.OperationalError,
    sqlalchemy.exc.TimeoutError,
    sqlalchemy.exc.DBAPIError,
)

@retry_with_backoff(
    max_retries=5, 
    initial_delay=2, 
    exceptions=DB_TRANSIENT_ERRORS
)
def get_database_connection(connection_string):
    """Establish database connection with retry logic"""
    from sqlalchemy import create_engine
    engine = create_engine(connection_string)
    connection = engine.connect()
    return connection

@retry_with_backoff(
    max_retries=3, 
    initial_delay=1,
    exceptions=DB_TRANSIENT_ERRORS
)
def execute_query(connection, query, params=None):
    """Execute database query with retry logic"""
    try:
        result = connection.execute(query, params or {})
        return result
    except Exception as e:
        # Log the query that failed (but sanitize any sensitive parameters)
        logging.error(f"Query execution failed: {query}")
        raise
```

### API Connection Retries

```python
import requests

# Define API-specific retryable exceptions
API_TRANSIENT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.TooManyRedirects,
    requests.exceptions.RequestException,
)

# Define which HTTP status codes should trigger a retry
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}

@retry_with_backoff(
    max_retries=4, 
    initial_delay=2,
    backoff_factor=3,
    exceptions=API_TRANSIENT_ERRORS
)
def api_request(url, method="GET", headers=None, data=None, timeout=10):
    """Make API request with retry logic for both exceptions and certain status codes"""
    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        json=data,
        timeout=timeout
    )
    
    # Handle retryable status codes
    if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
        # If we have a Retry-After header, use that value
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                # If it's an integer, it's seconds to wait
                sleep_time = int(retry_after)
                logging.info(f"API request returned status {response.status_code}. "
                             f"Waiting {sleep_time}s as specified in Retry-After header")
                time.sleep(sleep_time)
            except ValueError:
                # If it's not an integer, it might be an HTTP date
                # For simplicity, we'll just use our backoff strategy
                pass
        
        # Raise an exception that will be caught by the retry decorator
        raise requests.exceptions.RequestException(
            f"Received status code {response.status_code}"
        )
    
    return response
```

## Advanced: Context-Aware Retry Strategy

For more complex ETL pipelines, you might want to implement context-aware retry strategies:

```python
class RetryContext:
    """Context object for advanced retry decisions"""
    def __init__(self, operation, max_retries=3):
        self.operation = operation
        self.max_retries = max_retries
        self.attempt = 0
        self.errors = []
        self.start_time = time.time()
    
    def should_retry(self, exception):
        """Determine if another retry should be attempted"""
        self.attempt += 1
        self.errors.append(exception)
        
        # Don't retry if we've hit max attempts
        if self.attempt >= self.max_retries:
            return False
            
        # Don't retry for certain error types
        if isinstance(exception, (ValueError, KeyError, TypeError)):
            return False
            
        # Check how long we've been trying
        elapsed_time = time.time() - self.start_time
        if elapsed_time > 300:  # 5 minutes
            logging.warning(f"Giving up on {self.operation} after {elapsed_time:.1f}s")
            return False
            
        return True
    
    def get_delay(self):
        """Calculate the next retry delay"""
        # Base exponential backoff
        delay = 2 ** self.attempt
        
        # Add jitter (±15%)
        jitter = random.uniform(0.85, 1.15)
        delay *= jitter
        
        # Cap at 60 seconds
        return min(delay, 60)

def retry_operation(operation_func, *args, max_retries=3, **kwargs):
    """Generic retry function using RetryContext"""
    context = RetryContext(operation_func.__name__, max_retries)
    
    while True:
        try:
            return operation_func(*args, **kwargs)
        except Exception as e:
            if not context.should_retry(e):
                logging.error(f"Failed to execute {context.operation} after {context.attempt} attempts")
                raise
                
            delay = context.get_delay()
            logging.info(f"Retrying {context.operation} in {delay:.2f}s (attempt {context.attempt}/{max_retries})")
            time.sleep(delay)
```

## Implementing Retry in a Complete ETL Pipeline

Here's how to integrate retry logic into a complete ETL pipeline:

```python
class ETLPipeline:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("etl_pipeline")
        
    def run(self):
        try:
            # Extract data with retries
            raw_data = self.extract_with_retries()
            
            # Transform (usually doesn't need retries unless calling external services)
            transformed_data = self.transform(raw_data)
            
            # Load data with retries
            self.load_with_retries(transformed_data)
            
            return {"success": True, "records_processed": len(transformed_data)}
        except Exception as e:
            self.logger.exception("Pipeline failed")
            return {"success": False, "error": str(e)}
    
    @retry_with_backoff(max_retries=3, initial_delay=5, exceptions=(IOError, ConnectionError))
    def extract_with_retries(self):
        """Extract data with automatic retries for I/O and connection errors"""
        self.logger.info("Extracting data from source")
        
        source_type = self.config.get("source_type")
        
        if source_type == "database":
            # Database extraction with dedicated connection handling
            connection = get_database_connection(self.config["connection_string"])
            query = self.config["extract_query"]
            result = execute_query(connection, query)
            return result.fetchall()
            
        elif source_type == "api":
            # API extraction with dedicated request handling
            response = api_request(
                url=self.config["api_url"],
                headers=self.config.get("api_headers"),
                data=self.config.get("api_params")
            )
            return response.json()
            
        elif source_type == "file":
            # File extraction
            with open(self.config["file_path"], "r") as f:
                if self.config["file_path"].endswith(".json"):
                    import json
                    return json.load(f)
                elif self.config["file_path"].endswith(".csv"):
                    import csv
                    return list(csv.DictReader(f))
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def transform(self, data):
        """Transform data (typically doesn't need retries)"""
        self.logger.info("Transforming data")
        # Transformation logic here
        return data
    
    @retry_with_backoff(max_retries=5, initial_delay=2, backoff_factor=2)
    def load_with_retries(self, data):
        """Load data with automatic retries"""
        self.logger.info(f"Loading {len(data)} records")
        
        target_type = self.config.get("target_type")
        
        if target_type == "database":
            # Database loading
            connection = get_database_connection(self.config["connection_string"])
            # Implement your database loading logic here
            
        elif target_type == "api":
            # API loading
            response = api_request(
                url=self.config["target_api_url"],
                method="POST",
                headers=self.config.get("api_headers"),
                data=data
            )
            if not response.ok:
                raise ValueError(f"API returned error: {response.status_code}, {response.text}")
                
        elif target_type == "file":
            # File loading
            with open(self.config["target_path"], "w") as f:
                if self.config["target_path"].endswith(".json"):
                    import json
                    json.dump(data, f, indent=2)
                elif self.config["target_path"].endswith(".csv"):
                    import csv
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
```

## Additional Best Practices

1. **Circuit Breaker Pattern**: If a component is consistently failing, stop attempting calls for a "cooling period"

```python
class CircuitBreaker:
    """Implements the circuit breaker pattern"""
    
    # Circuit states
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Not allowing calls
    HALF_OPEN = "half_open"  # Testing if service is back
    
    def __init__(self, failure_threshold=5, recovery_timeout=30, max_failures=10):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.max_failures = max_failures
        
        self.state = self.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if self.state == self.OPEN:
                # Check if recovery timeout has elapsed
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = self.HALF_OPEN
                    logging.info(f"Circuit half-open for {func.__name__}")
                else:
                    raise CircuitBreakerError(f"Circuit open for {func.__name__}")
            
            try:
                result = func(*args, **kwargs)
                
                # If successful and in HALF_OPEN, reset circuit
                if self.state == self.HALF_OPEN:
                    self.reset()
                    
                return result
                
            except Exception as e:
                self.record_failure()
                raise
                
        return wrapper
    
    def record_failure(self):
        """Record a failure and possibly trip the circuit"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == self.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = self.OPEN
            logging.warning(f"Circuit breaker tripped after {self.failure_count} failures")
        elif self.state == self.HALF_OPEN:
            self.state = self.OPEN
            logging.warning("Circuit breaker returned to open state after test failure")
            
        # Cap the failure count
        self.failure_count = min(self.failure_count, self.max_failures)
    
    def reset(self):
        """Reset the circuit breaker to closed state"""
        self.failure_count = 0
        self.state = self.CLOSED
        logging.info("Circuit breaker reset to closed state")

class CircuitBreakerError(Exception):
    """Raised when the circuit is open"""
    pass
```

2. **Bulkhead Pattern**: Isolate components so failure in one doesn't affect others

```python
import threading
from concurrent.futures import ThreadPoolExecutor

class Bulkhead:
    """Implements the bulkhead pattern to isolate components"""
    
    def __init__(self, max_concurrent_calls=10, max_queue_size=20):
        self.semaphore = threading.Semaphore(max_concurrent_calls)
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_calls)
        self.max_queue_size = max_queue_size
        self.queue_count = 0
        self.queue_lock = threading.Lock()
    
    def execute(self, func, *args, **kwargs):
        """Execute function with bulkhead protection"""
        with self.queue_lock:
            if self.queue_count >= self.max_queue_size:
                raise BulkheadError("Queue is full")
            self.queue_count += 1
        
        try:
            # Submit task to executor
            future = self.executor.submit(self._execute_with_semaphore, func, *args, **kwargs)
            return future
        finally:
            with self.queue_lock:
                self.queue_count -= 1
    
    def _execute_with_semaphore(self, func, *args, **kwargs):
        with self.semaphore:
            return func(*args, **kwargs)

class BulkheadError(Exception):
    """Raised when bulkhead queue is full"""
    pass
```

By implementing these patterns in your ETL pipeline, you'll significantly improve resilience against transient errors, especially when dealing with external systems like databases and APIs!

---

## More Examples of AI-Assisted Learning

The examples above demonstrate how AI tools like Claude can help you understand Python concepts for ETL/ELT testing. Feel free to ask AI assistants about:

- Python data structures (lists, dictionaries)
- File operations and error handling
- Data validation techniques
- Database connections in Python
- Regular expressions for data cleaning
- Testing frameworks for ETL pipelines
- Data transformation techniques
- Performance optimization tips

## How to Use AI for Learning

1. **Ask specific questions**: The more specific your question, the better the answer
2. **Request examples**: Ask for code examples relevant to ETL/ELT testing
3. **Follow up**: If something isn't clear, ask for clarification
4. **Try variations**: Ask about different approaches to the same problem

Remember that AI tools are available 24/7 to help you learn and solve problems during this course!