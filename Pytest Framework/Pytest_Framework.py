# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# my_functions.py (or any notebook containing your logic)
def add(a, b):
    return a + b

# COMMAND ----------

# test_my_functions (can be another notebook or within same notebook)
def test_add():
    assert add(2, 3) == 5
    assert add(-1, 1) == 0

# COMMAND ----------

test_add()  # Will raise error if the assertion fails

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs

# COMMAND ----------

# MAGIC %%sh
# MAGIC mkdir -p /dbfs/tmp/pytest_framework/src
# MAGIC mkdir -p /dbfs/tmp/pytest_framework/tests

# COMMAND ----------

# MAGIC %%sh
# MAGIC # Copy your files from workspace to DBFS
# MAGIC cp src/my_module.py /dbfs/tmp/pytest_framework/src/
# MAGIC cp tests/test_my_module.py /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %%sh
# MAGIC cp tests/* /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %%sh
# MAGIC ls -lh /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %sh
# MAGIC pytest /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This is how we have write the pytest

# COMMAND ----------

import pytest
import os

test_dir = "/dbfs/tmp/pytest_framework/tests/"
test_files = [os.path.join(test_dir, "test_my_module.py"), os.path.join(test_dir, "test_date.py")]

# print(test_files)

# Run pytest and generate a report
pytest.main(["--junitxml=/dbfs/tmp/pytest_results.xml"] + test_files)

# COMMAND ----------

# MAGIC %%sh
# MAGIC cat /dbfs/tmp/pytest_results.xml

# COMMAND ----------

# MAGIC %pip install pytest-json-report

# COMMAND ----------

import pytest
import os

test_dir = "/dbfs/tmp/pytest_framework/tests/"
test_files = [
    os.path.join(test_dir, "test_my_module.py"),
    os.path.join(test_dir, "test_date.py")
]

# Run pytest and generate a JSON report
pytest.main(["--json-report", "--json-report-file=/dbfs/tmp/pytest_results.json"] + test_files)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/tmp/pytest_results.json

# COMMAND ----------



# COMMAND ----------

import pytest
import pandas as pd

# Sample function to test
def add(a, b):
    return a + b

# Sample test
def test_add():
    assert add(2, 3) == 5
    assert add(-1, 1) == 0

# Run test manually using pytest
test_add()

# COMMAND ----------

# MAGIC %%sh
# MAGIC cp tests/test_phone_no_format.py /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %%sh
# MAGIC ls -lh /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %sh
# MAGIC pytest -v /dbfs/tmp/pytest_framework/tests/test_phone_no_format.py

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Added Test Data Validation

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %%sh
# MAGIC cp tests/test_data_validation.py /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %sh
# MAGIC pytest /dbfs/tmp/pytest_framework/tests/test_data_validation.py

# COMMAND ----------

# MAGIC %%sh
# MAGIC cp tests/test_square_function_mark_parametrization.py /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %sh
# MAGIC pytest /dbfs/tmp/pytest_framework/tests/test_square_function_mark_parametrization.py

# COMMAND ----------

# MAGIC %%sh
# MAGIC cp src/data_validation_functions.py /dbfs/tmp/pytest_framework/src/
# MAGIC cp tests/test_data_validation_functions.py /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# Let's run it via python

import pytest
import os

test_dir = "/dbfs/tmp/pytest_framework/tests/"
test_files = [
    os.path.join(test_dir, "test_data_validation_functions.py"),
]

# Run pytest and generate a JSON report
pytest.main(["--junitxml=/dbfs/tmp/pytest_results.xml"] + test_files)

# COMMAND ----------

def validate_email(email):
    """Validate email format"""
    return "@" in email and "." in email

validate_email("@example.com")

# COMMAND ----------

# Valid domains are com, org, net

def validate_email(email):
    # email = "abc@example.com"
    one_check = False # @ is proper or not
    second_check = False # domain is proper or not
    if len(email.split("@"))==2 and email.split("@")[0] and email.split("@")[1]:
        one_check = True

    if len(email.split("."))==2 and email.split(".")[0] and email.split(".")[1]:
        domain = email.split(".")[1]
        if domain in ["com", "org", "net"]:
            second_check = True

    if one_check and second_check:
        return True
    else:
        return False
    
print(validate_email("@example.com"))
print(validate_email("example.com"))
print(validate_email("example"))
print(validate_email("abc.example.com"))
print(validate_email("abc@example.com")) # Valid
print(validate_email("abc@example.in"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Example 2: Testing Data Transformation Functions

# COMMAND ----------

# MAGIC %%sh
# MAGIC cp src/data_transformations.py /dbfs/tmp/pytest_framework/src/
# MAGIC cp tests/test_data_transformations.py /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %sh
# MAGIC pytest /dbfs/tmp/pytest_framework/tests/test_data_transformations.py

# COMMAND ----------

# Let's run it via python

import pytest
import os

test_dir = "/dbfs/tmp/pytest_framework/tests/"
test_files = [
    os.path.join(test_dir, "test_data_transformations.py"),
]

# Run pytest and generate a report
pytest.main(["--junitxml=/dbfs/tmp/pytest_results.xml"] + test_files)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/tmp/pytest_results.xml

# COMMAND ----------

# MAGIC %%sh
# MAGIC cp src/data_quality.py /dbfs/tmp/pytest_framework/src/
# MAGIC cp tests/test_data_quality.py /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# Let's run it via python

import pytest
import os

test_dir = "/dbfs/tmp/pytest_framework/tests/"
test_files = [
    os.path.join(test_dir, "test_data_quality.py"),
]

# Run pytest and generate a report
pytest.main(["--junitxml=/dbfs/tmp/pytest_results.xml"] + test_files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I have done the changes but still the previous code only get's executed and new code is not executing, it is because of caching.
# MAGIC
# MAGIC When you run:
# MAGIC ```python
# MAGIC pytest.main([...])
# MAGIC ```
# MAGIC
# MAGIC Python **imports the test file and any modules it uses**. If you've already run the test once in the current Python session (like a notebook or a persistent shell), then **any changes you make to the test file or source code afterward are not reflected** because:
# MAGIC
# MAGIC - The file is already loaded into memory.
# MAGIC - Python doesn’t automatically re-import the updated file unless you explicitly reload it.
# MAGIC
# MAGIC
# MAGIC ### Solution:
# MAGIC
# MAGIC Option 1: Restart the Python kernel/session (most reliable)
# MAGIC
# MAGIC Option 2: Force reload using `importlib`
# MAGIC
# MAGIC Option 3: Run pytest as a shell command (bypasses Python caching)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's Run pytest as a shell command (bypasses Python caching)

# COMMAND ----------

# MAGIC %sh
# MAGIC pytest /dbfs/tmp/pytest_framework/tests/test_data_quality.py

# COMMAND ----------

# MAGIC %%sh
# MAGIC pytest /dbfs/tmp/pytest_framework/tests/test_data_quality.py::test_check_data_completeness

# COMMAND ----------

# MAGIC %%sh
# MAGIC
# MAGIC cp src/elt_functions.py /dbfs/tmp/pytest_framework/src/
# MAGIC cp tests/test_elt_functions.py /dbfs/tmp/pytest_framework/tests/

# COMMAND ----------

# MAGIC %sh
# MAGIC pytest /dbfs/tmp/pytest_framework/tests/test_elt_functions.py

# COMMAND ----------

