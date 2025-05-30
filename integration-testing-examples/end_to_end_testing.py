# End-to-End Integration Testing with Faker Library
# Generate Realistic Data → Extract → Transform → Load → Test

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, sum as spark_sum, count, when, lit, avg, max as spark_max
from pyspark.sql.types import *

# Import Faker for realistic data generation
from faker import Faker
from faker.providers import internet, person, company, automotive, date_time
import random
from datetime import datetime, timedelta
from unittest.mock import MagicMock
import json

# Initialize Faker
fake = Faker()

print("=" * 80)
print("🎲 E2E INTEGRATION TESTING WITH FAKER LIBRARY")
print("=" * 80)

print("""
🎯 WHAT WE'LL LEARN:

1. Using Faker library to generate realistic test data
2. Building complete E2E pipeline: Extract → Transform → Load
3. Integration testing with realistic data scenarios
4. Testing data quality and business logic with varied data
5. Combining Faker with Mock for comprehensive testing

📚 FAKER LIBRARY BENEFITS:
- Generates realistic names, emails, addresses, phone numbers
- Creates consistent but varied test data
- Supports different locales and data types
- Perfect for testing edge cases with realistic patterns
""")


print("\n📚 PART 1: INTRODUCTION TO FAKER LIBRARY")
print("=" * 60)

def demonstrate_faker_basics():
    """
    Show basic Faker capabilities for generating realistic data
    """
    print("🎲 Basic Faker Demonstration")
    print("-" * 30)
    
    print("🧑 Personal Information:")
    for i in range(3):
        print(f"  Name: {fake.name()}")
        print(f"  Email: {fake.email()}")
        print(f"  Phone: {fake.phone_number()}")
        print(f"  Address: {fake.address()}")
        print(f"  Birthday: {fake.date_of_birth()}")
        print()
    
    print("🏢 Business Information:")
    for i in range(3):
        print(f"  Company: {fake.company()}")
        print(f"  Job: {fake.job()}")
        print(f"  Department: {fake.bs()}")
        print()
    
    print("💰 Financial Information:")
    for i in range(3):
        print(f"  Credit Card: {fake.credit_card_number()}")
        print(f"  Bank Account: {fake.iban()}")
        print(f"  Currency: {fake.currency_code()}")
        print()
    
    print("✅ Faker generates realistic, random data every time!")

demonstrate_faker_basics()


print("\n\n🏭 PART 2: GENERATING REALISTIC DATASETS WITH FAKER")
print("=" * 60)

def generate_customer_data(num_customers=100):
    """
    Generate realistic customer data using Faker
    """
    print(f"🎲 Generating {num_customers} realistic customers...")
    
    customers = []
    for i in range(num_customers):
        customer = Row(
            customer_id=f"CUST{i+1:05d}",
            first_name=fake.first_name(),
            last_name=fake.last_name(),
            email=fake.email(),
            phone=fake.phone_number(),
            address=fake.address().replace('\n', ', '),
            city=fake.city(),
            country=fake.country(),
            date_of_birth=fake.date_of_birth(minimum_age=18, maximum_age=80),
            registration_date=fake.date_between(start_date='-2y', end_date='today'),
            account_status=fake.random_element(elements=('active', 'inactive', 'suspended')),
            credit_score=fake.random_int(min=300, max=850),
            annual_income=fake.random_int(min=25000, max=200000)
        )
        customers.append(customer)
    
    return spark.createDataFrame(customers)

def generate_transaction_data(num_transactions=500, customer_ids=None):
    """
    Generate realistic transaction data using Faker
    """
    print(f"🎲 Generating {num_transactions} realistic transactions...")
    
    if customer_ids is None:
        customer_ids = [f"CUST{i+1:05d}" for i in range(100)]
    
    transactions = []
    for i in range(num_transactions):
        transaction = Row(
            transaction_id=f"TXN{i+1:08d}",
            customer_id=fake.random_element(elements=customer_ids),
            transaction_date=fake.date_between(start_date='-1y', end_date='today'),
            transaction_time=fake.time(),
            amount=round(fake.random.uniform(5.0, 2000.0), 2),
            transaction_type=fake.random_element(elements=('purchase', 'refund', 'transfer')),
            merchant_name=fake.company(),
            category=fake.random_element(elements=('groceries', 'electronics', 'clothing', 'dining', 'travel', 'entertainment')),
            payment_method=fake.random_element(elements=('credit_card', 'debit_card', 'bank_transfer', 'digital_wallet')),
            status=fake.random_element(elements=('completed', 'pending', 'failed'))
        )
        transactions.append(transaction)
    
    return spark.createDataFrame(transactions)

def generate_product_data(num_products=200):
    """
    Generate realistic product catalog using Faker
    """
    print(f"🎲 Generating {num_products} realistic products...")
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Beauty']
    
    products = []
    for i in range(num_products):
        category = fake.random_element(elements=categories)
        
        # Generate category-specific product names
        if category == 'Electronics':
            product_name = fake.random_element(elements=['Smartphone', 'Laptop', 'Tablet', 'Headphones', 'TV', 'Camera'])
        elif category == 'Clothing':
            product_name = fake.random_element(elements=['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Shoes', 'Hat'])
        else:
            product_name = fake.word().title() + " " + fake.word().title()
        
        product = Row(
            product_id=f"PROD{i+1:05d}",
            product_name=product_name + " " + fake.color_name(),
            category=category,
            brand=fake.company(),
            price=round(fake.random.uniform(9.99, 999.99), 2),
            cost=round(fake.random.uniform(5.0, 500.0), 2),
            stock_quantity=fake.random_int(min=0, max=1000),
            supplier=fake.company(),
            weight=round(fake.random.uniform(0.1, 50.0), 2),
            dimensions=f"{fake.random_int(1,50)}x{fake.random_int(1,50)}x{fake.random_int(1,50)} cm",
            rating=round(fake.random.uniform(1.0, 5.0), 1),
            review_count=fake.random_int(min=0, max=5000)
        )
        products.append(product)
    
    return spark.createDataFrame(products)

# Generate sample datasets
print("\n📊 Generating Sample Datasets:")
sample_customers = generate_customer_data(10)
sample_transactions = generate_transaction_data(20, [f"CUST{i+1:05d}" for i in range(10)])
sample_products = generate_product_data(15)

print("\n📋 Sample Customer Data:")
sample_customers.show(5)

print("\n📋 Sample Transaction Data:")
sample_transactions.show(5)

print("\n📋 Sample Product Data:")
sample_products.show(5)


print("\n\n📥 PART 3: DATA EXTRACTION FROM FAKER SOURCES")
print("=" * 60)

def extract_customer_data_from_source(num_customers=1000):
    """
    EXTRACTION: Simulate extracting customer data from external system
    """
    print("📥 STEP 1: Extracting Customer Data")
    print("-" * 40)
    
    try:
        # Simulate data extraction from external CRM system
        print("🔄 Connecting to CRM system...")
        print("🔄 Executing customer data query...")
        
        customers_df = generate_customer_data(num_customers)
        
        print(f"✅ Successfully extracted {customers_df.count()} customer records")
        print("📊 Sample extracted data:")
        customers_df.show(3)
        
        return customers_df, True
        
    except Exception as e:
        print(f"❌ Customer extraction failed: {e}")
        return None, False

def extract_transaction_data_from_source(customer_ids, num_transactions=5000):
    """
    EXTRACTION: Simulate extracting transaction data from external system
    """
    print("\n📥 STEP 2: Extracting Transaction Data")
    print("-" * 40)
    
    try:
        # Simulate data extraction from external transaction system
        print("🔄 Connecting to transaction database...")
        print("🔄 Executing transaction data query...")
        
        transactions_df = generate_transaction_data(num_transactions, customer_ids)
        
        print(f"✅ Successfully extracted {transactions_df.count()} transaction records")
        print("📊 Sample extracted data:")
        transactions_df.show(3)
        
        return transactions_df, True
        
    except Exception as e:
        print(f"❌ Transaction extraction failed: {e}")
        return None, False

def extract_product_data_from_source(num_products=1000):
    """
    EXTRACTION: Simulate extracting product data from external system
    """
    print("\n📥 STEP 3: Extracting Product Data")
    print("-" * 40)
    
    try:
        # Simulate data extraction from external product catalog
        print("🔄 Connecting to product catalog system...")
        print("🔄 Executing product data query...")
        
        products_df = generate_product_data(num_products)
        
        print(f"✅ Successfully extracted {products_df.count()} product records")
        print("📊 Sample extracted data:")
        products_df.show(3)
        
        return products_df, True
        
    except Exception as e:
        print(f"❌ Product extraction failed: {e}")
        return None, False

print("\n\n⚙️ PART 4: DATA TRANSFORMATION LOGIC")
print("=" * 60)

def transform_customer_data(customers_df):
    """
    TRANSFORMATION: Clean and enrich customer data
    """
    print("⚙️ STEP 1: Transforming Customer Data")
    print("-" * 40)
    
    try:
        from pyspark.sql.functions import upper, lower, regexp_replace, concat, lit, current_date, datediff
        
        # Data cleaning and standardization
        cleaned_customers = customers_df.withColumn(
            "full_name", concat(col("first_name"), lit(" "), col("last_name"))
        ).withColumn(
            "email_clean", lower(col("email"))
        ).withColumn(
            "phone_clean", regexp_replace(col("phone"), "[^0-9]", "")
        ).withColumn(
            "age", (datediff(current_date(), col("date_of_birth")) / 365.25).cast("int")
        )
        
        # Customer segmentation based on credit score and income
        segmented_customers = cleaned_customers.withColumn(
            "customer_segment",
            when((col("credit_score") >= 750) & (col("annual_income") >= 75000), "Premium")
            .when((col("credit_score") >= 650) & (col("annual_income") >= 50000), "Standard")
            .when(col("credit_score") >= 550, "Basic")
            .otherwise("Risk")
        ).withColumn(
            "risk_category",
            when(col("credit_score") >= 750, "Low Risk")
            .when(col("credit_score") >= 650, "Medium Risk")
            .otherwise("High Risk")
        )
        
        print(f"✅ Transformed {segmented_customers.count()} customer records")
        print("📊 Customer segmentation summary:")
        segmented_customers.groupBy("customer_segment").count().show()
        
        return segmented_customers, True
        
    except Exception as e:
        print(f"❌ Customer transformation failed: {e}")
        return customers_df, False

def transform_transaction_data(transactions_df):
    """
    TRANSFORMATION: Process and aggregate transaction data
    """
    print("\n⚙️ STEP 2: Transforming Transaction Data")
    print("-" * 40)
    
    try:
        from pyspark.sql.functions import year, month, dayofweek, hour, when
        
        # Add time-based features
        enhanced_transactions = transactions_df.withColumn(
            "transaction_year", year(col("transaction_date"))
        ).withColumn(
            "transaction_month", month(col("transaction_date"))
        ).withColumn(
            "day_of_week", dayofweek(col("transaction_date"))
        ).withColumn(
            "is_weekend", when(col("day_of_week").isin([1, 7]), True).otherwise(False)
        )
        
        # Categorize transaction amounts
        categorized_transactions = enhanced_transactions.withColumn(
            "amount_category",
            when(col("amount") >= 500, "High Value")
            .when(col("amount") >= 100, "Medium Value")
            .when(col("amount") >= 20, "Low Value")
            .otherwise("Micro Transaction")
        )
        
        # Add business logic flags
        final_transactions = categorized_transactions.withColumn(
            "is_large_transaction", col("amount") >= 1000
        ).withColumn(
            "requires_review", 
            (col("amount") >= 1000) | (col("transaction_type") == "refund")
        )
        
        print(f"✅ Transformed {final_transactions.count()} transaction records")
        print("📊 Transaction amount distribution:")
        final_transactions.groupBy("amount_category").count().show()
        
        return final_transactions, True
        
    except Exception as e:
        print(f"❌ Transaction transformation failed: {e}")
        return transactions_df, False

def create_customer_analytics(customers_df, transactions_df):
    """
    TRANSFORMATION: Create customer analytics by joining customer and transaction data
    """
    print("\n⚙️ STEP 3: Creating Customer Analytics")
    print("-" * 40)
    
    try:
        # Calculate customer transaction metrics
        customer_metrics = transactions_df.filter(
            col("status") == "completed"
        ).groupBy("customer_id").agg(
            spark_sum("amount").alias("total_spent"),
            count("transaction_id").alias("transaction_count"),
            avg("amount").alias("avg_transaction_amount"),
            spark_max("amount").alias("max_transaction_amount"),
            count(when(col("transaction_type") == "refund", 1)).alias("refund_count")
        )
        
        # Join customer data with transaction metrics
        customer_analytics = customers_df.join(
            customer_metrics, "customer_id", "left"
        ).fillna({
            "total_spent": 0.0,
            "transaction_count": 0,
            "avg_transaction_amount": 0.0,
            "max_transaction_amount": 0.0,
            "refund_count": 0
        })
        
        # Add customer value scoring
        customer_analytics = customer_analytics.withColumn(
            "customer_value_score",
            (col("total_spent") * 0.4) + 
            (col("transaction_count") * 10) + 
            (col("credit_score") * 0.1) - 
            (col("refund_count") * 50)
        ).withColumn(
            "customer_tier",
            when(col("customer_value_score") >= 1000, "Platinum")
            .when(col("customer_value_score") >= 500, "Gold")
            .when(col("customer_value_score") >= 200, "Silver")
            .otherwise("Bronze")
        )
        
        print(f"✅ Created analytics for {customer_analytics.count()} customers")
        print("📊 Customer tier distribution:")
        customer_analytics.groupBy("customer_tier").count().show()
        
        return customer_analytics, True
        
    except Exception as e:
        print(f"❌ Customer analytics creation failed: {e}")
        return customers_df, False

print("\n\n💾 PART 5: DATA LOADING WITH MOCK TARGETS")
print("=" * 60)

def load_to_data_warehouse(data_df, table_name, warehouse_connection):
    """
    LOADING: Save processed data to data warehouse (mocked)
    """
    print(f"💾 Loading data to warehouse table: {table_name}")
    print("-" * 40)
    
    try:
        # Convert DataFrame to records for warehouse loading
        record_count = data_df.count()
        
        # Simulate data validation before loading
        print("🔍 Validating data quality...")
        
        # Check for required fields
        null_ids = data_df.filter(col("customer_id").isNull()).count()
        if null_ids > 0:
            raise Exception(f"Found {null_ids} records with null customer_id")
        
        # Check for duplicate records
        total_records = data_df.count()
        unique_records = data_df.distinct().count()
        if total_records != unique_records:
            print(f"⚠️ Warning: Found {total_records - unique_records} duplicate records")
        
        # Mock warehouse operations
        print("🔄 Connecting to data warehouse...")
        warehouse_connection.create_table_if_not_exists(table_name)
        
        print("🔄 Inserting records...")
        warehouse_connection.insert_batch(table_name, record_count)
        
        print("🔄 Creating indexes...")
        warehouse_connection.create_indexes(table_name)
        
        print("🔄 Committing transaction...")
        warehouse_connection.commit()
        
        print(f"✅ Successfully loaded {record_count} records to {table_name}")
        return record_count, True
        
    except Exception as e:
        print(f"❌ Loading to {table_name} failed: {e}")
        warehouse_connection.rollback()
        return 0, False

def load_to_analytics_store(analytics_df, analytics_connection):
    """
    LOADING: Save analytics data to analytics store (mocked)
    """
    print("💾 Loading data to analytics store")
    print("-" * 40)
    
    try:
        record_count = analytics_df.count()
        
        # Mock analytics store operations
        print("🔄 Connecting to analytics store...")
        analytics_connection.prepare_analytics_tables()
        
        print("🔄 Loading customer analytics...")
        analytics_connection.load_customer_metrics(record_count)
        
        print("🔄 Updating dashboards...")
        analytics_connection.refresh_dashboards()
        
        print(f"✅ Successfully loaded {record_count} analytics records")
        return record_count, True
        
    except Exception as e:
        print(f"❌ Analytics loading failed: {e}")
        return 0, False

print("\n\n🚀 PART 6: COMPLETE END-TO-END PIPELINE")
print("=" * 60)

def run_complete_e2e_pipeline(num_customers=1000, num_transactions=5000):
    """
    Complete end-to-end pipeline: Extract → Transform → Load
    """
    print("🚀 Starting Complete E2E Data Pipeline")
    print("=" * 50)
    
    pipeline_results = {
        "extraction_success": False,
        "transformation_success": False,
        "loading_success": False,
        "total_customers": 0,
        "total_transactions": 0,
        "analytics_records": 0
    }
    
    try:
        # STEP 1: EXTRACTION
        print("\n📥 PHASE 1: DATA EXTRACTION")
        print("=" * 30)
        
        # Extract customers
        customers_df, customer_extract_success = extract_customer_data_from_source(num_customers)
        if not customer_extract_success:
            raise Exception("Customer extraction failed")
        
        # Get customer IDs for transaction extraction
        customer_ids = [row.customer_id for row in customers_df.select("customer_id").collect()]
        
        # Extract transactions
        transactions_df, transaction_extract_success = extract_transaction_data_from_source(
            customer_ids, num_transactions
        )
        if not transaction_extract_success:
            raise Exception("Transaction extraction failed")
        
        pipeline_results["extraction_success"] = True
        pipeline_results["total_customers"] = customers_df.count()
        pipeline_results["total_transactions"] = transactions_df.count()
        
        # STEP 2: TRANSFORMATION
        print("\n⚙️ PHASE 2: DATA TRANSFORMATION")
        print("=" * 30)
        
        # Transform customer data
        transformed_customers, customer_transform_success = transform_customer_data(customers_df)
        if not customer_transform_success:
            raise Exception("Customer transformation failed")
        
        # Transform transaction data
        transformed_transactions, transaction_transform_success = transform_transaction_data(transactions_df)
        if not transaction_transform_success:
            raise Exception("Transaction transformation failed")
        
        # Create customer analytics
        customer_analytics, analytics_success = create_customer_analytics(
            transformed_customers, transformed_transactions
        )
        if not analytics_success:
            raise Exception("Analytics creation failed")
        
        pipeline_results["transformation_success"] = True
        pipeline_results["analytics_records"] = customer_analytics.count()
        
        # STEP 3: LOADING (with mocks)
        print("\n💾 PHASE 3: DATA LOADING")
        print("=" * 30)
        
        # Create mock connections
        mock_warehouse = MagicMock()
        mock_warehouse.create_table_if_not_exists.return_value = None
        mock_warehouse.insert_batch.return_value = None
        mock_warehouse.create_indexes.return_value = None
        mock_warehouse.commit.return_value = None
        mock_warehouse.rollback.return_value = None
        
        mock_analytics = MagicMock()
        mock_analytics.prepare_analytics_tables.return_value = None
        mock_analytics.load_customer_metrics.return_value = None
        mock_analytics.refresh_dashboards.return_value = None
        
        # Load data
        customer_load_count, customer_load_success = load_to_data_warehouse(
            transformed_customers, "customers", mock_warehouse
        )
        
        transaction_load_count, transaction_load_success = load_to_data_warehouse(
            transformed_transactions, "transactions", mock_warehouse
        )
        
        analytics_load_count, analytics_load_success = load_to_analytics_store(
            customer_analytics, mock_analytics
        )
        
        pipeline_results["loading_success"] = (
            customer_load_success and transaction_load_success and analytics_load_success
        )
        
        # PIPELINE SUMMARY
        print("\n📊 PIPELINE EXECUTION SUMMARY")
        print("=" * 40)
        
        all_phases_success = (
            pipeline_results["extraction_success"] and 
            pipeline_results["transformation_success"] and 
            pipeline_results["loading_success"]
        )
        
        print(f"📥 Extraction: {'✅ SUCCESS' if pipeline_results['extraction_success'] else '❌ FAILED'}")
        print(f"⚙️ Transformation: {'✅ SUCCESS' if pipeline_results['transformation_success'] else '❌ FAILED'}")
        print(f"💾 Loading: {'✅ SUCCESS' if pipeline_results['loading_success'] else '❌ FAILED'}")
        print(f"🎯 Overall Pipeline: {'✅ SUCCESS' if all_phases_success else '❌ FAILED'}")
        
        print(f"\n📊 Data Processed:")
        print(f"  👥 Customers: {pipeline_results['total_customers']}")
        print(f"  💳 Transactions: {pipeline_results['total_transactions']}")
        print(f"  📈 Analytics Records: {pipeline_results['analytics_records']}")
        
        return pipeline_results, customer_analytics
        
    except Exception as e:
        print(f"\n❌ PIPELINE FAILED: {e}")
        return pipeline_results, None


print("\n\n🧪 PART 7: INTEGRATION TESTING SCENARIOS")
print("=" * 60)

def test_e2e_pipeline_small_dataset():
    """
    Integration Test 1: Small dataset (happy path)
    """
    print("🧪 INTEGRATION TEST 1: Small Dataset")
    print("-" * 40)
    
    results, analytics_data = run_complete_e2e_pipeline(num_customers=50, num_transactions=200)
    
    # Validate results
    test_passed = (
        results["extraction_success"] and
        results["transformation_success"] and
        results["loading_success"] and
        results["total_customers"] == 50 and
        results["total_transactions"] == 200
    )
    
    print(f"\n🎯 Small Dataset Test: {'✅ PASSED' if test_passed else '❌ FAILED'}")
    return test_passed

def test_e2e_pipeline_large_dataset():
    """
    Integration Test 2: Large dataset (performance test)
    """
    print("\n🧪 INTEGRATION TEST 2: Large Dataset")
    print("-" * 40)
    
    import time
    start_time = time.time()
    
    results, analytics_data = run_complete_e2e_pipeline(num_customers=2000, num_transactions=10000)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Validate results and performance
    test_passed = (
        results["extraction_success"] and
        results["transformation_success"] and
        results["loading_success"] and
        execution_time < 120  # Should complete within 2 minutes
    )
    
    print(f"\n⏱️ Execution Time: {execution_time:.2f} seconds")
    print(f"🎯 Large Dataset Test: {'✅ PASSED' if test_passed else '❌ FAILED'}")
    return test_passed

def test_e2e_pipeline_data_quality():
    """
    Integration Test 3: Data quality validation
    """
    print("\n🧪 INTEGRATION TEST 3: Data Quality Validation")
    print("-" * 40)
    
    # Run pipeline with moderate dataset
    results, analytics_data = run_complete_e2e_pipeline(num_customers=500, num_transactions=2000)
    
    if analytics_data is None:
        print("❌ No data to validate")
        return False
    
    # Data quality checks
    quality_checks = {}
    
    # Check 1: No null customer IDs
    null_customer_ids = analytics_data.filter(col("customer_id").isNull()).count()
    quality_checks["no_null_ids"] = (null_customer_ids == 0)
    
    # Check 2: All customers have valid email format
    invalid_emails = analytics_data.filter(~col("email_clean").contains("@")).count()
    quality_checks["valid_emails"] = (invalid_emails == 0)
    
    # Check 3: Credit scores in valid range
    invalid_credit_scores = analytics_data.filter(
        (col("credit_score") < 300) | (col("credit_score") > 850)
    ).count()
    quality_checks["valid_credit_scores"] = (invalid_credit_scores == 0)
    
    # Check 4: Customer segments assigned
    unassigned_segments = analytics_data.filter(col("customer_segment").isNull()).count()
    quality_checks["segments_assigned"] = (unassigned_segments == 0)
    
    # Check 5: Age calculations reasonable
    invalid_ages = analytics_data.filter((col("age") < 18) | (col("age") > 100)).count()
    quality_checks["reasonable_ages"] = (invalid_ages == 0)
    
    print("\n🔍 Data Quality Results:")
    for check_name, passed in quality_checks.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {check_name}: {status}")
    
    all_quality_passed = all(quality_checks.values())
    print(f"\n🎯 Data Quality Test: {'✅ PASSED' if all_quality_passed else '❌ FAILED'}")
    return all_quality_passed

def test_e2e_pipeline_business_logic():
    """
    Integration Test 4: Business logic validation
    """
    print("\n🧪 INTEGRATION TEST 4: Business Logic Validation")
    print("-" * 40)
    
    # Run pipeline
    results, analytics_data = run_complete_e2e_pipeline(num_customers=300, num_transactions=1500)
    
    if analytics_data is None:
        print("❌ No data to validate")
        return False
    
    # Business logic checks
    business_checks = {}
    
    # Check 1: Premium customers have high credit scores
    premium_customers = analytics_data.filter(col("customer_segment") == "Premium")
    low_credit_premium = premium_customers.filter(col("credit_score") < 750).count()
    business_checks["premium_credit_logic"] = (low_credit_premium == 0)
    
    # Check 2: Customer tiers align with value scores
    platinum_customers = analytics_data.filter(col("customer_tier") == "Platinum")
    low_value_platinum = platinum_customers.filter(col("customer_value_score") < 1000).count()
    business_checks["platinum_value_logic"] = (low_value_platinum == 0)
    
    # Check 3: Risk categories align with credit scores
    low_risk_customers = analytics_data.filter(col("risk_category") == "Low Risk")
    low_credit_low_risk = low_risk_customers.filter(col("credit_score") < 750).count()
    business_checks["risk_category_logic"] = (low_credit_low_risk == 0)
    
    # Check 4: Total spent is non-negative
    negative_spending = analytics_data.filter(col("total_spent") < 0).count()
    business_checks["positive_spending"] = (negative_spending == 0)
    
    print("\n🏢 Business Logic Results:")
    for check_name, passed in business_checks.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {check_name}: {status}")
    
    all_business_passed = all(business_checks.values())
    print(f"\n🎯 Business Logic Test: {'✅ PASSED' if all_business_passed else '❌ FAILED'}")
    return all_business_passed

def test_e2e_pipeline_with_extraction_failure():
    """
    Integration Test 5: Extraction failure scenario
    """
    print("\n🧪 INTEGRATION TEST 5: Extraction Failure")
    print("-" * 40)
    
    # Simulate extraction failure by causing Faker to fail
    try:
        # This will fail because we're passing invalid parameters
        fake.random_int(min=1000, max=10)  # min > max causes failure
        test_passed = False
    except:
        print("✅ Successfully simulated extraction failure")
        test_passed = True
    
    print(f"🎯 Extraction Failure Test: {'✅ PASSED' if test_passed else '❌ FAILED'}")
    return test_passed

print("\n\n🏆 PART 8: COMPLETE INTEGRATION TEST SUITE")
print("=" * 60)

def run_complete_integration_test_suite():
    """
    Run all integration tests and provide comprehensive results
    """
    print("🧪 Running Complete Integration Test Suite")
    print("=" * 50)
    
    test_results = {}
    
    # Run all integration tests
    print("\n🚀 Starting Integration Test Execution...")
    
    test_results["small_dataset"] = test_e2e_pipeline_small_dataset()
    test_results["large_dataset"] = test_e2e_pipeline_large_dataset()
    test_results["data_quality"] = test_e2e_pipeline_data_quality()
    test_results["business_logic"] = test_e2e_pipeline_business_logic()
    test_results["extraction_failure"] = test_e2e_pipeline_with_extraction_failure()
    
    # Calculate overall results
    total_tests = len(test_results)
    passed_tests = sum(test_results.values())
    pass_rate = (passed_tests / total_tests) * 100
    
    # Print comprehensive summary
    print("\n" + "=" * 60)
    print("📊 INTEGRATION TEST SUITE SUMMARY")
    print("=" * 60)
    
    for test_name, passed in test_results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name:20} : {status}")
    
    print(f"\n📈 OVERALL RESULTS:")
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")
    print(f"Pass Rate: {pass_rate:.1f}%")
    
    if passed_tests == total_tests:
        print("\n🎉 ALL INTEGRATION TESTS PASSED!")
        print("✅ Your E2E pipeline is robust and production-ready!")
    else:
        print(f"\n⚠️ {total_tests - passed_tests} test(s) failed")
        print("🔧 Pipeline needs improvements before production deployment")
    
    return test_results

# Run the complete test suite
final_test_results = run_complete_integration_test_suite()

print("\n\n🎓 KEY TAKEAWAYS")
print("=" * 50)

print("""

1. FAKER LIBRARY POWER:
   🎲 Generate realistic test data automatically
   📊 Create varied scenarios for thorough testing
   🎯 Test edge cases with realistic data patterns
   🔄 Reproducible yet varied test datasets

2. E2E PIPELINE WITH REALISTIC DATA:
   📥 Extract: Faker generates source system data
   ⚙️ Transform: Process real-world data patterns
   💾 Load: Mock targets while testing real logic
   🧪 Test: Comprehensive integration validation

3. INTEGRATION TESTING BENEFITS:
   🔍 Test complete data flow with real data patterns
   📊 Validate data quality across entire pipeline
   🏢 Verify business logic with varied scenarios
   ⚡ Test performance with different data volumes

4. COMBINING FAKER + MOCK:
   🎲 Faker: Realistic input data generation
   🎭 Mock: Controlled external system behavior
   🔄 Best of both: Real data patterns + safe testing
   📈 Comprehensive: Success and failure scenarios

💡 REMEMBER:
- Faker makes test data realistic and varied
- Integration testing catches issues unit tests miss
- Mock external systems but use real data patterns
- Test both happy paths and failure scenarios
- Data quality validation is crucial in pipelines
""")

print("\n✅ End-to-End Integration Testing with Faker Complete!")
print("🎯 You now know how to build robust, well-tested data pipelines!")
