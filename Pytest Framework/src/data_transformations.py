# In Databricks cell 3: Define transformation functions
def standardize_name(name):
    """Standardize name format"""
    if not name:
        return ""
    return name.strip().title()

def calculate_total_sales(sales_list):
    """Calculate total from sales list"""
    if not sales_list:
        return 0
    return sum(sales_list)

def convert_currency(amount, rate):
    """Convert currency with exchange rate"""
    if rate <= 0:
        raise ValueError("Exchange rate must be positive")
    return round(amount * rate, 2)