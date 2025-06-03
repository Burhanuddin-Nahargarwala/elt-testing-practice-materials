def check_order_id(order_id):
    """Check if order ID is valid format: ORD-YYYY-NNN"""
    if not order_id:
        return False, "Order ID is empty"
    
    if not order_id.startswith("ORD-"):
        return False, "Order ID must start with ORD-"
    
    return True, "Valid"

def check_email(email):
    """Check if email has basic valid format"""
    if not email:
        return False, "Email is empty"
        
    if "@" not in email or "." not in email:
        return False, "Email must have @ and ."
        
    return True, "Valid"

def check_quantity(quantity, max_allowed):
    """Check if quantity is reasonable"""
    if quantity is None:
        return False, "Quantity is missing"
        
    if quantity <= 0:
        return False, "Quantity must be positive"
        
    if quantity > max_allowed:
        return False, f"Quantity too high (max: {max_allowed})"
        
    return True, "Valid"

def check_price(price, min_price, max_price):
    """Check if price is in valid range"""
    if price is None:
        return False, "Price is missing"
        
    if price < min_price:
        return False, f"Price too low (min: ${min_price})"
        
    if price > max_price:
        return False, f"Price too high (max: ${max_price})"
        
    return True, "Valid"

def check_age(age, min_age, max_age):
    """Check if customer age is reasonable"""
    if age is None:
        return False, "Age is missing"
        
    if age < min_age:
        return False, f"Customer too young (min: {min_age})"
        
    if age > max_age:
        return False, f"Age unrealistic (max: {max_age})"
        
    return True, "Valid"

def clean_name(name):
    """Clean customer name - remove extra spaces and capitalize properly"""
    if not name:
        return ""
    return name.strip().title()

def clean_email(email):
    """Clean email - remove spaces and convert to lowercase"""
    if not email:
        return ""
    return email.strip().lower()

def clean_product_name(product_name):
    """Clean product name - remove extra spaces and capitalize"""
    if not product_name:
        return ""
    return product_name.strip().title()

def calculate_total(quantity, price):
    """Calculate total amount for the order"""
    if quantity is None or price is None:
        return 0.0
    return round(quantity * price, 2)

def calculate_tax(total_amount):
    """Calculate tax (10% of total)"""
    return round(total_amount * 0.10, 2)

def get_order_category(total_amount):
    """Categorize order by total value"""
    if total_amount >= 1000:
        return "HIGH"
    elif total_amount >= 500:
        return "MEDIUM" 
    elif total_amount > 0:
        return "LOW"
    else:
        return "INVALID"