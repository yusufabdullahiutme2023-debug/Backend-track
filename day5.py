def classify_message(text):
    if "price" in text.lower():
        return "pricing"
    elif "order" in text.lower():
        return "order_status"
    else:
        return "general"

print(classify_message("what's the price for 50 bags?"))
print(classify_message("where is my order"))

def safe_divide(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return "Error: cannot divide by zero"

print(safe_divide(10, 2))
print(safe_divide(10, 0))
