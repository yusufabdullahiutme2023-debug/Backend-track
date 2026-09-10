messages = [
    {"from": "customer_A", "text": "what's the price for 50 bags of rice?"},
    {"from": "customer_B", "text": "where is my order #221?"},
    {"from": "customer_C", "text": "do you deliver to Kano?"},
]

def classify_message(text):
    text = text.lower()
    if "price" in text:
        return "pricing"
    elif "order" in text:
        return "order_status"
    else:
        return "general"

for m in messages:
    category = classify_message(m["text"])
    print(f"From {m['from']}: [{category}] {m['text']}")
