class Message:
    def __init__(self, sender, text):
        self.sender = sender
        self.text = text
        self.status = "pending"

    def classify(self):
        t = self.text.lower()
        if "price" in t:
            return "pricing"
        elif "order" in t:
            return "order_status"
        else:
            return "general"

msg = Message("customer_A", "what's the price for 50 bags?")
print(msg.classify())
print(msg.status)
