class Message:
    def __init__(self, sender, text):
        self.sender = sender
        self.text = text

    def classify(self):
        t = self.text.lower()
        if "price" in t:
            return "pricing"
        elif "order" in t:
            return "order_status"
        return "general"

messages = [
    Message("customer_A", "what's the price for 50 bags of rice?"),
    Message("customer_B", "where is my order #221?"),
    Message("customer_C", "do you deliver to Kano?"),
]

for m in messages:
    category = m.classify()
    line = f"From {m.sender}: [{category}] {m.text}\n"
    print(line.strip())
    with open("log.txt", "a") as f:
        f.write(line)
