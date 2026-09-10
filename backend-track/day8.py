import json

message = {"from": "customer_A", "text": "price check", "status": "pending"}

with open("message.json", "w") as f:
    json.dump(message, f)

with open("message.json", "r") as f:
    loaded = json.load(f)
print(loaded)
