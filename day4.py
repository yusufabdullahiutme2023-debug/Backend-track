orders = ["order_1", "order_2", "order_3"]
print(orders[0])
orders.append("order_4")
print(orders)

message = {
    "from": "customer_A",
    "text": "what's the price for 50 bags?",
    "status": "pending"
}
print(message["text"])
message["status"] = "answered"
print(message)
