order_total = 45000
if order_total > 50000:
    print("Flag for approval")
elif order_total > 20000:
    print("Auto-approve with note")
else:
    print("Auto-approve")

messages = ["price list?", "order status", "can I pay on delivery?"]
for msg in messages:
    print("Processing:", msg)

count = 0
while count < 3:
    print("Attempt", count)
    count += 1
