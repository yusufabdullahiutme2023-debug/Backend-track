with open("log.txt", "a") as f:
    f.write("Message from customer_A classified as pricing\n")

with open("log.txt", "r") as f:
    print(f.read())
