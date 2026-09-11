from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class MessageIn(BaseModel):
    sender: str
    text: str

class MessageOut(MessageIn):
    id: int
    category: str
    status: str

messages_db = {}
next_id = 1

def classify(text: str) -> str:
    t = text.lower()
    if "price" in t:
        return "pricing"
    elif "order" in t:
        return "order_status"
    return "general"

@app.get("/")
def read_root():
    return {"status": "alive"}

@app.post("/messages", response_model=MessageOut)
def create_message(message: MessageIn):
    global next_id
    new_msg = {
        "id": next_id,
        "sender": message.sender,
        "text": message.text,
        "category": classify(message.text),
        "status": "pending",
    }
    messages_db[next_id] = new_msg
    next_id += 1
    return new_msg

@app.get("/messages")
def list_messages():
    return list(messages_db.values())

@app.get("/messages/{message_id}", response_model=MessageOut)
def get_message(message_id: int):
    if message_id not in messages_db:
        raise HTTPException(status_code=404, detail="Message not found")
    return messages_db[message_id]

@app.put("/messages/{message_id}/approve")
def approve_message(message_id: int):
    if message_id not in messages_db:
        raise HTTPException(status_code=404, detail="Message not found")
    messages_db[message_id]["status"] = "approved"
    return messages_db[message_id]

@app.delete("/messages/{message_id}")
def delete_message(message_id: int):
    if message_id not in messages_db:
        raise HTTPException(status_code=404, detail="Message not found")
    del messages_db[message_id]
    return {"deleted": message_id}
