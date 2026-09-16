from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3

app = FastAPI()

DB_PATH = "messages.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class MessageIn(BaseModel):
    sender: str
    text: str

class MessageOut(MessageIn):
    id: int
    category: str
    status: str

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
    category = classify(message.text)
    conn = get_conn()
    cursor = conn.execute(
        "INSERT INTO messages (sender, text, category, status) VALUES (?, ?, ?, ?)",
        (message.sender, message.text, category, "pending"),
    )
    conn.commit()
    new_id = cursor.lastrowid
    conn.close()
    return {"id": new_id, "sender": message.sender, "text": message.text, "category": category, "status": "pending"}

@app.get("/messages")
def list_messages():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM messages").fetchall()
    conn.close()
    return [dict(row) for row in rows]

@app.get("/messages/{message_id}", response_model=MessageOut)
def get_message(message_id: int):
    conn = get_conn()
    row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
    conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Message not found")
    return dict(row)

@app.put("/messages/{message_id}/approve")
def approve_message(message_id: int):
    conn = get_conn()
    row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Message not found")
    conn.execute("UPDATE messages SET status = 'approved' WHERE id = ?", (message_id,))
    conn.commit()
    updated = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
    conn.close()
    return dict(updated)

@app.delete("/messages/{message_id}")
def delete_message(message_id: int):
    conn = get_conn()
    row = conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
    if row is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Message not found")
    conn.execute("DELETE FROM messages WHERE id = ?", (message_id,))
    conn.commit()
    conn.close()
    return {"deleted": message_id}
