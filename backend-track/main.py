from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field, validator
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
from contextlib import contextmanager
from datetime import datetime, timedelta
from passlib.context import CryptContext
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm


app = FastAPI()

import os
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-only-insecure-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    credentials_error = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise credentials_error
    except JWTError:
        raise credentials_error
    return username


DATABASE_URL = os.environ["DATABASE_URL"]

REDIS_URL = os.environ.get("REDIS_URL")
redis_client = redis.from_url(REDIS_URL) if REDIS_URL else None
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "30"))


def check_rate_limit(current_user: str = Depends(get_current_user)) -> str:
    if redis_client is None:
        return current_user
    key = f"ratelimit:{current_user}"
    count = redis_client.incr(key)
    if count == 1:
        redis_client.expire(key, 60)
    if count > RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again in a minute.")
    return current_user


class DB:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=()):
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql.replace("?", "%s"), params)
        return cur

    def commit(self):
        self.conn.commit()


@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield DB(conn)
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages ("
            "id SERIAL PRIMARY KEY, "
            "sender TEXT NOT NULL, "
            "text TEXT NOT NULL, "
            "category TEXT, "
            "status TEXT)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "id SERIAL PRIMARY KEY, "
            "username TEXT UNIQUE NOT NULL, "
            "hashed_password TEXT NOT NULL)"
        )
        conn.commit()


init_db()


@app.post("/register")
def register(username: str, password: str):
    with get_db() as conn:
        existing = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,)
        ).fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="Username already taken")
        conn.execute(
            "INSERT INTO users (username, hashed_password) VALUES (?, ?)",
            (username, hash_password(password)),
        )
        conn.commit()
    return {"username": username, "registered": True}


@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    with get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE username = ?", (form_data.username,)
        ).fetchone()
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    token = create_access_token({"sub": user["username"]})
    return {"access_token": token, "token_type": "bearer"}



class MessageIn(BaseModel):
    sender: str = Field(..., min_length=1, max_length=100)
    text: str = Field(..., min_length=1, max_length=2000)

    @validator("sender", "text")
    def not_blank(cls, v):
        if not v.strip():
            raise ValueError("must not be blank or whitespace-only")
        return v.strip()


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
def create_message(message: MessageIn, current_user: str = Depends(check_rate_limit)):
    category = classify(message.text)
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO messages (sender, text, category, status) VALUES (?, ?, ?, ?) RETURNING id",
            (message.sender, message.text, category, "pending"),
        )
        conn.commit()
        new_id = cursor.fetchone()["id"]
    return {
        "id": new_id,
        "sender": message.sender,
        "text": message.text,
        "category": category,
        "status": "pending",
    }


@app.get("/messages")
def list_messages():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM messages").fetchall()
    return [dict(row) for row in rows]


@app.get("/messages/{message_id}", response_model=MessageOut)
def get_message(message_id: int):
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM messages WHERE id = ?", (message_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Message not found")
    return dict(row)


@app.put("/messages/{message_id}/approve")
def approve_message(message_id: int, current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM messages WHERE id = ?", (message_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Message not found")
        conn.execute(
            "UPDATE messages SET status = 'approved' WHERE id = ?", (message_id,)
        )
        conn.commit()
        updated = conn.execute(
            "SELECT * FROM messages WHERE id = ?", (message_id,)
        ).fetchone()
    return dict(updated)


@app.delete("/messages/{message_id}")
def delete_message(message_id: int, current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM messages WHERE id = ?", (message_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Message not found")
        conn.execute("DELETE FROM messages WHERE id = ?", (message_id,))
        conn.commit()
    return {"deleted": message_id}
