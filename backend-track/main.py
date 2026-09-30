from fastapi import FastAPI, HTTPException, Depends, Header, Request, BackgroundTasks
from pydantic import BaseModel, Field, validator
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
from contextlib import contextmanager
from datetime import datetime, timedelta
from passlib.context import CryptContext
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

import os
import json
import time
import hmac
import hashlib
import secrets
import logging
import urllib.request


app = FastAPI(title="Message Management API", version="2.0.0")
logger = logging.getLogger("messages-api")

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

# ---------------------------------------------------------------------------
# Plans & billing configuration (the monetization layer)
# ---------------------------------------------------------------------------
PLANS = {
    "free": {"rpm": 30, "max_webhooks": 1},
    "pro": {"rpm": 600, "max_webhooks": 10},
    "business": {"rpm": 3000, "max_webhooks": 50},
}

STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
PAYSTACK_WEBHOOK_SECRET = os.environ.get("PAYSTACK_WEBHOOK_SECRET", "")
# Optional JSON mapping of Stripe price id -> plan, e.g. {"price_abc": "pro"}
STRIPE_PRICE_PLANS = json.loads(os.environ.get("STRIPE_PRICE_PLANS", "{}"))
WEBHOOK_TIMEOUT_SECONDS = 5
STRIPE_TIMESTAMP_TOLERANCE = 300


def enforce_rate_limit(scope: str, limit: int) -> None:
    """Fixed-window per-scope rate limit backed by Redis (no-op without Redis)."""
    if redis_client is None:
        return
    key = f"ratelimit:{scope}"
    count = redis_client.incr(key)
    if count == 1:
        redis_client.expire(key, 60)
    if count > limit:
        raise HTTPException(
            status_code=429, detail="Rate limit exceeded. Try again in a minute."
        )


def check_rate_limit(current_user: str = Depends(get_current_user)) -> str:
    enforce_rate_limit(f"user:{current_user}", RATE_LIMIT_PER_MINUTE)
    return current_user


# ---------------------------------------------------------------------------
# API keys & webhook signing helpers
# ---------------------------------------------------------------------------
def generate_api_key() -> str:
    return "sk_live_" + secrets.token_hex(24)


def hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def generate_webhook_secret() -> str:
    return "whsec_" + secrets.token_hex(16)


def sign_webhook_payload(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def verify_stripe_signature(payload: bytes, signature_header: str, secret: str) -> bool:
    """Verify a Stripe-Signature header: t=<timestamp>,v1=<hmac-sha256>."""
    if not secret or not signature_header:
        return False
    try:
        parts = dict(p.split("=", 1) for p in signature_header.split(","))
        timestamp, v1 = parts["t"], parts["v1"]
        if abs(time.time() - int(timestamp)) > STRIPE_TIMESTAMP_TOLERANCE:
            return False
        signed_payload = f"{timestamp}.".encode("utf-8") + payload
        expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, v1)
    except Exception:
        return False


def verify_paystack_signature(payload: bytes, signature: str, secret: str) -> bool:
    """Paystack signs the raw body with HMAC-SHA512."""
    if not secret or not signature:
        return False
    expected = hmac.new(secret.encode("utf-8"), payload, hashlib.sha512).hexdigest()
    return hmac.compare_digest(expected, signature)


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
        conn.execute(
            "CREATE TABLE IF NOT EXISTS organizations ("
            "id SERIAL PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "plan TEXT NOT NULL DEFAULT 'free', "
            "owner_id INTEGER REFERENCES users(id), "
            "created_at TIMESTAMP DEFAULT now())"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS api_keys ("
            "id SERIAL PRIMARY KEY, "
            "org_id INTEGER NOT NULL REFERENCES organizations(id), "
            "name TEXT, "
            "key_hash TEXT UNIQUE NOT NULL, "
            "key_last4 TEXT NOT NULL, "
            "active BOOLEAN NOT NULL DEFAULT TRUE, "
            "created_at TIMESTAMP DEFAULT now())"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS webhooks ("
            "id SERIAL PRIMARY KEY, "
            "org_id INTEGER NOT NULL REFERENCES organizations(id), "
            "url TEXT NOT NULL, "
            "secret TEXT NOT NULL, "
            "active BOOLEAN NOT NULL DEFAULT TRUE, "
            "created_at TIMESTAMP DEFAULT now())"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS solana_signals ("
            "org_id INTEGER NOT NULL REFERENCES organizations(id), "
            "launch_signature TEXT NOT NULL, mint TEXT NOT NULL, pool TEXT NOT NULL, "
            "launch_slot BIGINT NOT NULL, status TEXT NOT NULL, score NUMERIC NOT NULL, "
            "result JSONB NOT NULL, scored_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
            "PRIMARY KEY (org_id, launch_signature))"
        )
        # Extend the original messages table without breaking existing rows.
        conn.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS org_id INTEGER")
        conn.commit()


@app.on_event("startup")
def on_startup():
    init_db()


# ---------------------------------------------------------------------------
# Original user/auth/message endpoints (unchanged behaviour)
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Multi-tenant SaaS layer: organizations, API keys, webhooks, billing
# ---------------------------------------------------------------------------
class OrgIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)


class ApiKeyIn(BaseModel):
    name: str = Field(default="default", max_length=120)


class WebhookIn(BaseModel):
    url: str = Field(..., min_length=8, max_length=2048)

    @validator("url")
    def valid_url(cls, v):
        if not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v


def _get_user_row(conn, username: str):
    return conn.execute(
        "SELECT * FROM users WHERE username = ?", (username,)
    ).fetchone()


def _require_org_owner(conn, org_id: int, username: str):
    org = conn.execute(
        "SELECT * FROM organizations WHERE id = ?", (org_id,)
    ).fetchone()
    if org is None:
        raise HTTPException(status_code=404, detail="Organization not found")
    user = _get_user_row(conn, username)
    if user is None or org["owner_id"] != user["id"]:
        raise HTTPException(status_code=403, detail="Not your organization")
    return org


def get_org_by_api_key(x_api_key: str | None = Header(default=None)) -> dict:
    """Resolve an organization from the X-API-Key header."""
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing API key (send X-API-Key header)")
    with get_db() as conn:
        row = conn.execute(
            "SELECT ak.id AS key_id, o.id AS org_id, o.name AS org_name, o.plan "
            "FROM api_keys ak JOIN organizations o ON o.id = ak.org_id "
            "WHERE ak.key_hash = ? AND ak.active = TRUE",
            (hash_api_key(x_api_key),),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return dict(row)


@app.post("/orgs")
def create_org(org: OrgIn, current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        user = _get_user_row(conn, current_user)
        cursor = conn.execute(
            "INSERT INTO organizations (name, owner_id) VALUES (?, ?) RETURNING id, name, plan, created_at",
            (org.name, user["id"]),
        )
        conn.commit()
        new_org = dict(cursor.fetchone())
    new_org["created_at"] = str(new_org["created_at"])
    return new_org


@app.get("/orgs")
def list_orgs(current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        user = _get_user_row(conn, current_user)
        rows = conn.execute(
            "SELECT id, name, plan, created_at FROM organizations WHERE owner_id = ? ORDER BY id",
            (user["id"],),
        ).fetchall()
    return [
        {**dict(r), "created_at": str(r["created_at"])} for r in rows
    ]


@app.get("/orgs/{org_id}")
def get_org(org_id: int, current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        org = _require_org_owner(conn, org_id, current_user)
        keys = conn.execute(
            "SELECT COUNT(*) AS n FROM api_keys WHERE org_id = ? AND active = TRUE", (org_id,)
        ).fetchone()["n"]
        hooks = conn.execute(
            "SELECT COUNT(*) AS n FROM webhooks WHERE org_id = ? AND active = TRUE", (org_id,)
        ).fetchone()["n"]
    plan = PLANS.get(org["plan"], PLANS["free"])
    return {
        "id": org["id"],
        "name": org["name"],
        "plan": org["plan"],
        "limits": plan,
        "active_api_keys": keys,
        "active_webhooks": hooks,
        "created_at": str(org["created_at"]),
    }


@app.post("/orgs/{org_id}/keys")
def create_api_key(org_id: int, body: ApiKeyIn, current_user: str = Depends(get_current_user)):
    key = generate_api_key()
    with get_db() as conn:
        _require_org_owner(conn, org_id, current_user)
        cursor = conn.execute(
            "INSERT INTO api_keys (org_id, name, key_hash, key_last4) "
            "VALUES (?, ?, ?, ?) RETURNING id, name, key_last4, created_at",
            (org_id, body.name, hash_api_key(key), key[-4:]),
        )
        conn.commit()
        row = dict(cursor.fetchone())
    return {
        "id": row["id"],
        "name": row["name"],
        "key": key,  # shown exactly once — never stored in plaintext
        "key_last4": row["key_last4"],
        "created_at": str(row["created_at"]),
    }


@app.get("/orgs/{org_id}/keys")
def list_api_keys(org_id: int, current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        _require_org_owner(conn, org_id, current_user)
        rows = conn.execute(
            "SELECT id, name, key_last4, active, created_at FROM api_keys "
            "WHERE org_id = ? ORDER BY id",
            (org_id,),
        ).fetchall()
    return [
        {**dict(r), "created_at": str(r["created_at"])} for r in rows
    ]


@app.delete("/orgs/{org_id}/keys/{key_id}")
def revoke_api_key(org_id: int, key_id: int, current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        _require_org_owner(conn, org_id, current_user)
        row = conn.execute(
            "SELECT * FROM api_keys WHERE id = ? AND org_id = ?", (key_id, org_id)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="API key not found")
        conn.execute("UPDATE api_keys SET active = FALSE WHERE id = ?", (key_id,))
        conn.commit()
    return {"revoked": key_id}


@app.post("/orgs/{org_id}/webhooks")
def create_webhook(org_id: int, body: WebhookIn, current_user: str = Depends(get_current_user)):
    secret = generate_webhook_secret()
    with get_db() as conn:
        org = _require_org_owner(conn, org_id, current_user)
        plan = PLANS.get(org["plan"], PLANS["free"])
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM webhooks WHERE org_id = ? AND active = TRUE", (org_id,)
        ).fetchone()["n"]
        if count >= plan["max_webhooks"]:
            raise HTTPException(
                status_code=402,
                detail=f"Plan '{org['plan']}' allows {plan['max_webhooks']} webhook(s). Upgrade to add more.",
            )
        cursor = conn.execute(
            "INSERT INTO webhooks (org_id, url, secret) VALUES (?, ?, ?) "
            "RETURNING id, url, created_at",
            (org_id, body.url, secret),
        )
        conn.commit()
        row = dict(cursor.fetchone())
    return {
        "id": row["id"],
        "url": row["url"],
        "secret": secret,  # shown exactly once
        "created_at": str(row["created_at"]),
    }


@app.get("/orgs/{org_id}/webhooks")
def list_webhooks(org_id: int, current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        _require_org_owner(conn, org_id, current_user)
        rows = conn.execute(
            "SELECT id, url, active, created_at FROM webhooks WHERE org_id = ? ORDER BY id",
            (org_id,),
        ).fetchall()
    return [{**dict(r), "created_at": str(r["created_at"])} for r in rows]


@app.delete("/orgs/{org_id}/webhooks/{webhook_id}")
def delete_webhook(org_id: int, webhook_id: int, current_user: str = Depends(get_current_user)):
    with get_db() as conn:
        _require_org_owner(conn, org_id, current_user)
        row = conn.execute(
            "SELECT * FROM webhooks WHERE id = ? AND org_id = ?", (webhook_id, org_id)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Webhook not found")
        conn.execute("DELETE FROM webhooks WHERE id = ?", (webhook_id,))
        conn.commit()
    return {"deleted": webhook_id}


def dispatch_webhook(url: str, secret: str, payload: dict) -> None:
    """Fire one outbound webhook. Runs as a background task; never raises."""
    body = json.dumps(payload, default=str).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Event": payload.get("event", ""),
            "X-Webhook-Signature": sign_webhook_payload(secret, body),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=WEBHOOK_TIMEOUT_SECONDS) as resp:
            resp.read()
    except Exception as exc:  # noqa: BLE001 - delivery failure must not break the API
        logger.warning("webhook delivery failed for %s: %s", url, exc)


class ApiMessageOut(MessageOut):
    org_id: int


@app.post("/v1/messages", response_model=ApiMessageOut)
def api_create_message(
    message: MessageIn,
    background_tasks: BackgroundTasks,
    org: dict = Depends(get_org_by_api_key),
):
    """Create a message using an organization API key (plan-gated)."""
    plan = PLANS.get(org["plan"], PLANS["free"])
    enforce_rate_limit(f"org:{org['org_id']}", plan["rpm"])
    category = classify(message.text)
    with get_db() as conn:
        cursor = conn.execute(
            "INSERT INTO messages (sender, text, category, status, org_id) "
            "VALUES (?, ?, ?, ?, ?) RETURNING id",
            (message.sender, message.text, category, "pending", org["org_id"]),
        )
        conn.commit()
        new_id = cursor.fetchone()["id"]
        hooks = conn.execute(
            "SELECT url, secret FROM webhooks WHERE org_id = ? AND active = TRUE",
            (org["org_id"],),
        ).fetchall()
    result = {
        "id": new_id,
        "sender": message.sender,
        "text": message.text,
        "category": category,
        "status": "pending",
        "org_id": org["org_id"],
    }
    payload = {
        "event": "message.created",
        "created_at": datetime.utcnow().isoformat() + "Z",
        "data": result,
    }
    for hook in hooks:
        background_tasks.add_task(dispatch_webhook, hook["url"], hook["secret"], payload)
    return result


@app.get("/v1/messages")
def api_list_messages(org: dict = Depends(get_org_by_api_key)):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, sender, text, category, status, org_id FROM messages "
            "WHERE org_id = ? ORDER BY id",
            (org["org_id"],),
        ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Billing webhook receivers (plan switching)
# ---------------------------------------------------------------------------
def set_org_plan(org_id: int, plan: str) -> None:
    if plan not in PLANS:
        raise HTTPException(status_code=400, detail=f"Unknown plan: {plan}")
    with get_db() as conn:
        row = conn.execute(
            "UPDATE organizations SET plan = ? WHERE id = ? RETURNING id", (plan, org_id)
        ).fetchone()
        conn.commit()
    if row is None:
        raise HTTPException(status_code=404, detail="Organization not found")


@app.post("/billing/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    signature_header = request.headers.get("Stripe-Signature", "")
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=400, detail="STRIPE_WEBHOOK_SECRET not configured")
    if not verify_stripe_signature(payload, signature_header, STRIPE_WEBHOOK_SECRET):
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")

    event = json.loads(payload)
    event_type = event.get("type", "")
    obj = event.get("data", {}).get("object", {}) or {}
    metadata = obj.get("metadata") or {}
    org_id = metadata.get("org_id") or obj.get("client_reference_id")

    if event_type == "checkout.session.completed":
        plan = metadata.get("plan")
        if not (org_id and plan):
            raise HTTPException(
                status_code=400,
                detail="checkout session must carry org_id and plan in metadata",
            )
        set_org_plan(int(org_id), plan)
    elif event_type in ("customer.subscription.updated", "customer.subscription.created"):
        plan = metadata.get("plan")
        if not plan:
            items = (obj.get("items") or {}).get("data") or []
            price_id = ((items[0].get("price") or {}).get("id")) if items else None
            plan = STRIPE_PRICE_PLANS.get(price_id)
        if not (org_id and plan):
            raise HTTPException(
                status_code=400,
                detail="subscription must carry org_id in metadata or a mapped price id",
            )
        set_org_plan(int(org_id), plan)
    elif event_type == "customer.subscription.deleted":
        if not org_id:
            raise HTTPException(status_code=400, detail="subscription must carry org_id")
        set_org_plan(int(org_id), "free")
    else:
        return {"received": True, "ignored": event_type}
    return {"received": True, "applied": event_type}


@app.post("/billing/paystack/webhook")
async def paystack_webhook(request: Request):
    payload = await request.body()
    signature = request.headers.get("x-paystack-signature", "")
    if not PAYSTACK_WEBHOOK_SECRET:
        raise HTTPException(status_code=400, detail="PAYSTACK_WEBHOOK_SECRET not configured")
    if not verify_paystack_signature(payload, signature, PAYSTACK_WEBHOOK_SECRET):
        raise HTTPException(status_code=400, detail="Invalid Paystack signature")

    event = json.loads(payload)
    event_type = event.get("event", "")
    data = event.get("data") or {}
    metadata = data.get("metadata") or {}
    org_id = metadata.get("org_id")
    plan = metadata.get("plan")

    if event_type == "subscription.create":
        if data.get("status") == "active" and org_id and plan:
            set_org_plan(int(org_id), plan)
        else:
            return {"received": True, "ignored": "inactive-or-missing-metadata"}
    elif event_type == "subscription.disable":
        if not org_id:
            raise HTTPException(status_code=400, detail="subscription must carry org_id in metadata")
        set_org_plan(int(org_id), "free")
    elif event_type == "charge.success":
        # One-time purchases can also upgrade a plan via metadata.
        if org_id and plan:
            set_org_plan(int(org_id), plan)
        else:
            return {"received": True, "ignored": "no-plan-metadata"}
    else:
        return {"received": True, "ignored": event_type}
    return {"received": True, "applied": event_type}

# ---------------------------------------------------------------------------
# Read-only Solana intelligence: verified evidence submitted by an indexer.
# This endpoint does not place orders or claim to discover pools automatically.
# ---------------------------------------------------------------------------
from solana_signals import EvidenceBundle, score_bundle


@app.post('/v1/solana/signals')
def create_solana_signal(bundle: EvidenceBundle, org: dict = Depends(get_org_by_api_key)):
    enforce_rate_limit(f"org:{org['org_id']}:signals", PLANS[org['plan']]['rpm'])
    signal = score_bundle(bundle)
    with get_db() as conn:
        conn.execute(
            "INSERT INTO solana_signals (org_id, launch_signature, mint, pool, launch_slot, status, score, result) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT (org_id, launch_signature) DO UPDATE SET "
            "status=EXCLUDED.status, score=EXCLUDED.score, result=EXCLUDED.result, scored_at=now()",
            (org['org_id'], signal['launch_signature'], signal['mint'], signal['pool'],
             signal['launch_slot'], signal['status'], signal['score'], json.dumps(signal)),
        )
        conn.commit()
    return signal


@app.get('/v1/solana/signals')
def list_solana_signals(org: dict = Depends(get_org_by_api_key)):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT result FROM solana_signals WHERE org_id=%s ORDER BY scored_at DESC LIMIT 100",
            (org['org_id'],),
        ).fetchall()
    return [row['result'] for row in rows]
