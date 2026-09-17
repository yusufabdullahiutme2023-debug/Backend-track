# Backend Track — Customer Message Triage API

A progressive Python backend learning track that builds, day by day, toward a working
**customer message triage service**: messages come in, get automatically classified,
are persisted to SQLite, and can be listed, approved, or deleted over a REST API.

Built with **FastAPI**, **Pydantic**, and **SQLite**.

---

## What the project does

A customer message arrives with a `sender` and some `text`. The backend:

1. **Validates** it (non-empty, not whitespace-only, length-capped).
2. **Classifies** it by keyword into one of three categories.
3. **Stores** it in SQLite with a lifecycle `status` that starts as `pending`.
4. **Exposes** it over REST so an operator can review, approve, or delete it.

### Classification rules

Implemented in `classify()` in `main.py` (and as `Message.classify()` in the earlier exercises).
Matching is case-insensitive and checked in this order:

| If the message text contains… | Category |
| --- | --- |
| `price` | `pricing` |
| `order` | `order_status` |
| anything else | `general` |

### Message lifecycle

```
POST /messages  ──►  pending  ──PUT /messages/{id}/approve──►  approved
                        │
                        └──DELETE /messages/{id}──►  (removed)
```

---

## The learning track

Each file is a step. They are meant to be read in order — every one adds a skill the
final API depends on.

| Step | File | Concept introduced |
| --- | --- | --- |
| Day 6 | [`backend-track/day6.py`](backend-track/day6.py) | Classes & OOP — a `Message` object with `sender`, `text`, `status` and a `classify()` method |
| Day 7 | [`backend-track/day7.py`](backend-track/day7.py) | File I/O — appending to and reading back `log.txt` |
| Day 8 | [`backend-track/day8.py`](backend-track/day8.py) | JSON serialisation — `json.dump()` / `json.load()` round-trip |
| Day 9 | [`backend-track/day9.py`](backend-track/day9.py) | HTTP clients — calling the GitHub API with `requests` and reading `status_code` |
| Week 2 | [`backend-track/week2_project.py`](backend-track/week2_project.py) | Putting it together — batch-classify a list of messages and write an audit log |
| Week 5 | [`backend-track/main.py`](backend-track/main.py) | The real service — FastAPI CRUD endpoints, Pydantic validation, SQLite persistence |

### What Week 5 added

- **Input hardening** — a Pydantic validator rejects blank or whitespace-only `sender` /
  `text`, and `.strip()`s what it accepts. `Field(...)` enforces `min_length=1`,
  `max_length=100` for `sender` and `max_length=2000` for `text`.
- **Guaranteed connection cleanup** — `get_db()` is a `@contextmanager` around
  `sqlite3.connect()`, so the connection closes in a `finally` block even when a query
  raises. Every endpoint uses `with get_db() as conn:`.
- **Typed responses** — `MessageOut` (`id`, `sender`, `text`, `category`, `status`)
  documents and enforces the shape of `POST` / `GET /{id}` responses.

---

## Project structure

```
Backend-track/
├── README.md                  ← you are here
├── .gitignore                 ← repo-level: caches, credentials, messages.db, shell history
└── backend-track/
    ├── .gitignore             ← code-level: venv/, __pycache__/, log.txt, message.json
    ├── main.py                ← FastAPI application (the deliverable)
    ├── week2_project.py       ← batch classification exercise
    ├── day6.py … day9.py      ← daily concept exercises
    ├── messages.db            ← SQLite database (created at runtime, git-ignored)
    ├── log.txt                ← written by day7 / week2 (git-ignored)
    └── message.json           ← written by day8 (git-ignored)
```

---

## Getting started

### 1. Install dependencies

Python 3.11+ recommended.

```bash
pip install fastapi "uvicorn[standard]" requests
```

(`requests` is only needed for `day9.py`.)

### 2. Create the database table

⚠️ **Known gap:** `main.py` reads and writes a `messages` table but does not create it.
Until schema initialisation is added to the app (see [Roadmap](#roadmap--next-steps)),
create it once by hand from inside `backend-track/`:

```bash
sqlite3 messages.db <<'SQL'
CREATE TABLE IF NOT EXISTS messages (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sender   TEXT NOT NULL,
    text     TEXT NOT NULL,
    category TEXT NOT NULL,
    status   TEXT NOT NULL DEFAULT 'pending'
);
SQL
```

No `sqlite3` CLI? The same thing in Python:

```bash
python - <<'PY'
import sqlite3
with sqlite3.connect("messages.db") as conn:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            text TEXT NOT NULL,
            category TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
        )
    """)
PY
```

### 3. Run the API

```bash
cd backend-track
uvicorn main:app --reload
```

Then open:

- **Interactive docs (Swagger UI):** http://127.0.0.1:8000/docs
- **Alternative docs (ReDoc):** http://127.0.0.1:8000/redoc
- **Health check:** http://127.0.0.1:8000/

### 4. Run the earlier exercises (optional)

```bash
cd backend-track
python day6.py             # prints a category and a status
python day7.py             # appends to, then prints, log.txt
python day8.py             # writes and reloads message.json
python day9.py             # GETs https://api.github.com, prints the status code
python week2_project.py    # classifies 3 sample messages, appends them to log.txt
```

---

## API reference

Base URL: `http://127.0.0.1:8000`

### `GET /` — health check

```bash
curl http://127.0.0.1:8000/
```

```json
{ "status": "alive" }
```

### `POST /messages` — create and classify a message

```bash
curl -X POST http://127.0.0.1:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"sender": "customer_A", "text": "what is the price for 50 bags of rice?"}'
```

```json
{
  "id": 1,
  "sender": "customer_A",
  "text": "what is the price for 50 bags of rice?",
  "category": "pricing",
  "status": "pending"
}
```

Request body:

| Field | Type | Rules |
| --- | --- | --- |
| `sender` | string | required, 1–100 chars, no blank/whitespace-only, trimmed |
| `text` | string | required, 1–2000 chars, no blank/whitespace-only, trimmed |

Errors: `422 Unprocessable Entity` when validation fails.

### `GET /messages` — list all messages

```bash
curl http://127.0.0.1:8000/messages
```

```json
[
  { "id": 1, "sender": "customer_A", "text": "…", "category": "pricing", "status": "pending" }
]
```

### `GET /messages/{id}` — fetch one message

```bash
curl http://127.0.0.1:8000/messages/1
```

Returns the message, or `404 {"detail": "Message not found"}`.

### `PUT /messages/{id}/approve` — approve a message

```bash
curl -X PUT http://127.0.0.1:8000/messages/1/approve
```

```json
{ "id": 1, "sender": "customer_A", "text": "…", "category": "pricing", "status": "approved" }
```

Returns `404` if the id does not exist.

### `DELETE /messages/{id}` — delete a message

```bash
curl -X DELETE http://127.0.0.1:8000/messages/1
```

```json
{ "deleted": 1 }
```

Returns `404` if the id does not exist.

---

## Roadmap / next steps

- [ ] **Schema initialisation** — create the `messages` table automatically on app
      startup (a `lifespan` handler or `init_db()` called from one) so a fresh clone
      runs with zero manual SQL. *This is the current blocker for a clean setup.*
- [ ] **`requirements.txt`** — pin `fastapi`, `uvicorn`, `requests` (and dev deps).
- [ ] **Pydantic v2 spelling** — `@validator` is deprecated; migrate to
      `@field_validator("sender", "text")`.
- [ ] **Tests** — `pytest` + `httpx.AsyncClient`/`TestClient` covering validation
      rejections, each classification branch, the 404 paths, and the approve transition.
- [ ] **Pagination & filtering** — `GET /messages?limit=&offset=&category=&status=`.
- [ ] **Repository/service layer** — move SQL out of the route handlers.
- [ ] **Richer classification** — scoring or a small ML model instead of substring matching.
- [ ] **Auth** — protect `PUT /approve` and `DELETE` behind an API key or JWT.

---

## Notes

- Generated artefacts are git-ignored, so exercises can be re-run freely without
  dirtying the repo. The rules live in two files:
  - `backend-track/.gitignore` — `venv/`, `__pycache__/`, `*.pyc`, `log.txt`, `message.json`
  - root `.gitignore` — `messages.db`, `.cache/`, `.ssh/`, `.termux/`, `.gitconfig`,
    `.git-credentials`, `.python_history`, `.lesshst`
- Commit history mirrors the track: each step lands with a message naming the week and
  the concept it adds.
- Tidying opportunity: the root `.gitignore` repeats `messages.db` and `.python_history`,
  and duplicates several rules already covered by `backend-track/.gitignore`.
