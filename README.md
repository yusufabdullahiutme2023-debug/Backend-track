# Backend Track — Customer Message Triage API

A progressive Python backend learning track that builds, day by day, toward a working
**customer message triage service**: messages come in, get automatically classified,
are persisted to SQLite, and can be listed, approved, or deleted over a REST API.

Built with **FastAPI**, **Pydantic v2**, and **SQLite**. Covered by **31 pytest tests**.

---

## What the project does

A customer message arrives with a `sender` and some `text`. The backend:

1. **Validates** it (non-empty, not whitespace-only, length-capped, trimmed).
2. **Classifies** it by keyword into one of three categories.
3. **Stores** it in SQLite with a lifecycle `status` that starts as `pending`.
4. **Exposes** it over REST so an operator can review, approve, or delete it.

### Classification rules

Implemented in `classify()` in `main.py` (and as `Message.classify()` in the earlier exercises).
Matching is case-insensitive and checked in this order, so a message containing both
keywords is classified as `pricing`:

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
| Week 6 | [`backend-track/test_main.py`](backend-track/test_main.py) | Automated testing — pytest fixtures, parametrised cases, an isolated database per test |

### What Week 5 added

- **Input hardening** — a Pydantic validator rejects blank or whitespace-only `sender` /
  `text`, and `.strip()`s what it accepts. `Field(...)` enforces `min_length=1`,
  `max_length=100` for `sender` and `max_length=2000` for `text`.
- **Guaranteed connection cleanup** — `get_db()` is a `@contextmanager` around
  `sqlite3.connect()`, so the connection closes in a `finally` block even when a query
  raises. Every endpoint uses `with get_db() as conn:`.
- **Typed responses** — `MessageOut` (`id`, `sender`, `text`, `category`, `status`)
  documents and enforces the shape of the API's responses.

### What Week 6 added

- **Automatic schema creation** — `init_db()` runs from a FastAPI `lifespan` hook at
  startup, so `messages.db` and its `messages` table are created on a fresh clone with
  no manual SQL. Previously the app crashed with `no such table: messages`.
- **Pydantic v2 migration** — `@validator` → `@field_validator(...)` + `@classmethod`,
  clearing the deprecation warning (`@validator` is removed in Pydantic v3).
- **Dependency pinning** — `requirements.txt` and `requirements-dev.txt`.
- **Test suite** — 31 tests covering schema init, every classification branch,
  validation rejections, trimming, length limits, 404 paths, the approve transition
  and its persistence, and delete isolation.
- **Correct HTTP semantics** — `POST /messages` now returns `201 Created`; list,
  single-get and approve responses are typed with `response_model`.
- **Configurable database path** — set `MESSAGES_DB_PATH` to point the app elsewhere
  (this is how the tests isolate themselves).

---

## Project structure

```
Backend-track/
├── README.md                  ← you are here
├── verify.sh                  ← one-command proof script (see Getting started, step 3)
├── .gitignore                 ← repo-level: caches, credentials, messages.db, shell history
└── backend-track/
    ├── .gitignore             ← code-level: venv/, __pycache__/, log.txt, message.json
    ├── main.py                ← FastAPI application (the deliverable)
    ├── test_main.py           ← pytest suite (31 tests)
    ├── requirements.txt       ← runtime dependencies
    ├── requirements-dev.txt   ← test dependencies
    ├── week2_project.py       ← batch classification exercise
    ├── day6.py … day9.py      ← daily concept exercises
    ├── messages.db            ← SQLite database (created at startup, git-ignored)
    ├── log.txt                ← written by day7 / week2 (git-ignored)
    └── message.json           ← written by day8 (git-ignored)
```

---

## Getting started

Python 3.11+ recommended.

### 1. Install dependencies

```bash
cd backend-track
pip install -r requirements.txt              # runtime
pip install -r requirements-dev.txt          # optional, for the tests
```

### 2. Run the API

```bash
uvicorn main:app --reload
```

or simply:

```bash
python main.py
```

The `messages` table is created automatically on startup — there is nothing to set up.

Then open:

- **Interactive docs (Swagger UI):** http://127.0.0.1:8000/docs
- **Alternative docs (ReDoc):** http://127.0.0.1:8000/redoc
- **Health check:** http://127.0.0.1:8000/

### 3. Verify everything in one command

```bash
bash verify.sh
```

`verify.sh` (repo root) is a self-contained proof script. It prints who/where/when it
ran and the git push status, lists the dependency versions, **reproduces the original
`no such table: messages` crash using your Week 5 `main.py` pulled straight out of git
history**, then shows the current code working under identical conditions, runs pytest,
and boots uvicorn against a throwaway database to drive every route with `curl`.

Each assertion prints `PASS`/`FAIL` with the expected and actual value; the exit code is
the number of failures (`0` = everything passed). It never touches your real
`messages.db`, and it can be pointed at another port with `VERIFY_PORT=9000 bash verify.sh`.

A negative control confirms the checks are real: reverting only `backend-track/main.py`
to commit `bd62303` makes the same script report `passed: 6  failed: 19`.

### 4. Run the tests

```bash
cd backend-track
pytest -v
```

```
31 passed
```

Each test points `main.DB_PATH` at a temporary file, so your real `messages.db` is
never touched. To start from a clean database manually, just delete `messages.db`
(it is regenerated on the next startup).

### 5. Run the earlier exercises (optional)

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

### `POST /messages` — create and classify a message → `201 Created`

```bash
curl -X POST http://127.0.0.1:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"sender": "customer_A", "text": "what is the price for 50 bags of rice?"}'
```

```json
{
  "sender": "customer_A",
  "text": "what is the price for 50 bags of rice?",
  "id": 1,
  "category": "pricing",
  "status": "pending"
}
```

Request body:

| Field | Type | Rules |
| --- | --- | --- |
| `sender` | string | required, 1–100 chars, no blank/whitespace-only, trimmed |
| `text` | string | required, 1–2000 chars, no blank/whitespace-only, trimmed |

Errors: `422 Unprocessable Entity` when validation fails, e.g.

```json
{
  "detail": [
    {
      "type": "value_error",
      "loc": ["body", "sender"],
      "msg": "Value error, must not be blank or whitespace-only",
      "input": "   "
    }
  ]
}
```

### `GET /messages` — list all messages

```bash
curl http://127.0.0.1:8000/messages
```

Returns a JSON array ordered by `id`, empty (`[]`) on a fresh database.

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
{
  "sender": "customer_A",
  "text": "what is the price for 50 bags of rice?",
  "id": 1,
  "category": "pricing",
  "status": "approved"
}
```

Idempotent — approving twice is fine. Returns `404` if the id does not exist.

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

- [x] **Schema initialisation** — `init_db()` on app startup via a `lifespan` hook.
- [x] **`requirements.txt`** — runtime and dev dependencies pinned.
- [x] **Pydantic v2 spelling** — migrated to `@field_validator`.
- [x] **Tests** — 31 pytest cases covering validation, classification, 404s and the
      approve transition.
- [ ] **Pagination & filtering** — `GET /messages?limit=&offset=&category=&status=`.
- [ ] **Repository/service layer** — move SQL out of the route handlers.
- [ ] **Reject → resolved statuses** — a fuller lifecycle than `pending`/`approved`
      (e.g. `rejected`, `replied`) with transition rules.
- [ ] **Richer classification** — scoring or a small ML model instead of substring
      matching; make categories data-driven rather than hardcoded.
- [ ] **Auth** — protect `PUT /approve` and `DELETE` behind an API key or JWT.
- [ ] **CI** — a GitHub Action running `pytest` on push.
- [ ] **Containerise** — a `Dockerfile` plus a volume for `messages.db`.

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
- Verified against FastAPI 0.141, Pydantic 2.13, Starlette 1.6, uvicorn 0.53 on Python 3.11.
