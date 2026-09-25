# Python Backend API

A message-management API built with FastAPI, featuring JWT authentication and a PostgreSQL database. Deployed live on Render with automated testing via GitHub Actions.

**Live API:** https://backend-track-nz8c.onrender.com
(Free-tier instance — the first request after inactivity may take up to a minute.)

## Stack

- **Framework:** FastAPI (Python)
- **Database:** PostgreSQL (hosted on Neon)
- **Auth:** JWT via python-jose, password hashing via passlib/bcrypt
- **Deployment:** Docker, Render
- **CI/CD:** GitHub Actions — builds the Docker image, spins up a Postgres service container, and runs a live smoke test (register → login → create → list) on every push
- **Rate limiting:** per-user request throttling via Redis (Upstash), configurable via `RATE_LIMIT_PER_MINUTE`

## Endpoints

| Method | Path | Auth required | Description |
|---|---|---|---|
| POST | `/register` | No | Create a new user account |
| POST | `/token` | No | Log in, returns a JWT access token |
| GET | `/` | No | Health check |
| POST | `/messages` | Yes | Submit a message (auto-classified into a category) |
| GET | `/messages` | No | List all messages |
| GET | `/messages/{message_id}` | No | Get a single message |
| PUT | `/messages/{message_id}/approve` | Yes | Mark a message approved |
| DELETE | `/messages/{message_id}` | Yes | Delete a message |

## Run locally

```bash
git clone https://github.com/yusufabdullahiutme2023-debug/Backend-track.git
cd Backend-track/backend-track
pip install -r requirements.txt

export DATABASE_URL='postgresql://user:password@host/dbname'
export SECRET_KEY='your-own-secret-key'

uvicorn main:app --reload
```

The app creates its tables automatically on startup.

## Example usage

```bash
# Register
curl -X POST "http://localhost:8000/register?username=alice&password=secret123"

# Log in
curl -X POST http://localhost:8000/token -d "username=alice&password=secret123"

# Create a message (replace TOKEN with the access_token from above)
curl -X POST http://localhost:8000/messages \
  -H "Authorization: Bearer TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sender":"alice","text":"what is the price"}'
```
