# Python Backend API — Multi-Tenant SaaS Starter

A message-management API built with FastAPI, featuring JWT authentication, a PostgreSQL database, and a complete **multi-tenant SaaS layer**: organizations, plan-gated API keys, outbound webhooks, and Stripe/Paystack billing webhooks. Deployed live on Render with automated testing via GitHub Actions.

**Live API:** https://backend-track-nz8c.onrender.com
(Free-tier instance — the first request after inactivity may take up to a minute.)

## Stack

- **Framework:** FastAPI (Python)
- **Database:** PostgreSQL (hosted on Neon)
- **Auth:** JWT via python-jose, password hashing via passlib/bcrypt
- **Deployment:** Docker, Render
- **CI/CD:** GitHub Actions — builds the Docker image, spins up a Postgres service container, and runs a live smoke test (register → login → create → list) on every push
- **Rate limiting:** per-user request throttling via Redis (Upstash), configurable via `RATE_LIMIT_PER_MINUTE`; per-organization plan limits for API-key traffic
- **Multi-tenancy:** organizations own API keys and webhooks; every tenant is quota-gated by plan (`free` / `pro` / `business`)
- **Billing:** signed webhook receivers for Stripe and Paystack that switch an organization's plan automatically
- **Testing:** pytest suite (unit tests run anywhere; integration tests run against Postgres in CI)

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

### Organizations, API keys & webhooks (JWT auth)

| Method | Path | Description |
|---|---|---|
| POST | `/orgs` | Create an organization (JSON body: `{"name": "..."}`) |
| GET | `/orgs` | List your organizations |
| GET | `/orgs/{org_id}` | Org details, plan, and limits |
| POST | `/orgs/{org_id}/keys` | Create an API key — the full key is shown **once** |
| GET | `/orgs/{org_id}/keys` | List keys (masked) |
| DELETE | `/orgs/{org_id}/keys/{key_id}` | Revoke a key |
| POST | `/orgs/{org_id}/webhooks` | Register an outbound webhook URL — secret shown **once** |
| GET | `/orgs/{org_id}/webhooks` | List webhooks |
| DELETE | `/orgs/{org_id}/webhooks/{webhook_id}` | Remove a webhook |

### Programmatic access (API-key auth via `X-API-Key` header)

| Method | Path | Description |
|---|---|---|
| POST | `/v1/messages` | Create a message as the org (plan rate limit applies, webhooks fire) |
| GET | `/v1/messages` | List the org's messages |

### Billing

| Method | Path | Description |
|---|---|---|
| POST | `/billing/stripe/webhook` | Stripe events: `checkout.session.completed`, `customer.subscription.*` switch plans |
| POST | `/billing/paystack/webhook` | Paystack events: `subscription.create` / `subscription.disable` / `charge.success` switch plans |

Both receivers verify HMAC signatures and reject unsigned requests. Pass `org_id` and `plan`
in checkout/subscription metadata (or map Stripe price ids via `STRIPE_PRICE_PLANS`).

### Plans

| Plan | Requests/min | Webhooks |
|---|---|---|
| free | 30 | 1 |
| pro | 600 | 10 |
| business | 3000 | 50 |

### Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `SECRET_KEY` | Yes | JWT signing key |
| `REDIS_URL` | No | Enables rate limiting |
| `RATE_LIMIT_PER_MINUTE` | No | Per-user limit (default 30) |
| `STRIPE_WEBHOOK_SECRET` | No | Enables & verifies Stripe billing webhooks |
| `PAYSTACK_WEBHOOK_SECRET` | No | Enables & verifies Paystack billing webhooks |
| `STRIPE_PRICE_PLANS` | No | JSON map of Stripe price id → plan |

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

## Run the tests

```bash
cd backend-track
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -v
```

Unit tests run without a database. Integration tests run automatically when
`DATABASE_URL` points at a PostgreSQL instance (as in CI).

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
