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

### Experimental Solana launch intelligence (read-only)

`POST /v1/solana/signals` accepts an `EvidenceBundle` (see
`backend-track/solana_signals.py`) with a launch, verified venue-decoded buys,
verified explicit SOL transfer edges, verified CEX labels and **realized**
pre-buy performance evidence. Authenticate using an organization's `X-API-Key`.
`GET /v1/solana/signals` returns the organization's latest 100 results.
Signals are upserted per `(organization, launch signature)`. Partial coverage is
reported as `partial`, never silently interpreted as a negative result.

**Not a live trading bot:** pool detection, venue-specific swap decoding,
backfill, external PnL reconciliation, label verification, and alert delivery
require a trusted external indexer/provider and are not implemented here. Do not
send unsigned/unverified external data or act on these experimental scores as
trade recommendations. No credentials or trading keys are required by this module.

#### Pump.fun transaction replay (experimental)

`backend-track/pump_replay.py` decodes official Pump program `create`,
`create_v2`, and `buy` instruction discriminators from **raw** Solana
`getTransaction` JSON. It verifies the program ID, required signers, successful
execution, mint/curve accounts, slot window, and positive raw token-balance
change for the buy instruction's user. No private wallet key is required.

With a private provider URL in your shell environment (never commit it):

```bash
cd backend-track
export SOLANA_RPC_URL='your-private-https-rpc-url'
python pump_replay.py LAUNCH_SIGNATURE BUY_SIGNATURE [MORE_BUY_SIGNATURES...]
```

This replays **specified signatures**; it does not discover launches, enumerate
all first buyers, or prove profitability. Raw JSON with loaded-address metadata
is required; if your RPC omits the necessary data, the replay must be treated as
incomplete. A WebSocket watcher and persisted cursor/backfill are still required
for continuous monitoring. The tests use synthetic transactions, not verified
historical Mainnet fixtures.

To replay **real Mainnet** transactions in GitHub Actions without exposing your
provider URL, add a repository Actions secret named `HELIUS_RPC_URL` containing
your private **HTTP** Mainnet RPC URL. After the `Solana transaction replay`
workflow is available on the repository's default branch, choose **Run
workflow**, enter a public Pump.fun creation transaction signature and a public
buy signature for that same token (within ten slots), and select this branch.
The workflow fails closed if the secret is absent or no qualifying buy is found.
It does not submit orders or log your RPC URL. GitHub may only show a newly
added manual workflow in the Actions UI after it is present on the default
branch; do not merge solely to trigger this test without reviewing the changes.

#### Dedicated Pump.fun watcher (experimental, NOT deployed)

`backend-track/pump_worker.py` subscribes to confirmed Pump.fun program logs,
validates creation transactions against the official program ID and instruction
discriminator, and stores launch evidence plus a crash-safe cursor in PostgreSQL.
It subscribes before bounded reconnect backfill and refuses to silently cross a
gap larger than 10,000 transactions. It is read-only and does **not** claim a
verified first-buyer set, funding trace, PnL, or production alert signal.

Run only as a *separate persistent worker* with private `SOLANA_RPC_URL` (Helius
Mainnet HTTPS URL) and `DATABASE_URL` in the worker's secret environment. Do not
run it inside the web server or GitHub Actions: those are not persistent worker
hosts. Monitor Helius credits and run reconciliation tests before deployment.
Do not store the URL or credentials in Git or chat. If the worker encounters a
history gap, investigate it; do not reset the cursor to hide missing events.

#### Persistent worker hosting (Render Blueprint)

`render.yaml` describes a **separate**, one-instance background worker in
Frankfurt using the 0.5c-512mb plan. It is not the existing FastAPI web
service. Automatic deploys are disabled; deploy deliberately after changes.
Render currently lists that worker compute tier at **$7/month**, plus any
third-party API/database usage; check the Render confirmation screen for the
actual charge before creating it. It is not provisioned by committing this file.

In Render: **New → Blueprint → connect this GitHub repository → branch
`arena/01a0ed1a-backend-track` → `render.yaml`**. Before approving the initial
creation, set the prompted secrets directly in Render:

- `SOLANA_RPC_URL`: Helius **Mainnet HTTPS RPC URL** (not WSS). Do not paste in
  chat, Git, build logs, or a screenshot.
- `DATABASE_URL`: your PostgreSQL/Neon connection string. This worker's
  checkpoint tables are stored in that database, so it must persist across
  deploys. Use the correct SSL configuration for your provider.

The GitHub Actions secret `HELIUS_RPC_URL` is **not shared with Render**.
The current worker only records program-confirmed launch transactions as
`unscored`; it does not yet collect exhaustive buyers, trace funding, calculate
50x PnL, or send alerts. Watch Helius credits and logs, especially retries and
history-gap errors. Do not configure multiple instances: this worker does not
implement multi-instance leader election. Stop the worker in Render if it is
reconnecting repeatedly or using credits faster than expected.

#### No-card, bounded GitHub Actions research

`.github/workflows/solana-research.yml` uses the existing GitHub Actions
`HELIUS_RPC_URL` secret to listen for **60 seconds twice daily** (06:17 and
18:17 UTC), with at most eight on-chain transaction lookups per run. It retains
a public-evidence JSON artifact for seven days. It never uses a wallet, places
orders, writes to Neon, or claims first-buyer coverage. A quiet sample (zero
launches) is a valid result, **not** evidence that no launches occurred.
Actions schedules can be delayed or skipped and run only from the repository's
**default branch**. The workflow on the Arena branch can be push-tested now;
the schedule will not activate until the code is reviewed and merged into the
default branch. Do not mistake these short snapshots for persistent hosting.
