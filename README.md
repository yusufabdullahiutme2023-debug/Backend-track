# Backend Track: Messages API

A FastAPI service that accepts messages, auto-categorizes them, and supports an approval workflow. Containerized with Docker, built and smoke-tested by GitHub Actions on every push, and deployed on Render.

**Live API:** https://backend-track-nz8c.onrender.com
**Interactive docs (Swagger):** https://backend-track-nz8c.onrender.com/docs

> Hosted on a free tier: the first request after idle can take about 50 seconds, and stored messages reset when the service restarts.

## Endpoints

| Method | Path | What it does |
|---|---|---|
| GET | `/` | Health check |
| POST | `/messages` | Create a message (auto-categorized, status `pending`) |
| GET | `/messages` | List all messages |
| GET | `/messages/{id}` | Get one message |
| PUT | `/messages/{id}/approve` | Mark a message as approved |
| DELETE | `/messages/{id}` | Delete a message |

Validation: `sender` (1-100 chars) and `text` (1-2000 chars) are required and cannot be blank.

## Stack

FastAPI, Pydantic, SQLite, Docker, GitHub Actions (CI), Render (hosting).

## Run locally

    docker build -t backend-track ./backend-track
    docker run -p 8000:8000 backend-track

Then open http://localhost:8000/docs

## Roadmap

- Authentication (JWT) and user registration
- PostgreSQL instead of SQLite
- Automated tests (pytest) in CI
