"""Tests for the Customer Message Triage API.

Run with:  pytest -v        (from the backend-track directory)

Every test gets its own throwaway SQLite file, so the real messages.db is never
touched and tests can run in any order.
"""

import sqlite3

import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture
def client(tmp_path, monkeypatch):
    """A TestClient wired to a fresh, empty database."""
    monkeypatch.setattr(main, "DB_PATH", str(tmp_path / "test_messages.db"))
    main.init_db()
    with TestClient(main.app) as test_client:
        yield test_client


def create(client, sender="customer_A", text="do you deliver to Kano?"):
    return client.post("/messages", json={"sender": sender, "text": text})


# --- schema initialisation -------------------------------------------------


def test_init_db_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "DB_PATH", str(tmp_path / "init.db"))
    main.init_db()
    main.init_db()  # must not raise on the second call


def test_startup_creates_messages_table(tmp_path, monkeypatch):
    """App startup alone (no manual SQL) must create the table."""
    db_file = tmp_path / "startup.db"
    monkeypatch.setattr(main, "DB_PATH", str(db_file))

    with TestClient(main.app):
        pass  # entering the context runs the lifespan startup hook

    conn = sqlite3.connect(db_file)
    try:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        ]
    finally:
        conn.close()

    assert "messages" in tables


# --- classification --------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("what's the PRICE for 50 bags of rice?", "pricing"),
        ("send me your price list", "pricing"),
        ("where is my order #221?", "order_status"),
        ("ORDER status please", "order_status"),
        ("do you deliver to Kano?", "general"),
        ("", "general"),
    ],
)
def test_classify(text, expected):
    assert main.classify(text) == expected


def test_price_wins_over_order():
    """Documented precedence: 'price' is checked before 'order'."""
    assert main.classify("price of my order") == "pricing"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("what is the price for 50 bags?", "pricing"),
        ("where is my order #221?", "order_status"),
        ("do you deliver to Kano?", "general"),
    ],
)
def test_create_assigns_category(client, text, expected):
    response = create(client, text=text)
    assert response.status_code == 201
    assert response.json()["category"] == expected


# --- health check ----------------------------------------------------------


def test_root_reports_alive(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


# --- validation ------------------------------------------------------------


def test_create_starts_as_pending(client):
    body = create(client).json()
    assert body["status"] == "pending"
    assert isinstance(body["id"], int)


@pytest.mark.parametrize("field", ["sender", "text"])
def test_blank_and_whitespace_only_are_rejected(client, field):
    for blank in ["", " ", "\t", "\n", "   \t\n  "]:
        payload = {"sender": "customer_A", "text": "price please"}
        payload[field] = blank
        response = client.post("/messages", json=payload)
        assert response.status_code == 422, f"{field}={blank!r} should be rejected"


@pytest.mark.parametrize("field", ["sender", "text"])
def test_missing_fields_are_rejected(client, field):
    payload = {"sender": "customer_A", "text": "price please"}
    del payload[field]
    assert client.post("/messages", json=payload).status_code == 422


def test_surrounding_whitespace_is_trimmed(client):
    response = create(client, sender="  customer_A  ", text="  price check  ")
    body = response.json()
    assert body["sender"] == "customer_A"
    assert body["text"] == "price check"


def test_length_limits_are_enforced(client):
    assert create(client, sender="x" * 100).status_code == 201
    assert create(client, sender="x" * 101).status_code == 422
    assert create(client, text="price " + "x" * 1994).status_code == 201
    assert create(client, text="x" * 2001).status_code == 422


def test_non_string_types_are_rejected(client):
    response = client.post("/messages", json={"sender": 42, "text": None})
    assert response.status_code == 422


# --- reading ---------------------------------------------------------------


def test_list_is_empty_on_a_fresh_database(client):
    assert client.get("/messages").json() == []


def test_list_returns_messages_in_insertion_order(client):
    first = create(client, text="price of rice?").json()["id"]
    second = create(client, text="where is my order?").json()["id"]

    body = client.get("/messages").json()
    assert [message["id"] for message in body] == [first, second]


def test_get_single_message(client):
    created = create(client).json()
    response = client.get(f"/messages/{created['id']}")
    assert response.status_code == 200
    assert response.json() == created


def test_get_missing_message_returns_404(client):
    response = client.get("/messages/9999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Message not found"


# --- approval --------------------------------------------------------------


def test_approve_flips_status_to_approved(client):
    message_id = create(client).json()["id"]

    response = client.put(f"/messages/{message_id}/approve")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "approved"
    assert body["id"] == message_id
    # the change must be persisted, not just returned
    assert client.get(f"/messages/{message_id}").json()["status"] == "approved"


def test_approve_is_idempotent(client):
    message_id = create(client).json()["id"]
    client.put(f"/messages/{message_id}/approve")
    second = client.put(f"/messages/{message_id}/approve")
    assert second.status_code == 200
    assert second.json()["status"] == "approved"


def test_approve_missing_message_returns_404(client):
    assert client.put("/messages/9999/approve").status_code == 404


# --- deletion --------------------------------------------------------------


def test_delete_removes_the_message(client):
    message_id = create(client).json()["id"]

    response = client.delete(f"/messages/{message_id}")

    assert response.status_code == 200
    assert response.json() == {"deleted": message_id}
    assert client.get(f"/messages/{message_id}").status_code == 404
    assert client.get("/messages").json() == []


def test_delete_missing_message_returns_404(client):
    assert client.delete("/messages/9999").status_code == 404


def test_delete_only_removes_the_target_message(client):
    keep = create(client, text="price of rice?").json()["id"]
    drop = create(client, text="where is my order?").json()["id"]

    client.delete(f"/messages/{drop}")

    remaining = client.get("/messages").json()
    assert [message["id"] for message in remaining] == [keep]
