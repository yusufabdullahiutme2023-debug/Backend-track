"""Tests for the message-management API.

Unit tests run anywhere (no database needed).
Integration tests run automatically when DATABASE_URL points at a real
PostgreSQL instance (as in the GitHub Actions CI pipeline).
"""
import hashlib
import hmac
import json
import os
import time
import uuid

import pytest

# Importing main requires DATABASE_URL; use a dummy for pure unit tests.
HAS_REAL_DB = "DATABASE_URL" in os.environ and os.environ["DATABASE_URL"] != ""
os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/dummy")
os.environ.setdefault("SECRET_KEY", "test-secret-key")

import main  # noqa: E402
from jose import jwt  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


# ---------------------------------------------------------------------------
# Unit tests — no database required
# ---------------------------------------------------------------------------
class TestClassify:
    def test_pricing(self):
        assert main.classify("What is the PRICE of this?") == "pricing"

    def test_order_status(self):
        assert main.classify("where is my order") == "order_status"

    def test_general(self):
        assert main.classify("hello there") == "general"


class TestPlans:
    def test_plans_have_required_fields(self):
        for name, limits in main.PLANS.items():
            assert limits["rpm"] > 0
            assert limits["max_webhooks"] >= 1

    def test_plans_are_monotonic(self):
        ordered = [main.PLANS[p] for p in ("free", "pro", "business")]
        assert ordered[0]["rpm"] < ordered[1]["rpm"] < ordered[2]["rpm"]
        assert ordered[0]["max_webhooks"] < ordered[1]["max_webhooks"] < ordered[2]["max_webhooks"]


class TestApiKeys:
    def test_generated_key_format(self):
        key = main.generate_api_key()
        assert key.startswith("sk_live_")
        assert len(key) == len("sk_live_") + 48

    def test_keys_are_unique(self):
        assert main.generate_api_key() != main.generate_api_key()

    def test_hash_is_deterministic_sha256(self):
        key = "sk_live_abc123"
        assert main.hash_api_key(key) == hashlib.sha256(key.encode()).hexdigest()
        assert main.hash_api_key(key) == main.hash_api_key(key)


class TestWebhookSigning:
    def test_signature_roundtrip(self):
        secret = main.generate_webhook_secret()
        assert secret.startswith("whsec_")
        body = b'{"event": "message.created"}'
        sig = main.sign_webhook_payload(secret, body)
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        assert sig == expected

    def test_different_secrets_differ(self):
        body = b"payload"
        assert main.sign_webhook_payload("a", body) != main.sign_webhook_payload("b", body)


class TestStripeSignature:
    SECRET = "whsec_test_secret"

    def _sign(self, payload: bytes, ts: int) -> str:
        signed = f"{ts}.".encode() + payload
        v1 = hmac.new(self.SECRET.encode(), signed, hashlib.sha256).hexdigest()
        return f"t={ts},v1={v1}"

    def test_valid_signature(self):
        payload = b'{"type": "checkout.session.completed"}'
        header = self._sign(payload, int(time.time()))
        assert main.verify_stripe_signature(payload, header, self.SECRET) is True

    def test_tampered_payload_rejected(self):
        header = self._sign(b"original", int(time.time()))
        assert main.verify_stripe_signature(b"tampered", header, self.SECRET) is False

    def test_stale_timestamp_rejected(self):
        old_ts = int(time.time()) - main.STRIPE_TIMESTAMP_TOLERANCE - 10
        payload = b"{}"
        header = self._sign(payload, old_ts)
        assert main.verify_stripe_signature(payload, header, self.SECRET) is False

    def test_empty_secret_rejected(self):
        assert main.verify_stripe_signature(b"{}", "t=1,v1=x", "") is False

    def test_malformed_header_rejected(self):
        assert main.verify_stripe_signature(b"{}", "garbage", self.SECRET) is False


class TestPaystackSignature:
    SECRET = "sk_paystack_test"

    def test_valid_signature(self):
        payload = b'{"event": "charge.success"}'
        sig = hmac.new(self.SECRET.encode(), payload, hashlib.sha512).hexdigest()
        assert main.verify_paystack_signature(payload, sig, self.SECRET) is True

    def test_invalid_signature(self):
        assert main.verify_paystack_signature(b"{}", "0" * 128, self.SECRET) is False

    def test_empty_inputs_rejected(self):
        assert main.verify_paystack_signature(b"{}", "", self.SECRET) is False
        assert main.verify_paystack_signature(b"{}", "sig", "") is False


class TestAuthTokens:
    def test_jwt_roundtrip(self):
        token = main.create_access_token({"sub": "alice"})
        payload = jwt.decode(token, main.SECRET_KEY, algorithms=[main.ALGORITHM])
        assert payload["sub"] == "alice"
        assert "exp" in payload


class FakeRedis:
    """Minimal in-memory stand-in for the redis client."""

    def __init__(self):
        self.store = {}

    def incr(self, key):
        self.store[key] = self.store.get(key, 0) + 1
        return self.store[key]

    def expire(self, key, seconds):
        return True


class TestRateLimit:
    def test_allows_up_to_limit_then_blocks(self, monkeypatch):
        monkeypatch.setattr(main, "redis_client", FakeRedis())
        main.enforce_rate_limit("test-scope", 2)  # 1st
        main.enforce_rate_limit("test-scope", 2)  # 2nd
        with pytest.raises(Exception) as exc:
            main.enforce_rate_limit("test-scope", 2)  # 3rd -> blocked
        assert exc.value.status_code == 429

    def test_no_redis_means_no_limit(self, monkeypatch):
        monkeypatch.setattr(main, "redis_client", None)
        for _ in range(100):
            main.enforce_rate_limit("scope", 1)  # would raise if enforced


# ---------------------------------------------------------------------------
# Integration tests — require a real PostgreSQL (CI provides one)
# ---------------------------------------------------------------------------
requires_db = pytest.mark.skipif(
    not HAS_REAL_DB, reason="DATABASE_URL not set — skipping integration tests"
)


@requires_db
class TestFullFlow:
    @pytest.fixture(scope="class")
    def client(self):
        with TestClient(main.app) as c:  # context manager triggers startup -> init_db
            yield c

    @pytest.fixture(scope="class")
    def auth(self, client):
        username = f"tester_{uuid.uuid4().hex[:12]}"
        password = "sup3r-secret"
        r = client.post(f"/register?username={username}&password={password}")
        assert r.status_code == 200
        r = client.post("/token", data={"username": username, "password": password})
        assert r.status_code == 200
        token = r.json()["access_token"]
        return {"username": username, "headers": {"Authorization": f"Bearer {token}"}}

    @pytest.fixture(scope="class")
    def org(self, client, auth):
        r = client.post("/orgs", json={"name": "Test Org"}, headers=auth["headers"])
        assert r.status_code == 200
        return r.json()

    @pytest.fixture(scope="class")
    def api_key(self, client, auth, org):
        r = client.post(
            f"/orgs/{org['id']}/keys", json={"name": "ci"}, headers=auth["headers"]
        )
        assert r.status_code == 200
        body = r.json()
        assert body["key"].startswith("sk_live_")
        return body["key"]

    def test_health(self, client):
        assert client.get("/").json() == {"status": "alive"}

    def test_org_visible_to_owner(self, client, auth, org):
        r = client.get("/orgs", headers=auth["headers"])
        assert any(o["id"] == org["id"] for o in r.json())
        r = client.get(f"/orgs/{org['id']}", headers=auth["headers"])
        assert r.json()["plan"] == "free"
        assert r.json()["limits"] == main.PLANS["free"]

    def test_api_key_listing_hides_key_material(self, client, auth, org, api_key):
        r = client.get(f"/orgs/{org['id']}/keys", headers=auth["headers"])
        assert r.status_code == 200
        for k in r.json():
            assert "key" not in k or k.get("key") is None
            assert len(k["key_last4"]) == 4

    def test_v1_messages_requires_key(self, client):
        r = client.post("/v1/messages", json={"sender": "x", "text": "hello"})
        assert r.status_code == 401

    def test_v1_messages_rejects_bad_key(self, client):
        r = client.post(
            "/v1/messages",
            json={"sender": "x", "text": "hello"},
            headers={"X-API-Key": "sk_live_invalid"},
        )
        assert r.status_code == 401

    def test_v1_message_created_and_listed(self, client, api_key, org):
        r = client.post(
            "/v1/messages",
            json={"sender": "api", "text": "what is the price"},
            headers={"X-API-Key": api_key},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["category"] == "pricing"
        assert body["org_id"] == org["id"]
        r = client.get("/v1/messages", headers={"X-API-Key": api_key})
        assert any(m["id"] == body["id"] for m in r.json())

    def test_webhook_crud(self, client, auth, org):
        r = client.post(
            f"/orgs/{org['id']}/webhooks",
            json={"url": "https://example.com/hook"},
            headers=auth["headers"],
        )
        assert r.status_code == 200
        hook = r.json()
        assert hook["secret"].startswith("whsec_")

        r = client.get(f"/orgs/{org['id']}/webhooks", headers=auth["headers"])
        assert all("secret" not in w for w in r.json())

        r = client.delete(
            f"/orgs/{org['id']}/webhooks/{hook['id']}", headers=auth["headers"]
        )
        assert r.status_code == 200

    def test_webhook_quota_on_free_plan(self, client, auth, org):
        created = []
        for _ in range(main.PLANS["free"]["max_webhooks"] + 1):
            r = client.post(
                f"/orgs/{org['id']}/webhooks",
                json={"url": "https://example.com/hook2"},
                headers=auth["headers"],
            )
            if r.status_code == 200:
                created.append(r.json()["id"])
            else:
                assert r.status_code == 402
                break
        # cleanup
        for hook_id in created:
            client.delete(f"/orgs/{org['id']}/webhooks/{hook_id}", headers=auth["headers"])

    def test_paystack_webhook_upgrades_plan(self, client, auth, org, monkeypatch):
        secret = "ps_test_secret"
        monkeypatch.setattr(main, "PAYSTACK_WEBHOOK_SECRET", secret)
        payload = json.dumps(
            {
                "event": "subscription.create",
                "data": {
                    "status": "active",
                    "metadata": {"org_id": org["id"], "plan": "pro"},
                },
            }
        ).encode()
        sig = hmac.new(secret.encode(), payload, hashlib.sha512).hexdigest()
        r = client.post(
            "/billing/paystack/webhook",
            content=payload,
            headers={"x-paystack-signature": sig, "Content-Type": "application/json"},
        )
        assert r.status_code == 200
        r = client.get(f"/orgs/{org['id']}", headers=auth["headers"])
        assert r.json()["plan"] == "pro"

        # downgrade again via subscription.disable
        payload = json.dumps(
            {
                "event": "subscription.disable",
                "data": {"metadata": {"org_id": org["id"]}},
            }
        ).encode()
        sig = hmac.new(secret.encode(), payload, hashlib.sha512).hexdigest()
        r = client.post(
            "/billing/paystack/webhook",
            content=payload,
            headers={"x-paystack-signature": sig, "Content-Type": "application/json"},
        )
        assert r.status_code == 200
        r = client.get(f"/orgs/{org['id']}", headers=auth["headers"])
        assert r.json()["plan"] == "free"

    def test_paystack_webhook_rejects_bad_signature(self, client, monkeypatch):
        monkeypatch.setattr(main, "PAYSTACK_WEBHOOK_SECRET", "ps_test_secret")
        r = client.post(
            "/billing/paystack/webhook",
            content=b'{"event": "charge.success"}',
            headers={"x-paystack-signature": "0" * 128},
        )
        assert r.status_code == 400

@requires_db
def test_solana_signal_http_roundtrip():
    """Real Postgres + HTTP authentication + idempotent evidence scoring."""
    with TestClient(main.app) as client:
        username = 'signals_' + uuid.uuid4().hex[:12]
        assert client.post('/register', params={'username': username, 'password': 'testpass'}).status_code == 200
        token = client.post('/token', data={'username': username, 'password': 'testpass'}).json()['access_token']
        headers = {'Authorization': f'Bearer {token}'}
        org = client.post('/orgs', json={'name': 'Signal Tester'}, headers=headers).json()
        key = client.post(f"/orgs/{org['id']}/keys", json={'name': 'signal'}, headers=headers).json()['key']
        launch_signature = uuid.uuid4().hex * 2
        payload = {
            'launch': {'mint': 'M'*32, 'pool': 'P'*32, 'signature': launch_signature,
                       'venue': 'pump', 'slot': 100, 'end_slot': 110},
            'buys': [{'wallet': 'W'*32, 'signature': 'buy', 'slot': 105, 'order': 0,
                      'raw_amount': 10, 'verified': True}],
            'edges': [{'source': 'C'*32, 'destination': 'W'*32, 'signature': 'fund',
                       'slot': 90, 'lamports': 100000000, 'verified': True}],
            'labels': [{'address': 'C'*32, 'exchange': 'demo', 'source': 'test', 'verified': True}],
            'performance': [{'wallet': 'W'*32, 'token': 'old', 'multiple': 55,
                             'closed_slot': 80, 'evidence_signature': 'sale', 'verified': True}],
            'buyers_complete': True, 'funding_complete': ['W'*32], 'pnl_complete': ['W'*32],
        }
        assert client.post('/v1/solana/signals', json=payload).status_code == 401
        r = client.post('/v1/solana/signals', json=payload, headers={'X-API-Key': key})
        assert r.status_code == 200, r.text
        assert r.json()['cex_funded_50x_count'] == 1
        assert client.post('/v1/solana/signals', json=payload, headers={'X-API-Key': key}).status_code == 200
        rows = client.get('/v1/solana/signals', headers={'X-API-Key': key}).json()
        assert sum(row['launch_signature'] == launch_signature for row in rows) == 1
