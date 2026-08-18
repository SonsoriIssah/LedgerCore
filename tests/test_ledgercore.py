# These tests check the app while it is running.
# Start the app on port 8001 before running these tests.

import pytest
import httpx
import uuid
import asyncio
import socket

BASE_URL = "http://127.0.0.1:8001"


def _server_is_running() -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.5)
        return probe.connect_ex(("127.0.0.1", 8001)) == 0


# Skip rather than fail when the app isn't up, so a plain `pytest` on a fresh
# clone reports these as skipped instead of a wall of connection errors.
pytestmark = pytest.mark.skipif(
    not _server_is_running(),
    reason="needs the app running on port 8001 -- see DEVELOPMENT.md",
)


# Every transfer now needs a signed-in owner, so each test makes its own users
# rather than sharing fixed account numbers. Fresh users also keep tests
# independent: one test's transfers can't turn up in another test's balance.
async def new_user(client):
    """Register a user and return (account_id, auth_headers)."""
    email = f"test-{uuid.uuid4().hex[:12]}@example.com"
    password = "test-password-123"

    register = await client.post(f"{BASE_URL}/register", json={"email": email, "password": password})
    assert register.status_code == 200, register.text
    account_id = register.json()["account_id"]

    login = await client.post(f"{BASE_URL}/login", json={"email": email, "password": password})
    assert login.status_code == 200, login.text
    token = login.json()["access_token"]

    return account_id, {"Authorization": f"Bearer {token}"}


# Registering should hand back an account number, and it should match what
# /accounts/me reports afterwards.
@pytest.mark.asyncio
async def test_register_creates_an_account():
    async with httpx.AsyncClient() as client:
        account_id, headers = await new_user(client)
        assert isinstance(account_id, int)

        me = await client.get(f"{BASE_URL}/accounts/me", headers=headers)
        assert me.status_code == 200
        assert me.json()["account_id"] == account_id
        assert me.json()["balance"] == 0


# A normal transfer should succeed and come back marked as completed.
@pytest.mark.asyncio
async def test_transfer_completes():
    async with httpx.AsyncClient() as client:
        sender_account, sender_headers = await new_user(client)
        recipient_account, _ = await new_user(client)

        response = await client.post(f"{BASE_URL}/transfers", headers=sender_headers, json={
            "from_account": sender_account,
            "to_account": recipient_account,
            "amount": 10,
            "idempotency_key": str(uuid.uuid4())
        })
        assert response.status_code == 200
        assert response.json()["status"] == "completed"


# Sending the same idempotency_key twice should give back the same transaction,
# not create a second one. This protects against accidental retries.
@pytest.mark.asyncio
async def test_idempotency_returns_same_transaction():
    key = str(uuid.uuid4())
    async with httpx.AsyncClient() as client:
        sender_account, sender_headers = await new_user(client)
        recipient_account, _ = await new_user(client)

        payload = {
            "from_account": sender_account, "to_account": recipient_account,
            "amount": 5, "idempotency_key": key
        }
        first = await client.post(f"{BASE_URL}/transfers", headers=sender_headers, json=payload)
        second = await client.post(f"{BASE_URL}/transfers", headers=sender_headers, json=payload)
        assert first.json()["id"] == second.json()["id"]


# Money leaving one account should arrive in the other.
@pytest.mark.asyncio
async def test_transfer_moves_money_between_the_two_accounts():
    async with httpx.AsyncClient() as client:
        sender_account, sender_headers = await new_user(client)
        recipient_account, recipient_headers = await new_user(client)

        await client.post(f"{BASE_URL}/transfers", headers=sender_headers, json={
            "from_account": sender_account, "to_account": recipient_account,
            "amount": 30, "idempotency_key": str(uuid.uuid4())
        })

        sender_balance = await client.get(f"{BASE_URL}/accounts/{sender_account}/balance", headers=sender_headers)
        recipient_balance = await client.get(f"{BASE_URL}/accounts/{recipient_account}/balance", headers=recipient_headers)
        assert sender_balance.json()["balance"] == -30
        assert recipient_balance.json()["balance"] == 30


# ---- Ownership rules ----

# Without a token, the money routes should not answer at all.
@pytest.mark.asyncio
async def test_transfer_requires_authentication():
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{BASE_URL}/transfers", json={
            "from_account": 1, "to_account": 2, "amount": 10,
            "idempotency_key": str(uuid.uuid4())
        })
        assert response.status_code == 401


# Spending out of an account you do not own is the main thing ownership has to
# prevent, so it gets its own test.
@pytest.mark.asyncio
async def test_cannot_transfer_from_someone_elses_account():
    async with httpx.AsyncClient() as client:
        victim_account, _ = await new_user(client)
        attacker_account, attacker_headers = await new_user(client)

        response = await client.post(f"{BASE_URL}/transfers", headers=attacker_headers, json={
            "from_account": victim_account,
            "to_account": attacker_account,
            "amount": 10,
            "idempotency_key": str(uuid.uuid4())
        })
        assert response.status_code == 403


# Reading another person's balance or history should be impossible too.
@pytest.mark.asyncio
async def test_cannot_read_someone_elses_account():
    async with httpx.AsyncClient() as client:
        victim_account, _ = await new_user(client)
        _, other_headers = await new_user(client)

        for path in ("balance", "postings", "summary", "statement"):
            response = await client.get(f"{BASE_URL}/accounts/{victim_account}/{path}", headers=other_headers)
            assert response.status_code == 404, f"/{path} leaked another account"


# Sending to an account number nobody owns should be refused, rather than
# silently writing postings against an account that does not exist.
@pytest.mark.asyncio
async def test_transfer_to_unknown_account_is_rejected():
    async with httpx.AsyncClient() as client:
        sender_account, sender_headers = await new_user(client)

        response = await client.post(f"{BASE_URL}/transfers", headers=sender_headers, json={
            "from_account": sender_account, "to_account": 999999,
            "amount": 10, "idempotency_key": str(uuid.uuid4())
        })
        assert response.status_code == 404


# ---- Ledger-wide properties ----

# The ledger should always stay balanced: total debits should equal total credits.
@pytest.mark.asyncio
async def test_invariant_holds():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/system/invariant-check")
        assert response.json()["balanced"] == True


# Send many transfers at the same time and make sure none of them crash the app.
# Each one should either succeed (200) or be safely rejected (409).
@pytest.mark.asyncio
async def test_concurrent_transfers_no_crashes():
    async with httpx.AsyncClient() as client:
        sender_account, sender_headers = await new_user(client)
        recipient_account, _ = await new_user(client)

        async def fire():
            return await client.post(f"{BASE_URL}/transfers", headers=sender_headers, json={
                "from_account": sender_account, "to_account": recipient_account,
                "amount": 10, "idempotency_key": str(uuid.uuid4())
            })

        results = await asyncio.gather(*[fire() for _ in range(20)])
        status_codes = [r.status_code for r in results]
        assert all(code in (200, 409) for code in status_codes)
