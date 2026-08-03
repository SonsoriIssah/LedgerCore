# These tests check the app while it is running.
# Start the app on port 8001 before running these tests.

import pytest
import httpx
import uuid
import asyncio

BASE_URL = "http://127.0.0.1:8001"

# A normal transfer should succeed and come back marked as completed.
@pytest.mark.asyncio
async def test_transfer_completes():
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{BASE_URL}/transfers", json={
            "from_account": 1,
            "to_account": 2,
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
        first = await client.post(f"{BASE_URL}/transfers", json={
            "from_account": 1, "to_account": 2, "amount": 5, "idempotency_key": key
        })
        second = await client.post(f"{BASE_URL}/transfers", json={
            "from_account": 1, "to_account": 2, "amount": 5, "idempotency_key": key
        })
        assert first.json()["id"] == second.json()["id"]

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
    async def fire(client):
        return await client.post(f"{BASE_URL}/transfers", json={
            "from_account": 1, "to_account": 2, "amount": 10,
            "idempotency_key": str(uuid.uuid4())
        })

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[fire(client) for _ in range(20)])
        status_codes = [r.status_code for r in results]
        assert all(code in (200, 409) for code in status_codes)