import asyncio
import httpx
import uuid

async def fire_transfer(client):
    key = str(uuid.uuid4())
    try:
        response = await client.post("http://localhost:8001/transfers", json={
            "from_account": 1,
            "to_account": 2,
            "amount": 10,
            "idempotency_key": key
        })
        return response.status_code
    except Exception as e:
        return f"error: {e}"

async def run_chaos_test(n=50):
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[fire_transfer(client) for _ in range(n)])
        print(f"Results: {results}")

    async with httpx.AsyncClient() as client:
        result = await client.get("http://localhost:8001/system/invariant-check")
        print(result.json())

asyncio.run(run_chaos_test())