"""
Minimal FastAPI service to enqueue transactions in Redis and trigger the GitHub fraud batch pipeline (ETL).

Environment variables required:
  REDIS_URL    -> e.g. redis://:<PASSWORD>@<HOST>:<PORT>
  GITHUB_TOKEN -> personal access token with repo:dispatch rights

Optional:
  QUEUE_NAME   -> default "transactions_queue"
  API_TOKEN    -> if set, header 'X-API-TOKEN' must match
"""

import os
import json
import asyncio
from typing import Optional, List
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field, condecimal
import redis
import httpx


# -------- Configuration --------
REDIS_URL  = os.getenv("REDIS_URL")
QUEUE_NAME = os.getenv("QUEUE_NAME", "transactions_queue")
API_TOKEN  = os.getenv("API_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # required for instant GitHub trigger
REPO = "nholderTDM/humana-fraud-cloud"    # make sure this matches your GitHub org/repo

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not set. Configure it in Railway env vars.")

r = redis.from_url(REDIS_URL, decode_responses=True)  # works for redis:// or rediss://


# --- Trigger GitHub workflow instantly ---
async def trigger_pipeline():
    """Send a repository_dispatch to GitHub to start the fraud-run workflow."""
    if not GITHUB_TOKEN:
        # If no token is set we just skip silently; manual/scheduled runs still work.
        return
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://api.github.com/repos/{REPO}/dispatches",
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"token {GITHUB_TOKEN}",
            },
            json={"event_type": "fraud-run"},
        )
        resp.raise_for_status()


# -------- Data model ----------
class Transaction(BaseModel):
    transaction_id: str = Field(..., min_length=3)
    amount: condecimal(gt=0)   # positive number
    location: Optional[str] = "USA"
    device:   Optional[str] = "Web"


# -------- FastAPI app ---------
app = FastAPI(title="Fraud Ingestion API", version="1.0.0")


@app.get("/")
def health():
    """Simple health check to confirm API and Redis are live."""
    try:
        r.ping()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis not reachable: {e}")


@app.post("/transactions")
async def enqueue_transaction(
    txn: Transaction,
    x_api_token: Optional[str] = Header(None, alias="X-API-TOKEN")
):
    if API_TOKEN and x_api_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = txn.dict()
    payload["amount"] = float(payload["amount"])  # convert Decimal to float
    r.rpush(QUEUE_NAME, json.dumps(payload))

    # fire-and-forget GitHub Action trigger
    asyncio.create_task(trigger_pipeline())

    return {"status": "enqueued", "queue": QUEUE_NAME, "transaction_id": txn.transaction_id}


@app.post("/transactions/batch")
async def enqueue_batch(
    txns: List[Transaction],
    x_api_token: Optional[str] = Header(None, alias="X-API-TOKEN")
):
    if API_TOKEN and x_api_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    pipe = r.pipeline()
    for t in txns:
        payload = t.dict()
        payload["amount"] = float(payload["amount"])
        pipe.rpush(QUEUE_NAME, json.dumps(payload))
    pipe.execute()

    # fire-and-forget GitHub Action trigger
    asyncio.create_task(trigger_pipeline())

    return {"status": "enqueued", "queue": QUEUE_NAME, "count": len(txns)}
