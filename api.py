"""
Minimal FastAPI service to enqueue transactions in Redis and trigger GitHub Fraud Batch Pipeline ETL workflow.

Required env vars on Railway:
  REDIS_URL    -> redis://:<PASSWORD>@<HOST>:<PORT>
  GITHUB_TOKEN -> GitHub PAT with 'repo' scope
Optional:
  QUEUE_NAME   -> default "transactions_queue"
  API_TOKEN    -> if set, header 'X-API-TOKEN' must match
"""

import os, json, asyncio
from typing import Optional, List
from decimal import Decimal
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field, condecimal
import redis
import httpx

# ----- Configuration -----
REDIS_URL   = os.getenv("REDIS_URL")
QUEUE_NAME  = os.getenv("QUEUE_NAME", "transactions_queue")
API_TOKEN   = os.getenv("API_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO        = "nholderTDM/humana-fraud-cloud"   # adjust if repo path changes

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not set in Railway env vars.")

r = redis.from_url(REDIS_URL, decode_responses=True)

# ----- GitHub trigger -----
async def trigger_pipeline():
    if not GITHUB_TOKEN:
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

# ----- Data model -----
class Transaction(BaseModel):
    transaction_id: str = Field(..., min_length=3)
    amount: condecimal(gt=0)
    location: Optional[str] = "USA"
    device:   Optional[str] = "Web"

# ----- FastAPI app -----
app = FastAPI(title="Fraud Ingestion API", version="1.0.0")

@app.get("/")
def health():
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
    payload["amount"] = float(payload["amount"])
    r.rpush(QUEUE_NAME, json.dumps(payload))
    asyncio.create_task(trigger_pipeline())  # trigger GitHub workflow
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
    asyncio.create_task(trigger_pipeline())
    return {"status": "enqueued", "queue": QUEUE_NAME, "count": len(txns)}
