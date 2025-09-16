"""
api.py
------
Minimal FastAPI service that accepts transactions and enqueues them in Redis.
The fraud batch pipeline (GitHub Actions) will drain the queue on the next run.

Env vars required in hosting platform (Railway):
  REDIS_URL   -> e.g., rediss://:<PASSWORD>@<HOST>:<PORT
Optional env vars:
  QUEUE_NAME  -> default "transactions_queue"
  API_TOKEN   -> if set, clients must provide header 'X-API-TOKEN: <token>'
"""

import os
import json
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field, condecimal
import redis


# --------- configuration ----------
REDIS_URL = os.getenv("REDIS_URL")
QUEUE_NAME = os.getenv("QUEUE_NAME", "transactions_queue")
API_TOKEN = os.getenv("API_TOKEN")  # optional auth

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not set. Configure it in Railway env vars.")

r = redis.from_url(REDIS_URL, decode_responses=True)  # decode strings, use TLS with rediss://


# --------- pydantic models --------
class Transaction(BaseModel):
    transaction_id: str = Field(..., min_length=3)
    amount: condecimal(gt=0)  # positive amount
    location: Optional[str] = "USA"
    device: Optional[str] = "Web"


# --------- app init ---------------
app = FastAPI(title="Fraud Ingestion API", version="1.0.0")


@app.get("/")
def health():
    """Simple health check."""
    try:
        r.ping()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis not reachable: {e}")


@app.post("/transactions")
def enqueue_transaction(txn: Transaction, x_api_token: Optional[str] = Header(default=None, alias="X-API-TOKEN")):
    if API_TOKEN and x_api_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # Convert Decimal to float for JSON serialization
        payload = txn.dict()
        payload["amount"] = float(payload["amount"])
        r.rpush(QUEUE_NAME, json.dumps(payload))
        return {"status": "enqueued", "queue": QUEUE_NAME, "transaction_id": txn.transaction_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/transactions/batch")
def enqueue_batch(txns: list[Transaction], x_api_token: Optional[str] = Header(default=None, alias="X-API-TOKEN")):
    if API_TOKEN and x_api_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        pipe = r.pipeline()
        for t in txns:
            payload = t.dict()
            payload["amount"] = float(payload["amount"])
            pipe.rpush(QUEUE_NAME, json.dumps(payload))
        pipe.execute()
        return {"status": "enqueued", "queue": QUEUE_NAME, "count": len(txns)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
