"""
Minimal FastAPI service to enqueue transactions in Redis.
Env vars required:
  REDIS_URL   -> e.g. redis://:<PASSWORD>@<HOST>:<PORT>
Optional:
  QUEUE_NAME  -> default "transactions_queue"
  API_TOKEN   -> if set, header 'X-API-TOKEN' must match
"""

import os
import json
from typing import Optional, List
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field, condecimal
import redis

# -------- Configuration --------
REDIS_URL  = os.getenv("REDIS_URL")
QUEUE_NAME = os.getenv("QUEUE_NAME", "transactions_queue")
API_TOKEN  = os.getenv("API_TOKEN")

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not set. Configure it in Railway env vars.")

r = redis.from_url(REDIS_URL, decode_responses=True)  # handles redis:// or rediss://

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
    try:
        r.ping()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Redis not reachable: {e}")

@app.post("/transactions")
def enqueue_transaction(txn: Transaction, x_api_token: Optional[str] = Header(None, alias="X-API-TOKEN")):
    if API_TOKEN and x_api_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    payload = txn.dict()
    payload["amount"] = float(payload["amount"])  # convert Decimal
    r.rpush(QUEUE_NAME, json.dumps(payload))
    return {"status": "enqueued", "queue": QUEUE_NAME, "transaction_id": txn.transaction_id}

@app.post("/transactions/batch")
def enqueue_batch(txns: List[Transaction], x_api_token: Optional[str] = Header(None, alias="X-API-TOKEN")):
    if API_TOKEN and x_api_token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")

    pipe = r.pipeline()
    for t in txns:
        payload = t.dict()
        payload["amount"] = float(payload["amount"])
        pipe.rpush(QUEUE_NAME, json.dumps(payload))
    pipe.execute()
    return {"status": "enqueued", "queue": QUEUE_NAME, "count": len(txns)}
