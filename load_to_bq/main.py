from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery
import base64
import json
import os

app = FastAPI()

# Load environment variables
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID")  # e.g., "my-project"
BQ_DATASET_ID = os.environ.get("BQ_DATASET_ID")  # e.g., "webhooks"
BQ_TABLE_ID = os.environ.get("BQ_TABLE_ID")      # e.g., "hubspot_events"

EXPECTED_COLUMNS = [
    'appId',
    'eventId',
    'subscriptionId',
    'portalId',
    'occurredAt',
    'subscriptionType',
    'attemptNumber',
    'objectId',
    'changeSource',
    'changeFlag'
]

def decode_pubsub_message(body: dict) -> list[dict]:
    """
    Decodes a base64-encoded Pub/Sub message and parses the JSON payload.
    """
    if "message" not in body or "data" not in body["message"]:
        raise ValueError("Invalid Pub/Sub message format")

    encoded_data = body["message"]["data"]
    decoded_bytes = base64.b64decode(encoded_data)
    payload = json.loads(decoded_bytes)

    return payload if isinstance(payload, list) else [payload]

def normalize_rows(rows: list[dict]) -> list[dict]:
    """
    Normalizes each row to match the expected BigQuery schema.
    """
    return [
        {col: row.get(col, None) for col in EXPECTED_COLUMNS}
        for row in rows
    ]

def insert_into_bigquery(rows: list[dict]) -> list:
    """
    Inserts a list of rows into the specified BigQuery table.
    """
    client = bigquery.Client()
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    return client.insert_rows_json(table_ref, rows)

@app.post("/hubspot-events")
async def receive_pubsub_message(request: Request):
    try:
        body = await request.json()

        rows = decode_pubsub_message(body)

        normalized_rows = normalize_rows(rows)

        errors = insert_into_bigquery(normalized_rows)

        if errors:
            raise HTTPException(status_code=500, detail=f"BigQuery insertion errors: {errors}")

        return {"status": "ok", "inserted_rows": len(normalized_rows)}

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing message: {str(e)}")
