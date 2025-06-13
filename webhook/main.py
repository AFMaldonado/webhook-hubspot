from fastapi import FastAPI, Request, Header, HTTPException
from google.cloud import pubsub_v1
import os
import json
import hashlib

app = FastAPI()

# Configuración global
HUBSPOT_SECRET_TOKEN = os.environ.get("HUBSPOT_SECRET_TOKEN")
PUBSUB_PROJECT_ID = os.environ.get("PUBSUB_PROJECT_ID")
PUBSUB_TOPIC_ID = os.environ.get("PUBSUB_TOPIC_ID")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, PUBSUB_TOPIC_ID)

def compute_hubspot_signature(secret_token: str, payload: dict) -> str:
    """Calcula el hash SHA256 como firma para validar la autenticidad."""
    request_body = json.dumps(payload, separators=(',', ':'))  # Compacta sin espacios
    source_string = f"{secret_token}{request_body}"
    return hashlib.sha256(source_string.encode("utf-8")).hexdigest()

def validate_signature(header_signature: str, expected_signature: str):
    """Lanza excepción si la firma no es válida."""
    if header_signature != expected_signature:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid signature")

def publish_to_pubsub(payload: dict) -> str:
    """Publica el mensaje en Pub/Sub y retorna el ID del mensaje."""
    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(topic_path, data)
    return future.result()

@app.post("/")
async def receive_webhook(
    request: Request,
    hubspot_signature: str = Header(default=None, alias="X-HubSpot-Signature"),
):
    # Obtener y verificar el cuerpo
    payload = await request.json()
    expected_signature = compute_hubspot_signature(HUBSPOT_SECRET_TOKEN, payload)
    validate_signature(hubspot_signature, expected_signature)

    # Publicar en Pub/Sub
    try:
        message_id = publish_to_pubsub(payload)
        print(f"📨 Mensaje publicado en Pub/Sub con ID: {message_id}")
        return {"status": "ok", "message_id": message_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al publicar en Pub/Sub: {str(e)}")
