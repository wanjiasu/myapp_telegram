import os
import logging
from fastapi import FastAPI, Request, BackgroundTasks
import requests
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = FastAPI()
logger = logging.getLogger(__name__)

WELCOME_TEXT = "欢迎使用客服机器人！输入 /start 获取帮助"

def _chatwoot_base_url() -> str:
    url = os.getenv("CHATWOOT_BASE_URL", "")
    return url.rstrip("/")

def _chatwoot_token() -> str:
    return os.getenv("CHATWOOT_API_ACCESS_TOKEN", "")

def send_chatwoot_reply(account_id: int, conversation_id: int, content: str) -> None:
    base_url = _chatwoot_base_url()
    token = _chatwoot_token()
    if not base_url or not token:
        logger.warning("Chatwoot env missing, skip reply")
        return
    endpoint = f"{base_url}/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
    payload = {
        "content": content,
        "message_type": "outgoing",
        "private": False,
        "content_type": "text",
    }
    headers = {"Content-Type": "application/json", "api_access_token": token}
    try:
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=10)
        if resp.status_code >= 300:
            logger.error(f"Chatwoot reply failed: {resp.status_code} {resp.text[:200]}")
    except Exception:
        logger.exception("Chatwoot reply error")

def _is_start_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/start")

def _extract_chatwoot_fields(body: dict):
    b = body or {}
    data = b.get("data") or b.get("payload") or b
    message = data.get("message") or data.get("messages") or {}
    if isinstance(message, list):
        message = message[-1] if message else {}
    conversation = data.get("conversation") or b.get("conversation") or {}
    content = data.get("content") or message.get("content") or b.get("content")
    message_type = data.get("message_type") or message.get("message_type") or b.get("message_type")
    conversation_id = (
        data.get("conversation_id")
        or message.get("conversation_id")
        or conversation.get("id")
        or b.get("conversation_id")
    )
    account_id = (
        data.get("account_id")
        or conversation.get("account_id")
        or message.get("account_id")
        or b.get("account_id")
        or (b.get("account") or {}).get("id")
    )
    return content, message_type, conversation_id, account_id

@app.get("/start")
async def start():
    return {"message": WELCOME_TEXT}

@app.post("/webhooks/chatwoot")
async def chatwoot_webhook(request: Request, background_tasks: BackgroundTasks):
    headers = {k.lower(): v for k, v in request.headers.items()}
    event = headers.get("x-chatwoot-event")
    body = await request.json()
    if not event:
        event = body.get("event")
    if event == "message_created":
        content, message_type, conversation_id, account_id = _extract_chatwoot_fields(body)
        if _is_start_command(content) and message_type == "incoming":
            if conversation_id and account_id:
                background_tasks.add_task(
                    send_chatwoot_reply, int(account_id), int(conversation_id), WELCOME_TEXT
                )
            else:
                d = body.get("data") or {}
                p = body.get("payload") or {}
                presence = {
                    "has_data": bool(body.get("data")),
                    "has_payload": bool(body.get("payload")),
                    "has_message": bool((d.get("message") or p.get("message") or body.get("message"))),
                    "has_conversation": bool((d.get("conversation") or p.get("conversation") or body.get("conversation"))),
                    "has_conversation_id": bool(d.get("conversation_id") or p.get("conversation_id") or body.get("conversation_id")),
                    "has_account_id": bool(
                        d.get("account_id")
                        or p.get("account_id")
                        or body.get("account_id")
                        or (d.get("conversation") or {}).get("account_id")
                        or (p.get("conversation") or {}).get("account_id")
                    ),
                }
                logger.warning(f"Webhook missing conversation_id/account_id, presence={presence}")
    return {"status": "ok"}

@app.get("/health")
async def health():
    base_url = _chatwoot_base_url()
    token = _chatwoot_token()
    return {
        "base_url_configured": bool(base_url),
        "token_configured": bool(token),
        "base_url": base_url or "",
    }
