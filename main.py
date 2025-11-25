import os
from fastapi import FastAPI, Request, BackgroundTasks
import requests
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = FastAPI()

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
        requests.post(endpoint, json=payload, headers=headers, timeout=10)
    except Exception:
        pass

def _is_start_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/start")

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
    data = body.get("data") or body
    if event == "message_created":
        content = data.get("content")
        message_type = data.get("message_type")
        if _is_start_command(content) and message_type == "incoming":
            conversation_id = data.get("conversation_id")
            account_id = data.get("account_id")
            if conversation_id and account_id:
                background_tasks.add_task(
                    send_chatwoot_reply, int(account_id), int(conversation_id), WELCOME_TEXT
                )
    return {"status": "ok"}
