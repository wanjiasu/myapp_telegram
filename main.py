import os
import logging
from fastapi import FastAPI, Request, BackgroundTasks
import requests
import psycopg
import json
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = FastAPI()
logger = logging.getLogger(__name__)

WELCOME_TEXT = "欢迎使用客服机器人！输入 /start 获取帮助"

def _pg_dsn() -> str:
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", user or "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

def init_db() -> None:
    try:
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGSERIAL PRIMARY KEY,
                        external_id TEXT UNIQUE,
                        username TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chat_messages (
                        id BIGSERIAL PRIMARY KEY,
                        chatroom_id TEXT,
                        account_id BIGINT,
                        conversation_id BIGINT,
                        user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
                        content TEXT,
                        message_type TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE chat_messages
                    ADD COLUMN IF NOT EXISTS message_id BIGINT,
                    ADD COLUMN IF NOT EXISTS sender_id TEXT,
                    ADD COLUMN IF NOT EXISTS contact_id TEXT,
                    ADD COLUMN IF NOT EXISTS inbox_id BIGINT,
                    ADD COLUMN IF NOT EXISTS source_id TEXT
                    """
                )
                conn.commit()
    except Exception:
        logger.exception("DB init error")

def store_message(body: dict) -> None:
    try:
        content, message_type, conversation_id, account_id = _extract_chatwoot_fields(body)
        b = body or {}
        data = b.get("data") or b.get("payload") or b
        sender = data.get("sender") or data.get("contact") or {}
        message = data.get("message") or {}
        contact = data.get("contact") or {}
        external_id = (
            sender.get("id")
            or data.get("sender_id")
            or message.get("sender_id")
        )
        msg_id = data.get("id") or message.get("id")
        inbox_id = (
            (data.get("inbox_id") or message.get("inbox_id") or (data.get("conversation") or {}).get("inbox_id"))
        )
        source_id = (
            data.get("source_id")
            or message.get("source_id")
            or (data.get("conversation") or {}).get("source_id")
            or ((data.get("conversation") or {}).get("additional_attributes") or {}).get("source_id")
            or (message.get("additional_attributes") or {}).get("source_id")
        )
        username = (
            sender.get("name")
            or data.get("name")
            or b.get("name")
        )
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                user_id = None
                if external_id is not None:
                    cur.execute(
                        """
                        INSERT INTO users (external_id, username)
                        VALUES (%s, %s)
                        ON CONFLICT (external_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            updated_at = NOW()
                        RETURNING id
                        """,
                        (str(external_id), username),
                    )
                    row = cur.fetchone()
                    user_id = row[0] if row else None
                try:
                    conv_id_int = int(conversation_id) if conversation_id is not None else None
                except Exception:
                    conv_id_int = None
                try:
                    acc_id_int = int(account_id) if account_id is not None else None
                except Exception:
                    acc_id_int = None
                try:
                    msg_id_int = int(msg_id) if msg_id is not None else None
                except Exception:
                    msg_id_int = None
                try:
                    inbox_id_int = int(inbox_id) if inbox_id is not None else None
                except Exception:
                    inbox_id_int = None
                cur.execute(
                    """
                    INSERT INTO chat_messages (chatroom_id, account_id, conversation_id, user_id, content, message_type, message_id, sender_id, contact_id, inbox_id, source_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(conversation_id) if conversation_id is not None else None,
                        acc_id_int,
                        conv_id_int,
                        user_id,
                        content,
                        message_type,
                        msg_id_int,
                        str(external_id) if external_id is not None else None,
                        str(contact.get("id")) if contact.get("id") is not None else None,
                        inbox_id_int,
                        str(source_id) if source_id is not None else None,
                    ),
                )
                conn.commit()
    except Exception:
        logger.exception("DB store error")

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
        try:
            logger.info(f"Webhook body={json.dumps(body, ensure_ascii=False)[:2000]}")
        except Exception:
            logger.info(f"Webhook body_unserializable={str(body)[:2000]}")
        content, message_type, conversation_id, account_id = _extract_chatwoot_fields(body)
        try:
            logger.info(
                f"Webhook extracted content={str(content)[:200]} type={message_type} conv_id={conversation_id} account_id={account_id}"
            )
        except Exception:
            pass
        if message_type == "incoming":
            background_tasks.add_task(store_message, body)
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
    db_ok = False
    try:
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                db_ok = True
    except Exception:
        db_ok = False
    return {
        "base_url_configured": bool(base_url),
        "token_configured": bool(token),
        "base_url": base_url or "",
        "db_connected": db_ok,
    }

@app.on_event("startup")
async def on_startup():
    init_db()
