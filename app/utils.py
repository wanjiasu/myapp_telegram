import re
import logging

logger = logging.getLogger(__name__)

def to_int(v):
    try:
        if v is None:
            return None
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        s = str(v).strip()
        m = re.search(r"-?\d+", s)
        return int(m.group(0)) if m else None
    except Exception:
        return None

def normalize_country(text: str):
    t = str(text or "").strip().lower()
    if not t:
        return None
    if ("菲律宾" in t) or ("ph" == t) or ("🇵🇭" in t):
        return "PH"
    if ("美国" in t) or ("us" == t) or ("🇺🇸" in t):
        return "US"
    return None

def format_tags(s: str) -> str:
    t = str(s or "").strip()
    if not t:
        return ""
    parts = [p.strip() for p in re.split(r"[\/\|,]+", t) if p and p.strip()]
    seen = set()
    out = []
    for p in parts:
        k = p.lower()
        if k not in seen:
            seen.add(k)
            out.append(p)
    return "🔥 " + " · ".join(out[:6])

def is_start_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/start")

def is_ai_pick_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/ai_pick")

def is_ai_history_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/ai_history")

def is_ai_yesterday_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/ai_yesterday")

def is_help_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/help")

def extract_chatwoot_fields(body: dict):
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

def extract_chatroom_id(body: dict):
    b = body or {}
    data = b.get("data") or b.get("payload") or b
    message = data.get("message") or {}
    conversation = data.get("conversation") or {}
    attrs_conv = conversation.get("additional_attributes") or {}
    attrs_msg = message.get("additional_attributes") or {}
    chatroom_id = (
        attrs_conv.get("chat_id")
        or attrs_msg.get("chat_id")
        or attrs_conv.get("source_id")
        or attrs_msg.get("source_id")
        or data.get("chat_id")
        or data.get("source_id")
        or message.get("source_id")
    )
    if not chatroom_id:
        cid = data.get("conversation_id") or message.get("conversation_id")
        if isinstance(cid, str):
            chatroom_id = cid
    return chatroom_id
