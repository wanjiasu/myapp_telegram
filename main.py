import os
import logging
from fastapi import FastAPI, Request, BackgroundTasks
import requests
import psycopg
import json
import re
from datetime import datetime, timedelta, timezone
import asyncio
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

app = FastAPI()
logger = logging.getLogger(__name__)

WELCOME_TEXT = """欢迎使用客服机器人。
我们提供AI比赛推荐与基本面分析。
重点覆盖：英超、西甲、意甲、德甲、法甲、欧冠、世界杯。
请选择您所在的国家, 我们将为您用更准确的时间提供推荐。
"""

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
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS chatroom_id TEXT
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
                cur.execute(
                    """
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS country TEXT
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ai_eval (
                        id BIGSERIAL PRIMARY KEY,
                        fixture_id BIGINT,
                        predict_winner TEXT,
                        confidence DOUBLE PRECISION,
                        key_tag_evidence TEXT,
                        if_bet SMALLINT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE ai_eval
                    ADD COLUMN IF NOT EXISTS result TEXT
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS api_football_fixtures (
                        id BIGSERIAL PRIMARY KEY,
                        fixture_id BIGINT UNIQUE,
                        fixture_date TIMESTAMPTZ,
                        home_name TEXT,
                        away_name TEXT
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS push_log (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                        push_date DATE NOT NULL,
                        push_type TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uniq_push_log ON push_log(user_id, push_date, push_type)
                    """
                )
                conn.commit()
    except Exception:
        logger.exception("DB init error")

def store_message(body: dict) -> None:
    try:
        content, message_type, conversation_id, account_id = _extract_chatwoot_fields(body)
        chatroom_id_raw = _extract_chatroom_id(body)
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
                        INSERT INTO users (external_id, username, chatroom_id)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (external_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            chatroom_id = COALESCE(EXCLUDED.chatroom_id, users.chatroom_id),
                            updated_at = NOW()
                        RETURNING id
                        """,
                        (str(external_id), username, str(chatroom_id_raw) if chatroom_id_raw is not None else None),
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
                        str(chatroom_id_raw) if chatroom_id_raw is not None else (str(conversation_id) if conversation_id is not None else None),
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

def _to_int(v):
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

def _telegram_token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "")

def send_telegram_country_keyboard(chatroom_id_raw) -> None:
    token = _telegram_token()
    if not token or chatroom_id_raw is None:
        logger.warning("Telegram token/chat_id missing, skip keyboard")
        return
    chat_id = None
    try:
        if isinstance(chatroom_id_raw, int):
            chat_id = chatroom_id_raw
        else:
            m = re.search(r"-?\d+", str(chatroom_id_raw))
            chat_id = int(m.group(0)) if m else None
    except Exception:
        chat_id = None
    if chat_id is None:
        logger.warning("Telegram chat_id parse failed, skip keyboard")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "请选择地区",
        "reply_markup": {
            "inline_keyboard": [
                [
                    {"text": "🇵🇭 菲律宾", "callback_data": "PH"},
                    {"text": "🇺🇸 美国", "callback_data": "US"},
                ]
            ]
        },
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code >= 300:
            logger.error(f"Telegram keyboard failed: {resp.status_code} {resp.text[:200]}")
    except Exception:
        logger.exception("Telegram keyboard error")

def send_telegram_message(chatroom_id_raw, text: str) -> None:
    token = _telegram_token()
    if not token or chatroom_id_raw is None or not text:
        return
    chat_id = None
    try:
        if isinstance(chatroom_id_raw, int):
            chat_id = chatroom_id_raw
        else:
            m = re.search(r"-?\d+", str(chatroom_id_raw))
            chat_id = int(m.group(0)) if m else None
    except Exception:
        chat_id = None
    if chat_id is None:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception:
        logger.exception("Telegram sendMessage error")

def _telegram_webhook_url() -> str:
    return os.getenv("TELEGRAM_WEBHOOK_URL", "")

def set_telegram_webhook() -> None:
    token = _telegram_token()
    url = _telegram_webhook_url()
    if not token or not url:
        return
    api = f"https://api.telegram.org/bot{token}/setWebhook"
    try:
        resp = requests.post(api, json={"url": url}, timeout=10)
        if resp.status_code >= 300:
            logger.error(f"Telegram setWebhook failed: {resp.status_code} {resp.text[:200]}")
    except Exception:
        logger.exception("Telegram setWebhook error")

def answer_callback_query(token: str, callback_id: str, text: str = None) -> None:
    if not token or not callback_id:
        return
    api = f"https://api.telegram.org/bot{token}/answerCallbackQuery"
    payload = {"callback_query_id": callback_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = False
    try:
        resp = requests.post(api, json=payload, timeout=10)
        if resp.status_code >= 300:
            logger.error(f"Telegram answerCallbackQuery failed: {resp.status_code} {resp.text[:200]}")
    except Exception:
        logger.exception("Telegram answerCallbackQuery error")

def _is_start_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/start")

def _is_ai_pick_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/ai_pick")

def _is_ai_history_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/ai_history")

def _is_help_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/help")

def _is_ai_yesterday_command(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return False
    return t.startswith("/ai_yesterday")

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

def _extract_chatroom_id(body: dict):
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

def _read_offset(country: str) -> int:
    try:
        path = os.path.join(os.path.dirname(__file__), "时差.json")
        with open(path, "r", encoding="utf-8") as f:
            m = json.load(f)
        v = m.get(country)
        return int(v) if v is not None else 0
    except Exception:
        return 0

def _get_country_for_chat(body: dict) -> str:
    b = body or {}
    data = b.get("data") or b.get("payload") or b
    chatroom_id = _extract_chatroom_id(body)
    external_id = (
        (data.get("sender") or {}).get("id")
        or data.get("sender_id")
        or (data.get("contact") or {}).get("id")
    )
    with psycopg.connect(_pg_dsn()) as conn:
        with conn.cursor() as cur:
            country = None
            if chatroom_id is not None:
                cur.execute("SELECT country FROM users WHERE chatroom_id = %s LIMIT 1", (str(chatroom_id),))
                row = cur.fetchone()
                country = row[0] if row else None
            if (not country) and external_id is not None:
                cur.execute("SELECT country FROM users WHERE external_id = %s LIMIT 1", (str(external_id),))
                row = cur.fetchone()
                country = row[0] if row else None
            return country or None

def _format_tags(s: str) -> str:
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

def _is_prediction_success(predict_winner, result) -> bool:
    try:
        p = str(predict_winner).strip().lower()
        r = str(result).strip().lower()
        if not p or not r:
            return False
        return p == r
    except Exception:
        return False

def _calc_accuracy(rows, start=None, end=None) -> float:
    filtered = []
    for row in rows:
        dt = row.get("fixture_date")
        if dt is None:
            continue
        if start and dt < start:
            continue
        if end and dt >= end:
            continue
        filtered.append(row)
    total = len(filtered)
    if total == 0:
        return 0.0
    success = sum(1 for r in filtered if _is_prediction_success(r.get("predict_winner"), r.get("result")))
    return round((success / total) * 100, 1)

def _ai_history_reply(body: dict) -> str:
    country = _get_country_for_chat(body)
    offset = _read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local_now = now_utc + timedelta(hours=offset)
    local_today = datetime(local_now.year, local_now.month, local_now.day, tzinfo=timezone.utc)
    today_start_utc = local_today - timedelta(hours=offset)
    yesterday_start = today_start_utc - timedelta(days=1)
    yesterday_end = today_start_utc
    last7_start = now_utc - timedelta(days=7)
    last7_end = now_utc
    rows = []
    try:
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT e.fixture_id,
                           e.predict_winner,
                           e.result,
                           e.confidence,
                           f.fixture_date,
                           f.home_name,
                           f.away_name
                    FROM ai_eval e
                    INNER JOIN api_football_fixtures f ON f.fixture_id = e.fixture_id
                    WHERE COALESCE(e.if_bet, 0) = 1
                      --AND e.confidence > 0.6
                      AND e.result IS NOT NULL
                    ORDER BY f.fixture_date DESC
                    """
                )
                fetched = cur.fetchall() or []
                rows = [
                    {
                        "fixture_id": r[0],
                        "predict_winner": r[1],
                        "result": r[2],
                        "confidence": r[3],
                        "fixture_date": r[4],
                        "home_name": r[5],
                        "away_name": r[6],
                    }
                    for r in fetched
                ]
    except Exception:
        logger.exception("DB fetch ai_history error")
    if not rows:
        return "暂时没有AI历史记录，可以稍后再试哦～"
    overall = _calc_accuracy(rows)
    acc_7d = _calc_accuracy(rows, start=last7_start, end=last7_end)
    acc_yesterday = _calc_accuracy(rows, start=yesterday_start, end=yesterday_end)
    emojis = []
    for r in rows[:10]:
        emojis.append("✅" if _is_prediction_success(r.get("predict_winner"), r.get("result")) else "❌")
    emoji_line = "".join(emojis) if emojis else "暂无记录"
    return (
        f"📊 AI历史预测准确率: {overall:.1f}%\n\n"
        f"🗓️ AI7天内预测准确率: {acc_7d:.1f}%\n\n"
        f"🌙 AI昨日预测准确率: {acc_yesterday:.1f}%\n\n"
        f"🎯 AI最近10场预测:\n{emoji_line}"
    )

def _ai_yesterday_reply(body: dict) -> str:
    country = _get_country_for_chat(body)
    offset = _read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local_now = now_utc + timedelta(hours=offset)
    local_today = datetime(local_now.year, local_now.month, local_now.day, tzinfo=timezone.utc)
    today_start_utc = local_today - timedelta(hours=offset)
    yesterday_start = today_start_utc - timedelta(days=1)
    yesterday_end = today_start_utc
    rows = []
    acc = 0.0
    try:
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    
                    """
                    SELECT e.fixture_id,
                           e.predict_winner,
                           e.result,
                           e.confidence,
                           f.fixture_date,
                           f.home_name,
                           f.away_name,
                           CASE WHEN e.predict_winner ~ '^-?\\d+$' AND e.result ~ '^-?\\d+$' AND (e.predict_winner)::int = (e.result)::int THEN 1 ELSE 0 END AS success
                    FROM ai_eval e
                    INNER JOIN api_football_fixtures f ON f.fixture_id = e.fixture_id
                    WHERE COALESCE(e.if_bet, 0) = 1
                      AND e.confidence > 0.6
                      AND e.result IS NOT NULL
                      AND f.fixture_date >= %s AND f.fixture_date < %s
                    ORDER BY f.fixture_date ASC
                    """,
                    (yesterday_start, yesterday_end),
                )
                fetched = cur.fetchall() or []
                rows = [
                    {
                        "fixture_id": r[0],
                        "predict_winner": r[1],
                        "result": r[2],
                        "confidence": r[3],
                        "fixture_date": r[4],
                        "home_name": r[5],
                        "away_name": r[6],
                        "success": r[7],
                    }
                    for r in fetched
                ]
                cur.execute(
                    
                    """
                    SELECT COALESCE(ROUND(
                               SUM(CASE WHEN e.predict_winner ~ '^-?\\d+$' AND e.result ~ '^-?\\d+$' AND (e.predict_winner)::int = (e.result)::int THEN 1 ELSE 0 END)::numeric
                               / NULLIF(COUNT(1), 0) * 100, 1
                           ), 0.0) AS acc
                    FROM ai_eval e
                    INNER JOIN api_football_fixtures f ON f.fixture_id = e.fixture_id
                    WHERE COALESCE(e.if_bet, 0) = 1
                      AND e.confidence > 0.6
                      AND e.result IS NOT NULL
                      AND f.fixture_date >= %s AND f.fixture_date < %s
                    """,
                    (yesterday_start, yesterday_end),
                )
                row_acc = cur.fetchone()
                acc = float(row_acc[0]) if row_acc and row_acc[0] is not None else 0.0
    except Exception:
        logger.exception("DB fetch ai_yesterday error")
    if not rows:
        return "昨天暂无AI记录，可以稍后再试哦～"
    lines = []
    for i, r in enumerate(rows, 1):
        ok = bool(r.get("success"))
        emoji = "✅" if ok else "❌"
        lines.append(f"{i}. {r.get('home_name')} vs {r.get('away_name')} {emoji}")
    body_text = "\n".join(lines)
    return f"📊 AI昨日预测准确率: {acc:.1f}%\n\n{body_text}"

def _ai_yesterday_text_for_country(country: str) -> str:
    offset = _read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local_now = now_utc + timedelta(hours=offset)
    local_today = datetime(local_now.year, local_now.month, local_now.day, tzinfo=timezone.utc)
    today_start_utc = local_today - timedelta(hours=offset)
    yesterday_start = today_start_utc - timedelta(days=1)
    yesterday_end = today_start_utc
    rows = []
    acc = 0.0
    try:
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT e.fixture_id,
                           e.predict_winner,
                           e.result,
                           e.confidence,
                           f.fixture_date,
                           f.home_name,
                           f.away_name,
                           CASE WHEN e.predict_winner ~ '^-?\\d+$' AND e.result ~ '^-?\\d+$' AND (e.predict_winner)::int = (e.result)::int THEN 1 ELSE 0 END AS success
                    FROM ai_eval e
                    INNER JOIN api_football_fixtures f ON f.fixture_id = e.fixture_id
                    WHERE COALESCE(e.if_bet, 0) = 1
                      AND e.confidence > 0.6
                      AND e.result IS NOT NULL
                      AND f.fixture_date >= %s AND f.fixture_date < %s
                    ORDER BY f.fixture_date ASC
                    """,
                    (yesterday_start, yesterday_end),
                )
                fetched = cur.fetchall() or []
                rows = [
                    {
                        "home_name": r[5],
                        "away_name": r[6],
                        "success": r[7],
                    }
                    for r in fetched
                ]
                cur.execute(
                    """
                    SELECT COALESCE(ROUND(
                               SUM(CASE WHEN e.predict_winner ~ '^-?\\d+$' AND e.result ~ '^-?\\d+$' AND (e.predict_winner)::int = (e.result)::int THEN 1 ELSE 0 END)::numeric
                               / NULLIF(COUNT(1), 0) * 100, 1
                           ), 0.0) AS acc
                    FROM ai_eval e
                    INNER JOIN api_football_fixtures f ON f.fixture_id = e.fixture_id
                    WHERE COALESCE(e.if_bet, 0) = 1
                      AND e.confidence > 0.6
                      AND e.result IS NOT NULL
                      AND f.fixture_date >= %s AND f.fixture_date < %s
                    """,
                    (yesterday_start, yesterday_end),
                )
                row_acc = cur.fetchone()
                acc = float(row_acc[0]) if row_acc and row_acc[0] is not None else 0.0
    except Exception:
        logger.exception("DB fetch ai_yesterday country error")
    if not rows:
        return "昨天暂无AI记录，可以稍后再试哦～"
    lines = []
    for i, r in enumerate(rows, 1):
        emoji = "✅" if bool(r.get("success")) else "❌"
        lines.append(f"{i}. {r.get('home_name')} vs {r.get('away_name')} {emoji}")
    body_text = "\n".join(lines)
    return f"📊 AI昨日预测准确率: {acc:.1f}%\n\n{body_text}"

def _ai_pick_reply(body: dict) -> str:
    country = _get_country_for_chat(body)
    offset = _read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local = now_utc + timedelta(hours=offset)
    local_day = datetime(local.year, local.month, local.day, tzinfo=timezone.utc)
    tomorrow_local_day = local_day + timedelta(days=1)
    start_utc = tomorrow_local_day - timedelta(hours=offset)
    end_utc = start_utc + timedelta(days=1)
    rows = []
    with psycopg.connect(_pg_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.fixture_id, e.predict_winner, e.confidence, e.key_tag_evidence,
                       f.fixture_date, f.home_name, f.away_name
                FROM ai_eval e
                INNER JOIN api_football_fixtures f ON f.fixture_id = e.fixture_id
                WHERE COALESCE(e.if_bet, 0) = 1
                  AND e.confidence > 0.6
                  AND f.fixture_date >= %s AND f.fixture_date < %s
                ORDER BY f.fixture_date ASC
                """,
                (start_utc, end_utc),
            )
            rows = cur.fetchall() or []
    if not rows:
        return "明天暂无AI精选比赛，稍后再试试。"
    out = []
    for i, r in enumerate(rows, 1):
        fixture_id, predict_winner, confidence, key_tag_evidence, fixture_date, home_name, away_name = r
        when_local = fixture_date + timedelta(hours=offset) if fixture_date else None
        when_str = when_local.strftime("%Y-%m-%d %H:%M") if when_local else ""
        tags = _format_tags(key_tag_evidence)
        pw = str(predict_winner).strip().lower() if predict_winner is not None else ""
        if pw in ("3", "home", "主胜", "h"):
            result_label = "主胜"
        elif pw in ("1", "draw", "平局", "主平", "d"):
            result_label = "主平"
        elif pw in ("0", "away", "客胜", "a"):
            result_label = "客胜"
        else:
            result_label = str(predict_winner)
        try:
            confidence_pct = f"{round(float(confidence) * 100)}%"
        except Exception:
            confidence_pct = str(confidence)
        block = (
            f"⚽️ 第{i}场: {home_name} vs {away_name}\n"
            f"🕒 比赛时间: {when_str}\n"
            f"🏆 预测结果: {result_label}\n"
            f"🎯 把握: {confidence_pct}\n"
            f"💡 核心观点: {tags}\n"
            f"🔗 更多详情: https://betaione.com/fixture/{fixture_id}"
        )
        out.append(block)
    return "\n\n".join(out)

def _ai_pick_text_for_country(country: str) -> str:
    offset = _read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local = now_utc + timedelta(hours=offset)
    local_day = datetime(local.year, local.month, local.day, tzinfo=timezone.utc)
    tomorrow_local_day = local_day + timedelta(days=1)
    start_utc = now_utc
    end_utc = tomorrow_local_day - timedelta(hours=offset) + timedelta(days=1)
    rows = []
    with psycopg.connect(_pg_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select e.fixture_id, e.predict_winner, e.confidence, e.key_tag_evidence,
                       f.fixture_date, f.home_name, f.away_name
                from (select fixture_id, predict_winner, confidence, key_tag_evidence from ai_eval where if_bet = 1 and confidence > 0.6) e
                inner join
                (select fixture_id, fixture_date, home_name, away_name from api_football_fixtures where fixture_date >= %s and fixture_date < %s) f
                on e.fixture_id = f.fixture_id
                """,
                (start_utc, end_utc),
            )
            rows = cur.fetchall() or []
    if not rows:
        return "暂无AI精选比赛，稍后再试试。"
    out = []
    for i, r in enumerate(rows, 1):
        fixture_id, predict_winner, confidence, key_tag_evidence, fixture_date, home_name, away_name = r
        when_local = fixture_date + timedelta(hours=offset) if fixture_date else None
        when_str = when_local.strftime("%Y-%m-%d %H:%M") if when_local else ""
        tags = _format_tags(key_tag_evidence)
        pw = str(predict_winner).strip().lower() if predict_winner is not None else ""
        if pw in ("3", "home", "主胜", "h"):
            result_label = "主胜"
        elif pw in ("1", "draw", "平局", "主平", "d"):
            result_label = "主平"
        elif pw in ("0", "away", "客胜", "a"):
            result_label = "客胜"
        else:
            result_label = str(predict_winner)
        try:
            confidence_pct = f"{round(float(confidence) * 100)}%"
        except Exception:
            confidence_pct = str(confidence)
        block = (
            f"⚽️ 第{i}场: {home_name} vs {away_name}\n"
            f"🕒 比赛时间: {when_str}\n"
            f"🏆 预测结果: {result_label}\n"
            f"🎯 把握: {confidence_pct}\n"
            f"💡 核心观点: {tags}\n"
            f"🔗 更多详情: https://betaione.com/fixture/{fixture_id}"
        )
        out.append(block)
    return "\n\n".join(out)

def _extract_external_id(body: dict):
    b = body or {}
    data = b.get("data") or b.get("payload") or b
    sender = data.get("sender") or data.get("contact") or {}
    external_id = (
        sender.get("id")
        or data.get("sender_id")
        or (data.get("contact") or {}).get("id")
    )
    if external_id is None:
        external_id = _extract_chatroom_id(body)
    return external_id

def _normalize_country(text: str):
    t = str(text or "").strip().lower()
    if not t:
        return None
    if ("菲律宾" in t) or ("ph" == t) or ("🇵🇭" in t):
        return "PH"
    if ("美国" in t) or ("us" == t) or ("🇺🇸" in t):
        return "US"
    return None

def set_user_country(body: dict, choice_text: str) -> None:
    try:
        country = _normalize_country(choice_text)
        if not country:
            return
        external_id = _extract_external_id(body)
        chatroom_id_raw = _extract_chatroom_id(body)
        username = None
        b = body or {}
        data = b.get("data") or b.get("payload") or b
        sender = data.get("sender") or data.get("contact") or {}
        username = sender.get("name") or data.get("name") or b.get("name")
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (external_id, username, chatroom_id, country)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (external_id) DO UPDATE SET
                        username = COALESCE(EXCLUDED.username, users.username),
                        chatroom_id = COALESCE(EXCLUDED.chatroom_id, users.chatroom_id),
                        country = EXCLUDED.country,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (
                        str(external_id) if external_id is not None else None,
                        username,
                        str(chatroom_id_raw) if chatroom_id_raw is not None else None,
                        country,
                    ),
                )
                conn.commit()
    except Exception:
        logger.exception("DB set country error")

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
            if _is_help_command(content):
                background_tasks.add_task(send_lark_help_alert, body)
            choice = _normalize_country(content)
            if choice:
                background_tasks.add_task(set_user_country, body, content)
                acc_id_int = _to_int(account_id)
                conv_id_int = _to_int(conversation_id)
                if acc_id_int is not None and conv_id_int is not None:
                    ack = (
                        ("已选择菲律宾" if choice == "PH" else "已选择美国")
                        + "\n\n"
                        + "👇 可以点击左下方 menu 或直接发送以下指令\n"
                        + "🤖 /ai_pick - 查看 AI 今日推荐\n"
                        + "📊 /ai_history - 查看 AI 历史记录\n"
                        + "🆘 /help - 寻求人工客服协助"
                    )
                    background_tasks.add_task(
                        send_chatwoot_reply, acc_id_int, conv_id_int, ack
                    )
            if _is_ai_pick_command(content):
                try:
                    reply = _ai_pick_reply(body)
                    acc_id_int = _to_int(account_id)
                    conv_id_int = _to_int(conversation_id)
                    if acc_id_int is not None and conv_id_int is not None:
                        background_tasks.add_task(
                            send_chatwoot_reply, acc_id_int, conv_id_int, reply
                    )
                except Exception:
                    logger.exception("AI pick reply error")
            if _is_ai_history_command(content):
                try:
                    reply = _ai_history_reply(body)
                    acc_id_int = _to_int(account_id)
                    conv_id_int = _to_int(conversation_id)
                    if acc_id_int is not None and conv_id_int is not None:
                        background_tasks.add_task(
                            send_chatwoot_reply, acc_id_int, conv_id_int, reply
                        )
                except Exception:
                    logger.exception("AI history reply error")
            if _is_ai_yesterday_command(content):
                try:
                    reply = _ai_yesterday_reply(body)
                    acc_id_int = _to_int(account_id)
                    conv_id_int = _to_int(conversation_id)
                    if acc_id_int is not None and conv_id_int is not None:
                        background_tasks.add_task(
                            send_chatwoot_reply, acc_id_int, conv_id_int, reply
                        )
                except Exception:
                    logger.exception("AI yesterday reply error")
        if _is_start_command(content) and message_type == "incoming":
            acc_id_int = _to_int(account_id)
            conv_id_int = _to_int(conversation_id)
            if acc_id_int is not None and conv_id_int is not None:
                background_tasks.add_task(
                    send_chatwoot_reply, acc_id_int, conv_id_int, WELCOME_TEXT
                )
            try:
                chatroom_id_raw = _extract_chatroom_id(body)
                background_tasks.add_task(send_telegram_country_keyboard, chatroom_id_raw)
            except Exception:
                logger.exception("Send telegram keyboard on /start failed")
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
    asyncio.create_task(run_daily_push_scheduler())

@app.post("/webhooks/telegram")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    token = _telegram_token()
    msg = body.get("message") or {}
    cb = body.get("callback_query") or {}
    if msg:
        text = msg.get("text") or ""
        if _is_start_command(text):
            chat = msg.get("chat") or {}
            background_tasks.add_task(send_telegram_country_keyboard, chat.get("id"))
        choice = _normalize_country(text)
        if choice:
            background_tasks.add_task(set_user_country, body, text)
    if cb:
        data = cb.get("data") or ""
        choice = _normalize_country(data)
        if choice:
            background_tasks.add_task(set_user_country, body, data)
            background_tasks.add_task(answer_callback_query, token, cb.get("id"), "已记录选择")
    return {"status": "ok"}

def _list_users_for_push():
    try:
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, chatroom_id, country
                    FROM users
                    WHERE chatroom_id IS NOT NULL AND country IS NOT NULL
                    """
                )
                return cur.fetchall() or []
    except Exception:
        logger.exception("List users for push error")
        return []

def _has_pushed(user_id: int, push_date: datetime, push_type: str) -> bool:
    try:
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM push_log
                    WHERE user_id = %s AND push_date = %s AND push_type = %s
                    LIMIT 1
                    """,
                    (int(user_id), push_date.date(), push_type),
                )
                return bool(cur.fetchone())
    except Exception:
        return False

def _mark_pushed(user_id: int, push_date: datetime, push_type: str) -> None:
    try:
        with psycopg.connect(_pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO push_log (user_id, push_date, push_type)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, push_date, push_type) DO NOTHING
                    """,
                    (int(user_id), push_date.date(), push_type),
                )
                conn.commit()
    except Exception:
        logger.exception("Mark pushed error")

def _push_yesterday(user_row) -> None:
    try:
        user_id, chatroom_id, country = user_row
        text = _ai_yesterday_text_for_country(country)
        if text:
            send_telegram_message(chatroom_id, text)
    except Exception:
        logger.exception("Push yesterday error")

def _push_pick(user_row) -> None:
    try:
        user_id, chatroom_id, country = user_row
        text = _ai_pick_text_for_country(country)
        if text:
            send_telegram_message(chatroom_id, text)
    except Exception:
        logger.exception("Push pick error")

async def run_daily_push_scheduler():
    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            users = _list_users_for_push()
            for row in users:
                try:
                    user_id, chatroom_id, country = row
                    offset = _read_offset(country) if country else 0
                    local_now = now_utc + timedelta(hours=offset)
                    if local_now.hour == 11 and local_now.minute == 0:
                        if not _has_pushed(user_id, local_now, "yesterday"):
                            _push_yesterday(row)
                            _mark_pushed(user_id, local_now, "yesterday")
                    if local_now.hour == 20 and local_now.minute == 0:
                        if not _has_pushed(user_id, local_now, "pick"):
                            _push_pick(row)
                            _mark_pushed(user_id, local_now, "pick")
                except Exception:
                    logger.exception("Daily push per-user error")
        except Exception:
            logger.exception("Daily push scheduler error")
        await asyncio.sleep(60)

def _lark_webhook_url() -> str:
    return os.getenv("LARK_BOT_WEBHOOK_URL", "")

def send_lark_help_alert(body: dict) -> None:
    url = _lark_webhook_url()
    if not url:
        return
    try:
        b = body or {}
        data = b.get("data") or b.get("payload") or b
        message = data.get("message") or {}
        conversation = data.get("conversation") or {}
        sender = data.get("sender") or data.get("contact") or {}
        content = data.get("content") or message.get("content") or b.get("content") or ""
        username = sender.get("name") or data.get("name") or b.get("name") or ""
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
        chatroom_id = _extract_chatroom_id(body)
        text = (
            f"人工接入提醒\n"
            f"用户: {username or '未知'}\n"
            f"会话ID: {conversation_id or ''}\n"
            f"账户ID: {account_id or ''}\n"
            f"聊天ID: {chatroom_id or ''}\n"
            f"请求内容: {str(content)[:300]}"
        )
        payload = {"msg_type": "text", "content": {"text": text}}
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code >= 300:
            logger.error(f"Lark alert failed: {resp.status_code} {resp.text[:200]}")
    except Exception:
        logger.exception("Lark alert error")
