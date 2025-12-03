import logging
import os
import requests
import psycopg
from datetime import datetime, timezone
from .config import chatwoot_base_url, chatwoot_token, telegram_token, telegram_webhook_url, allowed_account_inbox_pairs, agent_url, agent_name, agent_endpoint_path
from .db import pg_dsn
from .utils import extract_chatwoot_fields, extract_chatroom_id, normalize_country, to_int

logger = logging.getLogger(__name__)

def send_chatwoot_reply(account_id: int, conversation_id: int, content: str, inbox_id: int = None) -> None:
    base_url = chatwoot_base_url()
    token = chatwoot_token()
    if not base_url or not token:
        logger.warning("Chatwoot env missing, skip reply")
        return
    try:
        allowed = allowed_account_inbox_pairs()
    except Exception:
        allowed = set()
    if allowed:
        try:
            if inbox_id is not None:
                if (int(account_id), int(inbox_id)) not in allowed:
                    return
            else:
                a = int(account_id)
                if all(pair[0] != a for pair in allowed):
                    return
        except Exception:
            return
    endpoint = f"{base_url}/api/v1/accounts/{account_id}/conversations/{conversation_id}/messages"
    payload = {"content": content, "message_type": "outgoing", "private": False, "content_type": "text"}
    headers = {"Content-Type": "application/json", "api_access_token": token}
    try:
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=10)
        if resp.status_code >= 300:
            logger.error(f"Chatwoot reply failed: {resp.status_code} {resp.text[:200]}")
    except Exception:
        logger.exception("Chatwoot reply error")

def send_telegram_country_keyboard(chatroom_id_raw) -> None:
    token = telegram_token()
    if not token or chatroom_id_raw is None:
        logger.warning("Telegram token/chat_id missing, skip keyboard")
        return
    chat_id = None
    try:
        if isinstance(chatroom_id_raw, int):
            chat_id = chatroom_id_raw
        else:
            import re
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
        "reply_markup": {"inline_keyboard": [[{"text": "🇵🇭 菲律宾", "callback_data": "PH"}, {"text": "🇺🇸 美国", "callback_data": "US"}]]},
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code >= 300:
            logger.error(f"Telegram keyboard failed: {resp.status_code} {resp.text[:200]}")
    except Exception:
        logger.exception("Telegram keyboard error")

def send_telegram_message(chatroom_id_raw, text: str) -> None:
    token = telegram_token()
    if not token or chatroom_id_raw is None or not text:
        return
    chat_id = None
    try:
        if isinstance(chatroom_id_raw, int):
            chat_id = chatroom_id_raw
        else:
            import re
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

def set_telegram_webhook() -> None:
    token = telegram_token()
    url = telegram_webhook_url()
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

def post_agent_message(payload: dict, idempotency_key: str = None):
    url = agent_url()
    if not url:
        return None
    endpoint_path = agent_endpoint_path()
    endpoint = f"{url}{endpoint_path}"
    headers = {"Content-Type": "application/json"}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    try:
        if "/a2a/" in endpoint_path:
            try:
                msgs = payload.get("messages") or []
                text = None
                if isinstance(msgs, list) and msgs:
                    m0 = msgs[-1]
                    c = m0.get("content")
                    if isinstance(c, str):
                        text = c
                    elif isinstance(c, list):
                        for part in c:
                            t = part.get("text") or part.get("content") or part.get("output_text")
                            if t:
                                text = str(t)
                                break
                meta = payload.get("metadata") or {}
                rpc_payload = {
                    "jsonrpc": "2.0",
                    "id": meta.get("message_id") or "",
                    "method": "message/send",
                    "params": {
                        "message": {
                            "role": "user",
                            "parts": [{"kind": "text", "text": text or ""}],
                        },
                        "messageId": meta.get("message_id") or "",
                        "thread": {"threadId": meta.get("thread_id") or ""},
                    },
                }
            except Exception:
                rpc_payload = {
                    "jsonrpc": "2.0",
                    "id": "",
                    "method": "message/send",
                    "params": {
                        "message": {"role": "user", "parts": [{"kind": "text", "text": ""}]},
                        "messageId": "",
                        "thread": {"threadId": ""},
                    },
                }
            resp = requests.post(endpoint, json=rpc_payload, headers=headers, timeout=10)
        else:
            if "/runs" in endpoint_path:
                try:
                    msgs = payload.get("messages") or []
                    run_payload = {
                        "assistant_id": agent_name() or "query_agent",
                        "input": {"messages": msgs},
                    }
                    if endpoint_path.endswith("/stream"):
                        headers["Accept"] = "text/event-stream"
                        run_payload["stream_mode"] = "messages"
                        resp = requests.post(endpoint, json=run_payload, headers=headers, timeout=60, stream=True)
                        segments = []
                        acc_text = ""
                        try:
                            for line in resp.iter_lines(decode_unicode=True):
                                if not line:
                                    continue
                                s = line.strip()
                                if s.startswith("data:"):
                                    import json as _json
                                    try:
                                        obj = _json.loads(s[5:].strip())
                                    except Exception:
                                        obj = None
                                    if isinstance(obj, list):
                                        for m in obj:
                                            c = m.get("content")
                                            if isinstance(c, str):
                                                if acc_text and c.startswith(acc_text):
                                                    delta = c[len(acc_text):]
                                                    if delta:
                                                        segments.append(delta)
                                                    acc_text = c
                                                else:
                                                    segments.append(c)
                                                    acc_text = c
                                            elif isinstance(c, list):
                                                parts_text = []
                                                for part in c:
                                                    t = part.get("text") or part.get("output_text") or part.get("content")
                                                    if t:
                                                        parts_text.append(str(t))
                                                if parts_text:
                                                    joined = "".join(parts_text)
                                                    if acc_text and joined.startswith(acc_text):
                                                        delta = joined[len(acc_text):]
                                                        if delta:
                                                            segments.append(delta)
                                                        acc_text = joined
                                                    else:
                                                        segments.append(joined)
                                                        acc_text = joined
                                    elif isinstance(obj, dict):
                                        data_obj = obj.get("data") or obj
                                        out_msgs = (
                                            data_obj.get("messages")
                                            or (data_obj.get("output") or {}).get("messages")
                                        )
                                        if not out_msgs:
                                            delta = data_obj.get("delta") or {}
                                            c = delta.get("content")
                                            if isinstance(c, str):
                                                if acc_text and c.startswith(acc_text):
                                                    d = c[len(acc_text):]
                                                    if d:
                                                        segments.append(d)
                                                    acc_text = c
                                                else:
                                                    segments.append(c)
                                                    acc_text = c
                                            elif isinstance(c, list):
                                                parts_text = []
                                                for part in c:
                                                    t = part.get("text") or part.get("output_text") or part.get("content")
                                                    if t:
                                                        parts_text.append(str(t))
                                                if parts_text:
                                                    joined = "".join(parts_text)
                                                    if acc_text and joined.startswith(acc_text):
                                                        d = joined[len(acc_text):]
                                                        if d:
                                                            segments.append(d)
                                                        acc_text = joined
                                                    else:
                                                        segments.append(joined)
                                                        acc_text = joined
                                        if isinstance(out_msgs, list):
                                            for m in out_msgs:
                                                c = m.get("content")
                                                if isinstance(c, str):
                                                    if acc_text and c.startswith(acc_text):
                                                        d = c[len(acc_text):]
                                                        if d:
                                                            segments.append(d)
                                                        acc_text = c
                                                    else:
                                                        segments.append(c)
                                                        acc_text = c
                                                elif isinstance(c, list):
                                                    parts_text = []
                                                    for part in c:
                                                        t = part.get("text") or part.get("output_text") or part.get("content")
                                                        if t:
                                                            parts_text.append(str(t))
                                                    if parts_text:
                                                        joined = "".join(parts_text)
                                                        if acc_text and joined.startswith(acc_text):
                                                            d = joined[len(acc_text):]
                                                            if d:
                                                                segments.append(d)
                                                            acc_text = joined
                                                        else:
                                                            segments.append(joined)
                                                            acc_text = joined
                        except Exception:
                            pass
                        if segments or acc_text:
                            final_text = acc_text if acc_text else "".join(segments)
                            return {"segments": [final_text]} if final_text else {"reply": "系统繁忙，请稍后再试。"}
                        # fallback to non-stream
                        try:
                            fallback_endpoint = endpoint.replace("/stream", "")
                            resp2 = requests.post(fallback_endpoint, json=run_payload, headers={k:v for k,v in headers.items() if k != "Accept"}, timeout=30)
                            if resp2.status_code < 300:
                                d2 = resp2.json()
                                out2 = d2.get("output") or {}
                                msgs2 = out2.get("messages") or d2.get("messages")
                                texts = []
                                if isinstance(msgs2, list):
                                    for m in msgs2:
                                        c = m.get("content")
                                        if isinstance(c, str):
                                            texts.append(c)
                                        elif isinstance(c, list):
                                            for part in c:
                                                t = part.get("text") or part.get("output_text") or part.get("content")
                                                if t:
                                                    texts.append(str(t))
                                if texts:
                                    return {"segments": texts}
                        except Exception:
                            pass
                        return {"reply": "系统繁忙，请稍后再试。"}
                    else:
                        resp = requests.post(endpoint, json=run_payload, headers=headers, timeout=20)
                except Exception:
                    resp = requests.post(endpoint, json=payload, headers=headers, timeout=10)
            else:
                resp = requests.post(endpoint, json=payload, headers=headers, timeout=10)
        if resp.status_code >= 300:
            return {"thread_id": None, "reply": "系统繁忙，请稍后再试。"}
        try:
            data = resp.json()
            if "/a2a/" in endpoint_path:
                try:
                    err = data.get("error")
                    if err:
                        return {"thread_id": None, "reply": "系统繁忙，请稍后再试。"}
                    res = data.get("result") or {}
                    texts = []
                    msg = res.get("message") or {}
                    parts = msg.get("parts") or []
                    for part in parts:
                        t = part.get("text") or part.get("output_text") or part.get("content")
                        if t:
                            texts.append(str(t))
                    if texts:
                        return {"thread_id": (res.get("thread") or {}).get("threadId"), "segments": texts}
                except Exception:
                    pass
            # /runs non-stream response normalization
            if "/runs" in endpoint_path:
                out = data.get("output") or {}
                out_msgs = out.get("messages") or data.get("messages")
                texts = []
                if isinstance(out_msgs, list):
                    for m in out_msgs:
                        c = m.get("content")
                        if isinstance(c, str):
                            texts.append(c)
                        elif isinstance(c, list):
                            for part in c:
                                t = part.get("text") or part.get("output_text") or part.get("content")
                                if t:
                                    texts.append(str(t))
                if texts:
                    return {"segments": texts}
            return data
        except Exception:
            return {"thread_id": None, "reply": "系统繁忙，请稍后再试。"}
    except Exception:
        logger.exception("Agent request error")
        return {"thread_id": None, "reply": "系统繁忙，请稍后再试。"}

def forward_chatwoot_to_agent(body: dict) -> None:
    try:
        content, message_type, conversation_id, account_id = extract_chatwoot_fields(body)
        if message_type != "incoming":
            return
        b = body or {}
        data = b.get("data") or b.get("payload") or b
        message = data.get("message") or {}
        sender = data.get("sender") or data.get("contact") or {}
        username = sender.get("name") or data.get("name") or b.get("name")
        chatroom_id_raw = extract_chatroom_id(body)
        msg_id = data.get("id") or message.get("id")
        inbox_id = (data.get("inbox_id") or message.get("inbox_id") or (data.get("conversation") or {}).get("inbox_id"))
        payload = {
            "messages": [{"role": "user", "content": content or ""}],
            "metadata": {
                "platform": "chatwoot",
                "agent": agent_name() or "query_agent",
                "chatroom_id": chatroom_id_raw or conversation_id,
                "thread_id": None,
                "message_id": msg_id,
                "sender_id": sender.get("id") or data.get("sender_id") or message.get("sender_id"),
                "username": username,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "conversation_id": conversation_id,
                "account_id": account_id,
                "inbox_id": inbox_id,
            },
        }
        idempotency_key = f"chatwoot:{msg_id}" if msg_id is not None else None
        acc_id_int = to_int(account_id)
        conv_id_int = to_int(conversation_id)
        inbox_id_int = to_int(inbox_id)
        if acc_id_int is not None and conv_id_int is not None:
            try:
                send_chatwoot_reply(acc_id_int, conv_id_int, "小助手正在加紧思考ing, 请稍后...", inbox_id_int)
            except Exception:
                pass
        result = post_agent_message(payload, idempotency_key)
        if not result:
            return
        reply = result.get("reply")
        segments = result.get("segments")
        msgs = result.get("messages")
        if not reply and not segments and isinstance(msgs, list):
            texts = []
            for m in msgs:
                try:
                    r = str(m.get("role")).lower()
                    if r in ("assistant", "tool"):
                        c = m.get("content")
                        if isinstance(c, str):
                            texts.append(c)
                        elif isinstance(c, list):
                            for part in c:
                                t = part.get("text") or part.get("content") or part.get("output_text")
                                if t:
                                    texts.append(str(t))
                except Exception:
                    pass
            if texts:
                segments = texts
        acc_id_int = to_int(account_id)
        conv_id_int = to_int(conversation_id)
        inbox_id_int = to_int(inbox_id)
        if acc_id_int is not None and conv_id_int is not None:
            if isinstance(segments, list):
                for seg in segments:
                    if isinstance(seg, str) and len(seg) > 3500:
                        t = seg
                        while t:
                            send_chatwoot_reply(acc_id_int, conv_id_int, t[:3000], inbox_id_int)
                            t = t[3000:]
                    else:
                        send_chatwoot_reply(acc_id_int, conv_id_int, seg, inbox_id_int)
            elif isinstance(reply, str):
                if len(reply) > 3500:
                    t = reply
                    while t:
                        send_chatwoot_reply(acc_id_int, conv_id_int, t[:3000], inbox_id_int)
                        t = t[3000:]
                else:
                    send_chatwoot_reply(acc_id_int, conv_id_int, reply, inbox_id_int)
    except Exception:
        logger.exception("Forward chatwoot to agent error")

def forward_telegram_to_agent(body: dict) -> None:
    try:
        msg = body.get("message") or {}
        text = msg.get("text") or ""
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        message_id = msg.get("message_id")
        sender = msg.get("from") or {}
        sender_id = sender.get("id")
        username = sender.get("first_name") or sender.get("username")
        if chat_id is not None:
            try:
                send_telegram_message(chat_id, "小助手正在加紧思考ing, 请稍后...")
            except Exception:
                pass
        payload = {
            "messages": [{"role": "user", "content": text}],
            "metadata": {
                "platform": "telegram",
                "agent": agent_name() or "query_agent",
                "chatroom_id": chat_id,
                "thread_id": None,
                "message_id": message_id,
                "sender_id": sender_id,
                "username": username,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }
        idempotency_key = f"telegram:{message_id}" if message_id is not None else None
        result = post_agent_message(payload, idempotency_key)
        if not result:
            return
        reply = result.get("reply")
        segments = result.get("segments")
        msgs = result.get("messages")
        if not reply and not segments and isinstance(msgs, list):
            texts = []
            for m in msgs:
                try:
                    r = str(m.get("role")).lower()
                    if r in ("assistant", "tool"):
                        c = m.get("content")
                        if isinstance(c, str):
                            texts.append(c)
                        elif isinstance(c, list):
                            for part in c:
                                t = part.get("text") or part.get("content") or part.get("output_text")
                                if t:
                                    texts.append(str(t))
                except Exception:
                    pass
            if texts:
                segments = texts
        if isinstance(segments, list):
            for seg in segments:
                if seg:
                    send_telegram_message(chat_id, seg)
        elif isinstance(reply, str) and reply:
            send_telegram_message(chat_id, reply)
    except Exception:
        logger.exception("Forward telegram to agent error")
def set_user_country(body: dict, choice_text: str) -> None:
    try:
        country = normalize_country(choice_text)
        if not country:
            return
        external_id = None
        chatroom_id_raw = extract_chatroom_id(body)
        b = body or {}
        data = b.get("data") or b.get("payload") or b
        sender = data.get("sender") or data.get("contact") or {}
        external_id = sender.get("id") or data.get("sender_id") or (data.get("contact") or {}).get("id")
        username = sender.get("name") or data.get("name") or b.get("name")
        with psycopg.connect(pg_dsn()) as conn:
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

def store_message(body: dict) -> None:
    try:
        content, message_type, conversation_id, account_id = extract_chatwoot_fields(body)
        chatroom_id_raw = extract_chatroom_id(body)
        b = body or {}
        data = b.get("data") or b.get("payload") or b
        sender = data.get("sender") or data.get("contact") or {}
        message = data.get("message") or {}
        contact = data.get("contact") or {}
        external_id = sender.get("id") or data.get("sender_id") or message.get("sender_id")
        msg_id = data.get("id") or message.get("id")
        inbox_id = (data.get("inbox_id") or message.get("inbox_id") or (data.get("conversation") or {}).get("inbox_id"))
        source_id = (
            data.get("source_id")
            or message.get("source_id")
            or (data.get("conversation") or {}).get("source_id")
            or ((data.get("conversation") or {}).get("additional_attributes") or {}).get("source_id")
            or (message.get("additional_attributes") or {}).get("source_id")
        )
        username = sender.get("name") or data.get("name") or b.get("name")
        with psycopg.connect(pg_dsn()) as conn:
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

def send_lark_help_alert(body: dict) -> None:
    url = os.getenv("LARK_BOT_WEBHOOK_URL", "")
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
        chatroom_id = extract_chatroom_id(body)
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
