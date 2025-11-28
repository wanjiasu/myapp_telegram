import logging
import json
import psycopg
import asyncio
from fastapi import APIRouter, Request, BackgroundTasks
from datetime import datetime, timezone
from .config import chatwoot_base_url, chatwoot_token, telegram_token
from .db import pg_dsn
from .utils import extract_chatwoot_fields, is_help_command, is_ai_pick_command, is_ai_history_command, is_ai_yesterday_command, is_start_command, normalize_country, extract_chatroom_id, to_int
from .services import send_chatwoot_reply, send_telegram_country_keyboard, answer_callback_query, set_user_country, store_message, send_lark_help_alert, send_telegram_message
from .ai import ai_pick_reply, ai_history_reply, ai_yesterday_reply

logger = logging.getLogger(__name__)

WELCOME_TEXT = """欢迎使用客服机器人。
我们提供AI比赛推荐与基本面分析。
重点覆盖：英超、西甲、意甲、德甲、法甲、欧冠、世界杯。
请选择您所在的国家, 我们将为您用更准确的时间提供推荐。
"""

router = APIRouter()

@router.get("/start")
async def start():
    return {"message": WELCOME_TEXT}

@router.post("/webhooks/chatwoot")
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
        content, message_type, conversation_id, account_id = extract_chatwoot_fields(body)
        try:
            logger.info(
                f"Webhook extracted content={str(content)[:200]} type={message_type} conv_id={conversation_id} account_id={account_id}"
            )
        except Exception:
            pass
        if message_type == "incoming":
            background_tasks.add_task(store_message, body)
            if is_help_command(content):
                background_tasks.add_task(send_lark_help_alert, body)
            choice = normalize_country(content)
            if choice:
                background_tasks.add_task(set_user_country, body, content)
                acc_id_int = to_int(account_id)
                conv_id_int = to_int(conversation_id)
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
            if is_ai_pick_command(content):
                try:
                    reply = ai_pick_reply(body)
                    acc_id_int = to_int(account_id)
                    conv_id_int = to_int(conversation_id)
                    if acc_id_int is not None and conv_id_int is not None:
                        if isinstance(reply, list):
                            for seg in reply:
                                if isinstance(seg, str) and len(seg) > 3500:
                                    t = seg
                                    while t:
                                        send_chatwoot_reply(acc_id_int, conv_id_int, t[:3000])
                                        t = t[3000:]
                                else:
                                    send_chatwoot_reply(acc_id_int, conv_id_int, seg)
                        else:
                            if isinstance(reply, str) and len(reply) > 3500:
                                t = reply
                                while t:
                                    send_chatwoot_reply(acc_id_int, conv_id_int, t[:3000])
                                    t = t[3000:]
                            else:
                                send_chatwoot_reply(acc_id_int, conv_id_int, reply)
                except Exception:
                    logger.exception("AI pick reply error")
            if is_ai_history_command(content):
                try:
                    reply = ai_history_reply(body)
                    acc_id_int = to_int(account_id)
                    conv_id_int = to_int(conversation_id)
                    if acc_id_int is not None and conv_id_int is not None:
                        background_tasks.add_task(
                            send_chatwoot_reply, acc_id_int, conv_id_int, reply
                        )
                except Exception:
                    logger.exception("AI history reply error")
            if is_ai_yesterday_command(content):
                try:
                    reply = ai_yesterday_reply(body)
                    acc_id_int = to_int(account_id)
                    conv_id_int = to_int(conversation_id)
                    if acc_id_int is not None and conv_id_int is not None:
                        background_tasks.add_task(
                            send_chatwoot_reply, acc_id_int, conv_id_int, reply
                        )
                except Exception:
                    logger.exception("AI yesterday reply error")
        if is_start_command(content) and message_type == "incoming":
            acc_id_int = to_int(account_id)
            conv_id_int = to_int(conversation_id)
            if acc_id_int is not None and conv_id_int is not None:
                background_tasks.add_task(
                    send_chatwoot_reply, acc_id_int, conv_id_int, WELCOME_TEXT
                )
            try:
                chatroom_id_raw = extract_chatroom_id(body)
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

@router.get("/health")
async def health():
    base_url = chatwoot_base_url()
    token = chatwoot_token()
    db_ok = False
    try:
        with psycopg.connect(pg_dsn()) as conn:
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

@router.post("/webhooks/telegram")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    token = telegram_token()
    msg = body.get("message") or {}
    cb = body.get("callback_query") or {}
    if msg:
        text = msg.get("text") or ""
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        if is_start_command(text):
            background_tasks.add_task(send_telegram_country_keyboard, chat.get("id"))
        choice = normalize_country(text)
        if choice:
            background_tasks.add_task(set_user_country, body, text)
        if is_ai_pick_command(text) and chat_id is not None:
            try:
                hint = {"data": {"message": {"additional_attributes": {"chat_id": chat_id}}}}
                reply = ai_pick_reply(hint)
                if isinstance(reply, list):
                    for seg in reply:
                        background_tasks.add_task(send_telegram_message, chat_id, seg)
                else:
                    background_tasks.add_task(send_telegram_message, chat_id, reply)
            except Exception:
                logger.exception("Telegram AI pick reply error")
        if is_ai_history_command(text) and chat_id is not None:
            try:
                hint = {"data": {"message": {"additional_attributes": {"chat_id": chat_id}}}}
                reply = ai_history_reply(hint)
                background_tasks.add_task(send_telegram_message, chat_id, reply)
            except Exception:
                logger.exception("Telegram AI history reply error")
        if is_ai_yesterday_command(text) and chat_id is not None:
            try:
                hint = {"data": {"message": {"additional_attributes": {"chat_id": chat_id}}}}
                reply = ai_yesterday_reply(hint)
                background_tasks.add_task(send_telegram_message, chat_id, reply)
            except Exception:
                logger.exception("Telegram AI yesterday reply error")
    if cb:
        data = cb.get("data") or ""
        choice = normalize_country(data)
        if choice:
            background_tasks.add_task(set_user_country, body, data)
            from .services import answer_callback_query
            background_tasks.add_task(answer_callback_query, token, cb.get("id"), "已记录选择")
    return {"status": "ok"}
