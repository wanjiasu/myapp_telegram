import logging
import psycopg
import asyncio
from datetime import datetime, timedelta, timezone
from .db import pg_dsn
from .config import read_offset, telegram_token
from .ai import ai_yesterday_text_for_country, ai_pick_text_for_country
from .services import send_telegram_message, send_telegram_message_with_url_button

logger = logging.getLogger(__name__)

def _list_users_for_push():
    try:
        with psycopg.connect(pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (chatroom_id) id, chatroom_id, country
                    FROM users
                    WHERE chatroom_id IS NOT NULL AND country IS NOT NULL
                    ORDER BY chatroom_id, updated_at DESC, id DESC
                    """
                )
                return cur.fetchall() or []
    except Exception:
        logger.exception("List users for push error")
        return []

def _has_pushed(user_id: int, push_date: datetime, push_type: str) -> bool:
    try:
        with psycopg.connect(pg_dsn()) as conn:
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
        with psycopg.connect(pg_dsn()) as conn:
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

def _claim_push(user_id: int, push_date: datetime, push_type: str) -> bool:
    try:
        with psycopg.connect(pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO push_log (user_id, push_date, push_type)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, push_date, push_type) DO NOTHING
                    RETURNING id
                    """,
                    (int(user_id), push_date.date(), push_type),
                )
                row = cur.fetchone()
                if row:
                    conn.commit()
                    return True
                return False
    except Exception:
        logger.exception("Claim push error")
        return False

def _push_yesterday(user_row) -> None:
    try:
        user_id, chatroom_id, country = user_row
        text = ai_yesterday_text_for_country(country)
        if text:
            send_telegram_message(chatroom_id, text)
    except Exception:
        logger.exception("Push yesterday error")

def _push_pick(user_row) -> None:
    try:
        user_id, chatroom_id, country = user_row
        text = ai_pick_text_for_country(country)
        if text:
            if isinstance(text, list):
                for seg in text:
                    if seg:
                        send_telegram_message_with_url_button(chatroom_id, seg)
            else:
                send_telegram_message_with_url_button(chatroom_id, text)
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
                    offset = read_offset(country) if country else 0
                    local_now = now_utc + timedelta(hours=offset)
                    if local_now.hour == 11 and local_now.minute == 0:
                        if _claim_push(user_id, local_now, "yesterday"):
                            _push_yesterday(row)
                    if local_now.hour == 20 and local_now.minute == 0:
                        if _claim_push(user_id, local_now, "pick"):
                            _push_pick(row)
                except Exception:
                    logger.exception("Daily push per-user error")
        except Exception:
            logger.exception("Daily push scheduler error")
        await asyncio.sleep(60)
