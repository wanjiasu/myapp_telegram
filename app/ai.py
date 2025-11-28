import logging
import psycopg
from datetime import datetime, timedelta, timezone
from .db import pg_dsn
from .config import read_offset
from .utils import format_tags

logger = logging.getLogger(__name__)

def get_country_for_chat(body: dict) -> str:
    b = body or {}
    data = b.get("data") or b.get("payload") or b
    from .utils import extract_chatroom_id
    chatroom_id = extract_chatroom_id(body)
    external_id = (
        (data.get("sender") or {}).get("id")
        or data.get("sender_id")
        or (data.get("contact") or {}).get("id")
    )
    with psycopg.connect(pg_dsn()) as conn:
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

def is_prediction_success(predict_winner, result) -> bool:
    try:
        p = str(predict_winner).strip().lower()
        r = str(result).strip().lower()
        if not p or not r:
            return False
        return p == r
    except Exception:
        return False

def calc_accuracy(rows, start=None, end=None) -> float:
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
    success = sum(1 for r in filtered if is_prediction_success(r.get("predict_winner"), r.get("result")))
    return round((success / total) * 100, 1)

def ai_history_reply(body: dict) -> str:
    country = get_country_for_chat(body)
    offset = read_offset(country) if country else 0
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
        with psycopg.connect(pg_dsn()) as conn:
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
    overall = calc_accuracy(rows)
    acc_7d = calc_accuracy(rows, start=last7_start, end=last7_end)
    acc_yesterday = calc_accuracy(rows, start=yesterday_start, end=yesterday_end)
    emojis = []
    for r in rows[:10]:
        emojis.append("✅" if is_prediction_success(r.get("predict_winner"), r.get("result")) else "❌")
    emoji_line = "".join(emojis) if emojis else "暂无记录"
    return (
        f"📊 AI历史预测准确率: {overall:.1f}%\n\n"
        f"🗓️ AI7天内预测准确率: {acc_7d:.1f}%\n\n"
        f"🌙 AI昨日预测准确率: {acc_yesterday:.1f}%\n\n"
        f"🎯 AI最近10场预测:\n{emoji_line}"
    )

def ai_yesterday_reply(body: dict) -> str:
    country = get_country_for_chat(body)
    offset = read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local_now = now_utc + timedelta(hours=offset)
    local_today = datetime(local_now.year, local_now.month, local_now.day, tzinfo=timezone.utc)
    today_start_utc = local_today - timedelta(hours=offset)
    yesterday_start = today_start_utc - timedelta(days=1)
    yesterday_end = today_start_utc
    rows = []
    acc = 0.0
    logger.info(f"ai_yesterday_reply country={country} offset={offset} y_start={yesterday_start} y_end={yesterday_end}")
    try:
        with psycopg.connect(pg_dsn()) as conn:
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
                           CASE WHEN (e.predict_winner)::text ~ '^-?\\d+$' AND (e.result)::text ~ '^-?\\d+$' AND (e.predict_winner)::int = (e.result)::int THEN 1 ELSE 0 END AS success
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
                logger.info(f"ai_yesterday_reply fetched_rows={len(rows)}")
                cur.execute(
                    """
                    SELECT COALESCE(ROUND(
                               SUM(CASE WHEN (e.predict_winner)::text ~ '^-?\\d+$' AND (e.result)::text ~ '^-?\\d+$' AND (e.predict_winner)::int = (e.result)::int THEN 1 ELSE 0 END)::numeric
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
                logger.info(f"ai_yesterday_reply acc={acc}")
    except Exception:
        logger.exception("DB fetch ai_yesterday error")
    if not rows:
        logger.warning(f"ai_yesterday_reply no rows for window start={yesterday_start} end={yesterday_end} offset={offset}")
        return "昨天暂无AI记录，可以稍后再试哦～"
    lines = []
    for i, r in enumerate(rows, 1):
        ok = bool(r.get("success"))
        emoji = "✅" if ok else "❌"
        lines.append(f"{i}. {r.get('home_name')} vs {r.get('away_name')} {emoji}")
    body_text = "\n".join(lines)
    return f"📊 AI昨日预测准确率: {acc:.1f}%\n\n{body_text}"

def ai_yesterday_text_for_country(country: str) -> str:
    offset = read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local_now = now_utc + timedelta(hours=offset)
    local_today = datetime(local_now.year, local_now.month, local_now.day, tzinfo=timezone.utc)
    today_start_utc = local_today - timedelta(hours=offset)
    yesterday_start = today_start_utc - timedelta(days=1)
    yesterday_end = today_start_utc
    rows = []
    acc = 0.0
    logger.info(f"ai_yesterday_text_for_country country={country} offset={offset} y_start={yesterday_start} y_end={yesterday_end}")
    try:
        with psycopg.connect(pg_dsn()) as conn:
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
                           CASE WHEN (e.predict_winner)::text ~ '^-?\\d+$' AND (e.result)::text ~ '^-?\\d+$' AND (e.predict_winner)::int = (e.result)::int THEN 1 ELSE 0 END AS success
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
                    {"home_name": r[5], "away_name": r[6], "success": r[7]}
                    for r in fetched
                ]
                logger.info(f"ai_yesterday_text_for_country fetched_rows={len(rows)}")
                cur.execute(
                    """
                    SELECT COALESCE(ROUND(
                               SUM(CASE WHEN (e.predict_winner)::text ~ '^-?\\d+$' AND (e.result)::text ~ '^-?\\d+$' AND (e.predict_winner)::int = (e.result)::int THEN 1 ELSE 0 END)::numeric
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
                logger.info(f"ai_yesterday_text_for_country acc={acc}")
    except Exception:
        logger.exception("DB fetch ai_yesterday country error")
    if not rows:
        logger.warning(f"ai_yesterday_text_for_country no rows for window start={yesterday_start} end={yesterday_end} offset={offset}")
        return "昨天暂无AI记录，可以稍后再试哦～"
    lines = []
    for i, r in enumerate(rows, 1):
        emoji = "✅" if bool(r.get("success")) else "❌"
        lines.append(f"{i}. {r.get('home_name')} vs {r.get('away_name')} {emoji}")
    body_text = "\n".join(lines)
    return f"📊 AI昨日预测准确率: {acc:.1f}%\n\n{body_text}"

def ai_pick_reply(body: dict) -> str:
    country = get_country_for_chat(body)
    offset = read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local = now_utc + timedelta(hours=offset)
    local_day = datetime(local.year, local.month, local.day, tzinfo=timezone.utc)
    tomorrow_local_day = local_day + timedelta(days=1)
    start_utc = now_utc
    end_utc = tomorrow_local_day - timedelta(hours=offset) + timedelta(days=2)
    logger.info(f"ai_pick_reply country={country} offset={offset} start_utc={start_utc} end_utc={end_utc}")
    rows = []
    with psycopg.connect(pg_dsn()) as conn:
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
    logger.info(f"ai_pick_reply fetched_rows={len(rows)}")
    if not rows:
        logger.warning(f"ai_pick_reply no rows for window start={start_utc} end={end_utc} offset={offset}")
        return "明天暂无AI精选比赛，稍后再试试。"
    out = []
    for i, r in enumerate(rows, 1):
        fixture_id, predict_winner, confidence, key_tag_evidence, fixture_date, home_name, away_name = r
        when_local = fixture_date + timedelta(hours=offset) if fixture_date else None
        when_str = when_local.strftime("%Y-%m-%d %H:%M") if when_local else ""
        tags = format_tags(key_tag_evidence)
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

def ai_pick_text_for_country(country: str) -> str:
    offset = read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local = now_utc + timedelta(hours=offset)
    local_day = datetime(local.year, local.month, local.day, tzinfo=timezone.utc)
    tomorrow_local_day = local_day + timedelta(days=1)
    start_utc = now_utc
    end_utc = tomorrow_local_day - timedelta(hours=offset) + timedelta(days=2)
    logger.info(f"ai_pick_text_for_country country={country} offset={offset} start_utc={start_utc} end_utc={end_utc}")
    rows = []
    with psycopg.connect(pg_dsn()) as conn:
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
    logger.info(f"ai_pick_text_for_country fetched_rows={len(rows)}")
    if not rows:
        logger.warning(f"ai_pick_text_for_country no rows for window start={start_utc} end={end_utc} offset={offset}")
        return "暂无AI精选比赛，稍后再试试。"
    out = []
    for i, r in enumerate(rows, 1):
        fixture_id, predict_winner, confidence, key_tag_evidence, fixture_date, home_name, away_name = r
        when_local = fixture_date + timedelta(hours=offset) if fixture_date else None
        when_str = when_local.strftime("%Y-%m-%d %H:%M") if when_local else ""
        tags = format_tags(key_tag_evidence)
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
