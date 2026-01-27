import logging
import re
import psycopg
from datetime import datetime, timedelta, timezone
from .db import pg_dsn
from .config import read_offset
from .utils import format_tags, extract_chatroom_id

logger = logging.getLogger(__name__)

def _fmt_odd(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s or s in ("未找到赔率",):
            return None
        try:
            v = float(s)
            return f"{v:.2f}"
        except Exception:
            m = re.search(r"-?\d+(?:\.\d+)?", s)
            if not m:
                return None
            v = float(m.group(0))
            return f"{v:.2f}"
    except Exception:
        return None

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

def get_user_profile_for_chat(body: dict):
    b = body or {}
    data = b.get("data") or b.get("payload") or b
    chatroom_id = extract_chatroom_id(body)
    external_id = (
        (data.get("sender") or {}).get("id")
        or data.get("sender_id")
        or (data.get("contact") or {}).get("id")
    )
    country = None
    initial_cash = None
    initial_date = None
    with psycopg.connect(pg_dsn()) as conn:
        with conn.cursor() as cur:
            if chatroom_id is not None:
                cur.execute(
                    "SELECT country, initial_cash, initial_date FROM users WHERE chatroom_id = %s LIMIT 1",
                    (str(chatroom_id),),
                )
                row = cur.fetchone()
                if row:
                    country, initial_cash, initial_date = row[0], row[1], row[2]
            if (country is None or initial_cash is None or initial_date is None) and external_id is not None:
                cur.execute(
                    "SELECT country, initial_cash, initial_date FROM users WHERE external_id = %s LIMIT 1",
                    (str(external_id),),
                )
                row = cur.fetchone()
                if row:
                    country = country or row[0]
                    if initial_cash is None:
                        initial_cash = row[1]
                    if initial_date is None:
                        initial_date = row[2]
    return country, initial_cash, initial_date

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

def help_reply(body: dict) -> str:
    now_utc = datetime.now(timezone.utc)
    bj = now_utc + timedelta(hours=8)
    if 8 <= bj.hour < 20:
        return "Connecting you to human support, please wait."
    return "Human support is available from 08:00 to 20:00 Beijing time."

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
                      AND e.confidence>0.6
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
        return "No AI history available, please try again later."
    overall = calc_accuracy(rows)
    acc_7d = calc_accuracy(rows, start=last7_start, end=last7_end)
    acc_yesterday = calc_accuracy(rows, start=yesterday_start, end=yesterday_end)
    emojis = []
    for r in rows[:10]:
        emojis.append("✅" if is_prediction_success(r.get("predict_winner"), r.get("result")) else "❌")
    emoji_line = "".join(emojis) if emojis else "No records"
    return (
        f"📊 AI Overall Accuracy: {overall:.1f}%\n\n"
        f"🗓️ AI 7-Day Accuracy: {acc_7d:.1f}%\n\n"
        f"🌙 AI Yesterday Accuracy: {acc_yesterday:.1f}%\n\n"
        f"🎯 Recent 10 Predictions:\n{emoji_line}"
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
                    ORDER BY e.confidence DESC, f.fixture_date ASC
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
        return "No AI records for yesterday, please try again later."
    lines = []
    for i, r in enumerate(rows, 1):
        ok = bool(r.get("success"))
        emoji = "✅" if ok else "❌"
        lines.append(f"{i}. {r.get('home_name')} vs {r.get('away_name')} {emoji}")
    body_text = "\n".join(lines)
    return f"📊 AI Yesterday Accuracy: {acc:.1f}%\n\n{body_text}"

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
                    ORDER BY e.confidence DESC, f.fixture_date ASC
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
        return "No AI records for yesterday, please try again later."
    lines = []
    for i, r in enumerate(rows, 1):
        emoji = "✅" if bool(r.get("success")) else "❌"
        lines.append(f"{i}. {r.get('home_name')} vs {r.get('away_name')} {emoji}")
    body_text = "\n".join(lines)
    return f"📊 AI Yesterday Accuracy: {acc:.1f}%\n\n{body_text}"

def query_daily_profit_reply(body: dict) -> str:
    country, initial_cash, initial_date = get_user_profile_for_chat(body)
    if initial_cash is None or initial_date is None:
        return "请先通过 /set 设置初始资金和开始日期"
    offset = read_offset(country) if country else 0
    rows = []
    try:
        with psycopg.connect(pg_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH RECURSIVE
                    fixtures AS (
                        SELECT (fixture_date + (%s || ' hour')::interval)::date AS day,
                               t1.fixture_id, home_odd, draw_odd, away_odd,
                               predict_winner, result, confidence
                        FROM (
                            SELECT fixture_id, predict_winner, confidence,
                                   home_odd, draw_odd, away_odd, result
                            FROM ai_eval
                            WHERE if_bet=1
                              AND home_odd IS NOT NULL
                              AND home_odd <> '未找到赔率'
                              AND result IS NOT NULL
                        ) t1
                        INNER JOIN (
                            SELECT fixture_id, fixture_date
                            FROM api_football_fixtures
                        ) t2 ON t1.fixture_id=t2.fixture_id
                    ),
                    ranked AS (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY day ORDER BY confidence DESC) AS rn
                        FROM fixtures
                    ),
                    daily_top AS (
                        SELECT day, fixture_id, predict_winner, result,
                               CASE predict_winner
                                    WHEN 3 THEN home_odd::numeric
                                    WHEN 1 THEN draw_odd::numeric
                                    WHEN 0 THEN away_odd::numeric
                               END::numeric AS win_odd
                        FROM ranked
                        WHERE rn <= 5
                    ),
                    days AS (
                        SELECT DISTINCT day FROM daily_top WHERE day >= %s ORDER BY day
                    ),
                    rec AS (
                        SELECT (SELECT MIN(day) FROM days) AS day,
                               CAST(%s AS numeric(12,2)) AS capital
                        UNION ALL
                        SELECT d.day,
                               CAST(
                                   ROUND(
                                       (
                                           ((5 - (SELECT COUNT(*) FROM daily_top t WHERE t.day = rec.day)) * (rec.capital / 5))
                                           +
                                           (
                                               SELECT SUM(
                                                   (rec.capital / 5) *
                                                   CASE WHEN t.predict_winner = t.result
                                                        THEN t.win_odd ELSE 0 END
                                               )
                                               FROM daily_top t
                                               WHERE t.day = rec.day
                                           )
                                       ), 2
                                   ) AS numeric(12,2)
                               ) AS capital
                        FROM rec
                        JOIN days d ON d.day > rec.day
                        WHERE d.day = (SELECT MIN(day) FROM days WHERE day > rec.day)
                    )
                    SELECT day, capital
                    FROM rec
                    WHERE day < (CURRENT_DATE - INTERVAL '1 day')
                    ORDER BY day
                    """,
                    (int(offset), initial_date, initial_cash),
                )
                rows = cur.fetchall() or []
    except Exception:
        logger.exception("DB query daily profit error")
    if not rows:
        return "暂无每日盈亏数据"
    lines = []
    for r in rows:
        day = r[0]
        capital = r[1]
        lines.append(f"{day}：{capital}")
    return "📈 每日盈亏（模拟）\n" + "\n".join(lines)

def ai_pick_reply(body: dict) -> str:
    country = get_country_for_chat(body)
    offset = read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local = now_utc + timedelta(hours=offset)
    local_day = datetime(local.year, local.month, local.day, tzinfo=timezone.utc)
    tomorrow_local_day = local_day + timedelta(days=1)
    start_utc = now_utc
    end_utc = tomorrow_local_day - timedelta(hours=offset) + timedelta(days=1)
    logger.info(f"ai_pick_reply country={country} offset={offset} start_utc={start_utc} end_utc={end_utc}")
    rows = []
    with psycopg.connect(pg_dsn()) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT e.fixture_id, e.predict_winner, e.confidence, e.key_tag_evidence,
                           f.fixture_date, f.home_name, f.away_name, e.home_odd, e.away_odd, e.draw_odd
                    FROM ai_eval e
                    INNER JOIN api_football_fixtures f ON f.fixture_id = e.fixture_id
                    WHERE COALESCE(e.if_bet, 0) = 1
                      AND e.confidence > 0.6
                      AND f.fixture_date >= %s AND f.fixture_date < %s
                    ORDER BY e.confidence DESC, f.fixture_date ASC
                    """,
                    (start_utc, end_utc),
                )
                rows = cur.fetchall() or []
            except psycopg.errors.UndefinedColumn:
                logger.warning("ai_pick_reply odds columns missing, fallback without odds")
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.execute(
                    """
                    SELECT e.fixture_id, e.predict_winner, e.confidence, e.key_tag_evidence,
                           f.fixture_date, f.home_name, f.away_name
                    FROM ai_eval e
                    INNER JOIN api_football_fixtures f ON f.fixture_id = e.fixture_id
                    WHERE COALESCE(e.if_bet, 0) = 1
                      AND e.confidence > 0.6
                      AND f.fixture_date >= %s AND f.fixture_date < %s
                    ORDER BY e.confidence DESC, f.fixture_date ASC
                    """,
                    (start_utc, end_utc),
                )
                rows = cur.fetchall() or []
    logger.info(f"ai_pick_reply fetched_rows={len(rows)}")
    if not rows:
        logger.warning(f"ai_pick_reply no rows for window start={start_utc} end={end_utc} offset={offset}")
        return "No AI picks available, please try again later."
    out = []
    for i, r in enumerate(rows, 1):
        fixture_id = r[0]
        predict_winner = r[1]
        confidence = r[2]
        key_tag_evidence = r[3]
        fixture_date = r[4]
        home_name = r[5]
        away_name = r[6]
        home_odd = draw_odd = away_odd = None
        if len(r) >= 10:
            home_odd = r[7]
            away_odd = r[8]
            draw_odd = r[9]
        when_local = fixture_date + timedelta(hours=offset) if fixture_date else None
        when_str = when_local.strftime("%Y-%m-%d %H:%M") if when_local else ""
        tags = format_tags(key_tag_evidence)
        pw = str(predict_winner).strip().lower() if predict_winner is not None else ""
        if pw in ("3", "home", "主胜", "h"):
            result_label = "Home Win"
        elif pw in ("1", "draw", "平局", "主平", "d"):
            result_label = "Draw"
        elif pw in ("0", "away", "客胜", "a"):
            result_label = "Away Win"
        else:
            result_label = str(predict_winner)
        try:
            confidence_pct = f"{round(float(confidence) * 100)}%"
        except Exception:
            confidence_pct = str(confidence)
        lines = [
            f"⚽️ Match {i}: {home_name} vs {away_name}",
            f"🕒 Kickoff: {when_str}",
            f"🏆 Predicted Result: {result_label}",
            f"🎯 Confidence: {confidence_pct}",
            f"💡 Key Points: {tags}",
        ]
        h = _fmt_odd(home_odd)
        d = _fmt_odd(draw_odd)
        a = _fmt_odd(away_odd)
        if h and d and a:
            lines.append(f"💰 Odds: Home Win {h} - Draw {d} - Away Win {a}")
        lines.append(f"🔗 More details: https://betaione.com/football/{fixture_id}")
        lines.append("🎟️ Place bet: https://stake.com/?c=1ZvG3ZP5")
        out.append("\n".join(lines))
    if not out:
        return "No AI picks available, please try again later."
    chunks = []
    i = 0
    n = len(out)
    while i < n:
        chunks.append("\n\n".join(out[i:i+8]))
        i += 8
    return chunks[0] if len(chunks) == 1 else chunks

def ai_pick_text_for_country(country: str) -> str:
    offset = read_offset(country) if country else 0
    now_utc = datetime.now(timezone.utc)
    local = now_utc + timedelta(hours=offset)
    local_day = datetime(local.year, local.month, local.day, tzinfo=timezone.utc)
    tomorrow_local_day = local_day + timedelta(days=1)
    start_utc = now_utc
    end_utc = tomorrow_local_day - timedelta(hours=offset) + timedelta(days=1)
    logger.info(f"ai_pick_text_for_country country={country} offset={offset} start_utc={start_utc} end_utc={end_utc}")
    rows = []
    with psycopg.connect(pg_dsn()) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    select e.fixture_id, e.predict_winner, e.confidence, e.key_tag_evidence,
                           f.fixture_date, f.home_name, f.away_name, e.home_odd, e.away_odd, e.draw_odd
                    from (
                        select fixture_id, predict_winner, confidence, key_tag_evidence, home_odd, away_odd, draw_odd
                        from ai_eval where if_bet = 1 and confidence > 0.6
                    ) e
                    inner join (
                        select fixture_id, fixture_date, home_name, away_name
                        from api_football_fixtures where fixture_date >= %s and fixture_date < %s
                    ) f on e.fixture_id = f.fixture_id
                    order by e.confidence desc, f.fixture_date asc
                    """,
                    (start_utc, end_utc),
                )
                rows = cur.fetchall() or []
            except psycopg.errors.UndefinedColumn:
                logger.warning("ai_pick_text_for_country odds columns missing, fallback without odds")
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.execute(
                    """
                    select e.fixture_id, e.predict_winner, e.confidence, e.key_tag_evidence,
                           f.fixture_date, f.home_name, f.away_name
                    from (
                        select fixture_id, predict_winner, confidence, key_tag_evidence
                        from ai_eval where if_bet = 1 and confidence > 0.6
                    ) e
                    inner join (
                        select fixture_id, fixture_date, home_name, away_name
                        from api_football_fixtures where fixture_date >= %s and fixture_date < %s
                    ) f on e.fixture_id = f.fixture_id
                    order by e.confidence desc, f.fixture_date asc
                    """,
                    (start_utc, end_utc),
                )
                rows = cur.fetchall() or []
    logger.info(f"ai_pick_text_for_country fetched_rows={len(rows)}")
    if not rows:
        logger.warning(f"ai_pick_text_for_country no rows for window start={start_utc} end={end_utc} offset={offset}")
        return "No AI picks available, please try again later."
    out = []
    for i, r in enumerate(rows, 1):
        fixture_id = r[0]
        predict_winner = r[1]
        confidence = r[2]
        key_tag_evidence = r[3]
        fixture_date = r[4]
        home_name = r[5]
        away_name = r[6]
        home_odd = draw_odd = away_odd = None
        if len(r) >= 10:
            home_odd, draw_odd, away_odd = r[7], r[8], r[9]
        when_local = fixture_date + timedelta(hours=offset) if fixture_date else None
        when_str = when_local.strftime("%Y-%m-%d %H:%M") if when_local else ""
        tags = format_tags(key_tag_evidence)
        pw = str(predict_winner).strip().lower() if predict_winner is not None else ""
        if pw in ("3", "home", "主胜", "h"):
            result_label = "Home Win"
        elif pw in ("1", "draw", "平局", "主平", "d"):
            result_label = "Draw"
        elif pw in ("0", "away", "客胜", "a"):
            result_label = "Away Win"
        else:
            result_label = str(predict_winner)
        try:
            confidence_pct = f"{round(float(confidence) * 100)}%"
        except Exception:
            confidence_pct = str(confidence)
        lines = [
            f"⚽️ Match {i}: {home_name} vs {away_name}",
            f"🕒 Kickoff: {when_str}",
            f"🏆 Predicted Result: {result_label}",
            f"🎯 Confidence: {confidence_pct}",
            f"💡 Key Points: {tags}",
        ]
        h = _fmt_odd(home_odd)
        d = _fmt_odd(draw_odd)
        a = _fmt_odd(away_odd)
        if h and d and a:
            lines.append(f"💰 Odds: Home Win {h} - Draw {d} - Away Win {a}")
        lines.append(f"🔗 More details: https://betaione.com/football/{fixture_id}")
        lines.append("🎟️ Place bet: https://stake.com/?c=1ZvG3ZP5")
        out.append("\n".join(lines))
    if not out:
        return "No AI picks available, please try again later."
    chunks = []
    i = 0
    n = len(out)
    while i < n:
        chunks.append("\n\n".join(out[i:i+8]))
        i += 8
    return chunks[0] if len(chunks) == 1 else chunks
