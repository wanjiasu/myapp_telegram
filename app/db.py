import os
import psycopg
import logging

logger = logging.getLogger(__name__)

def pg_dsn() -> str:
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", user or "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

def init_db() -> None:
    try:
        with psycopg.connect(pg_dsn()) as conn:
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
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_threads (
                        id BIGSERIAL PRIMARY KEY,
                        platform TEXT NOT NULL,
                        chatroom_id TEXT NOT NULL,
                        agent_thread_id TEXT NOT NULL,
                        subject TEXT,
                        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_activity_at TIMESTAMPTZ,
                        expires_at TIMESTAMPTZ,
                        status TEXT NOT NULL DEFAULT 'active',
                        metadata JSONB
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uniq_agent_thread_id ON agent_threads(agent_thread_id)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_agent_threads_active ON agent_threads(platform, chatroom_id, status)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_agent_threads_expires ON agent_threads(expires_at)
                    """
                )
                conn.commit()
    except Exception:
        logger.exception("DB init error")
