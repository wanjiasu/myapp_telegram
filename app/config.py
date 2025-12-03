import os
import json

def chatwoot_base_url() -> str:
    url = os.getenv("CHATWOOT_BASE_URL", "")
    return url.rstrip("/")

def chatwoot_token() -> str:
    return os.getenv("CHATWOOT_API_ACCESS_TOKEN", "")

def telegram_token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "")

def telegram_webhook_url() -> str:
    return os.getenv("TELEGRAM_WEBHOOK_URL", "")

def lark_webhook_url() -> str:
    return os.getenv("LARK_BOT_WEBHOOK_URL", "")

def read_offset(country: str) -> int:
    try:
        base = os.path.dirname(os.path.dirname(__file__))
        path = os.path.join(base, "时差.json")
        with open(path, "r", encoding="utf-8") as f:
            m = json.load(f)
        v = m.get(country)
        return int(v) if v is not None else 0
    except Exception:
        return 0

def allowed_account_inbox_pairs():
    try:
        s = os.getenv("accounts_id_list", "") or os.getenv("ACCOUNTS_ID_LIST", "")
        pairs = set()
        if s and s.strip():
            try:
                data = json.loads(s)
            except Exception:
                data = None
        else:
            data = None
        if data is None:
            try:
                base = os.path.dirname(os.path.dirname(__file__))
                path = os.path.join(base, ".env")
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                import re
                m = re.search(r"accounts_id_list\s*=\s*\[(.*?)\]", content, re.DOTALL)
                if m:
                    block = "[" + m.group(1) + "]"
                    data = json.loads(block)
            except Exception:
                data = None
        if isinstance(data, list):
            for item in data:
                try:
                    a = int(item.get("accounts_id"))
                    i = int(item.get("inbox_id"))
                    pairs.add((a, i))
                except Exception:
                    pass
        return pairs
    except Exception:
        return set()

def agent_url() -> str:
    try:
        s = os.getenv("agent_url", "") or os.getenv("AGENT_URL", "")
        s = s.strip()
        if s:
            return s.rstrip("/")
        base = os.path.dirname(os.path.dirname(__file__))
        path = os.path.join(base, ".env")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        import re
        m = re.search(r"agent_url\s*=\s*([^\s]+)", content)
        if m:
            return str(m.group(1)).strip().rstrip("/")
    except Exception:
        pass
    return ""

def agent_name() -> str:
    try:
        s = os.getenv("agent", "") or os.getenv("AGENT", "")
        if s and s.strip():
            return s.strip()
        base = os.path.dirname(os.path.dirname(__file__))
        path = os.path.join(base, ".env")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        import re
        m = re.search(r"agent\s*=\s*([\w\-]+)", content)
        if m:
            return str(m.group(1)).strip()
    except Exception:
        pass
    return ""

def agent_endpoint_path() -> str:
    try:
        p = os.getenv("agent_endpoint", "") or os.getenv("AGENT_ENDPOINT", "")
        if p and p.strip():
            t = p.strip()
            if not t.startswith("/"):
                t = "/" + t
            return t
        name = agent_name()
        if name:
            return f"/{name}/messages"
        return "/messages"
    except Exception:
        return "/messages"

def thread_ttl_minutes_telegram() -> int:
    try:
        v = os.getenv("THREAD_TTL_MINUTES_TELEGRAM", "")
        if v and str(v).strip():
            return int(str(v).strip())
    except Exception:
        pass
    return 30

def thread_ttl_minutes_chatwoot() -> int:
    try:
        v = os.getenv("THREAD_TTL_MINUTES_CHATWOOT", "")
        if v and str(v).strip():
            return int(str(v).strip())
    except Exception:
        pass
    return 720

def thread_max_age_days() -> int:
    try:
        v = os.getenv("THREAD_MAX_AGE_DAYS", "")
        if v and str(v).strip():
            return int(str(v).strip())
    except Exception:
        pass
    return 7
