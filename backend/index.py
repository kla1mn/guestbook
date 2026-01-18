import os
import json
import time
import uuid
import base64
import ydb
import ydb.iam


# ===== Config =====

BACKEND_VERSION = os.getenv("BACKEND_VERSION", "dev")
REPLICA_NAME = os.getenv("REPLICA_NAME", "unknown")

YDB_ENDPOINT = os.getenv("YDB_ENDPOINT")
YDB_DATABASE = os.getenv("YDB_DATABASE")


# ===== YDB driver =====

driver = ydb.Driver(
    endpoint=YDB_ENDPOINT,
    database=YDB_DATABASE,
    credentials=ydb.iam.MetadataUrlCredentials(),
)
driver.wait(fail_fast=True, timeout=5)

pool = ydb.QuerySessionPool(driver)
_table_ready = False


# ===== Helpers =====

def _cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Content-Type": "application/json; charset=utf-8",
    }


def _resp(status, body):
    return {
        "statusCode": status,
        "headers": _cors_headers(),
        "body": json.dumps(body, ensure_ascii=False),
    }


def _parse_event(event):
    if isinstance(event, str):
        event = json.loads(event)

    return (
        event.get("httpMethod", "GET"),
        event.get("path", "/"),
        event.get("body"),
        bool(event.get("isBase64Encoded")),
    )


# ===== Schema =====

def _ensure_table():
    global _table_ready
    if _table_ready:
        return

    ddl = """
    CREATE TABLE `messages` (
        created_at Uint64,
        id Utf8,
        name Utf8,
        text Utf8,
        PRIMARY KEY (created_at, id)
    );
    """

    try:
        pool.execute_with_retries(ddl)
    except Exception:
        pass

    _table_ready = True


# ===== Queries =====

def _list_messages(limit=50):
    _ensure_table()

    q = f"""
    SELECT created_at, id, name, text
    FROM messages
    ORDER BY created_at DESC
    LIMIT {limit};
    """

    result_sets = pool.execute_with_retries(q)
    rows = []

    for r in result_sets[0].rows:
        rows.append({
            "created_at": int(r.created_at),
            "id": str(r.id),
            "name": str(r.name),
            "text": str(r.text),
        })

    return rows


def _add_message(name, text):
    _ensure_table()

    created_at = int(time.time() * 1000)
    mid = str(uuid.uuid4())

    q = """
    DECLARE $created_at AS Uint64;
    DECLARE $id AS Utf8;
    DECLARE $name AS Utf8;
    DECLARE $text AS Utf8;

    UPSERT INTO messages (created_at, id, name, text)
    VALUES ($created_at, $id, $name, $text);
    """

    pool.execute_with_retries(
        q,
        {
            "$created_at": ydb.TypedValue(created_at, ydb.PrimitiveType.Uint64),
            "$id": mid,
            "$name": name,
            "$text": text,
        },
    )

    return {
        "created_at": created_at,
        "id": mid,
        "name": name,
        "text": text,
    }


# ===== Handler =====

def handler(event, context):
    method, path, body, is_b64 = _parse_event(event)

    if method == "OPTIONS":
        return _resp(200, {})

    if path.endswith("/api/version"):
        return _resp(200, {
            "backend_version": BACKEND_VERSION,
            "replica": REPLICA_NAME,
        })

    if path.endswith("/api/messages") and method == "GET":
        return _resp(200, {
            "backend_version": BACKEND_VERSION,
            "replica": REPLICA_NAME,
            "messages": _list_messages(),
        })

    if path.endswith("/api/messages") and method == "POST":
        if not body:
            return _resp(400, {"error": "empty body"})

        if is_b64:
            body = base64.b64decode(body).decode()

        try:
            data = json.loads(body)
        except Exception:
            return _resp(400, {"error": "invalid json"})

        name = (data.get("name") or "anonymous")[:50]
        text = (data.get("text") or "").strip()[:1000]

        if not text:
            return _resp(400, {"error": "text required"})

        msg = _add_message(name, text)

        return _resp(200, {
            "backend_version": BACKEND_VERSION,
            "replica": REPLICA_NAME,
            "saved": msg,
        })

    return _resp(404, {"error": "not found"})
