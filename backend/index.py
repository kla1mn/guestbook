import os
import json
import time
import uuid
import base64
import ydb
import ydb.iam
from typing import Dict, Any, Optional, Tuple, List


class Config:
    BACKEND_VERSION = os.getenv("BACKEND_VERSION", "unknown_backend_version")
    REPLICA_NAME = os.getenv("REPLICA_NAME", "unknown_REPLICA_NAME")
    YDB_ENDPOINT = os.getenv("YDB_ENDPOINT")
    YDB_DATABASE = os.getenv("YDB_DATABASE")


class Message:
    def __init__(self, created_at: int, id: str, name: str, text: str):
        self.created_at = created_at
        self.id = id
        self.name = name
        self.text = text

    def to_dict(self) -> Dict[str, Any]:
        return {
            "created_at": self.created_at,
            "id": self.id,
            "name": self.name,
            "text": self.text,
        }


class YDBClient:
    def __init__(self):
        self.driver = None
        self.pool = None
        self._table_ready = False

    def connect(self) -> None:
        self.driver = ydb.Driver(
            endpoint=Config.YDB_ENDPOINT,
            database=Config.YDB_DATABASE,
            credentials=ydb.iam.MetadataUrlCredentials(),
        )
        self.driver.wait(fail_fast=True, timeout=5)
        self.pool = ydb.QuerySessionPool(self.driver)

    def ensure_table(self) -> None:
        if self._table_ready:
            return

        ddl = """
              CREATE TABLE `messages` \
              ( \
                  created_at Uint64, \
                  id Utf8, \
                  name Utf8, \
                  text Utf8, \
                  PRIMARY KEY (created_at, id)
              ); \
              """

        try:
            self.pool.execute_with_retries(ddl)
        except Exception:
            pass

        self._table_ready = True

    def list_messages(self, limit: int = 20) -> List[Message]:
        self.ensure_table()

        query = f"""
        SELECT created_at, id, name, text
        FROM messages
        ORDER BY created_at DESC
        LIMIT {limit};
        """

        result_sets = self.pool.execute_with_retries(query)
        messages = []

        for row in result_sets[0].rows:
            messages.append(Message(
                created_at=int(row.created_at),
                id=str(row.id),
                name=str(row.name),
                text=str(row.text),
            ))

        return messages

    def add_message(self, name: str, text: str) -> Message:
        self.ensure_table()

        created_at = int(time.time() * 1000)
        message_id = str(uuid.uuid4())

        query = """
        DECLARE $created_at AS Uint64;
        DECLARE $id AS Utf8;
        DECLARE $name AS Utf8;
        DECLARE $text AS Utf8;

        UPSERT INTO messages (created_at, id, name, text)
        VALUES ($created_at, $id, $name, $text);
        """

        self.pool.execute_with_retries(
            query,
            {
                "$created_at": ydb.TypedValue(created_at, ydb.PrimitiveType.Uint64),
                "$id": message_id,
                "$name": name,
                "$text": text,
            },
        )

        return Message(
            created_at=created_at,
            id=message_id,
            name=name,
            text=text,
        )


class ResponseBuilder:
    @staticmethod
    def _cors_headers() -> Dict[str, str]:
        return {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Content-Type": "application/json; charset=utf-8",
        }

    @staticmethod
    def build(status: int, body: Any) -> Dict[str, Any]:
        return {
            "statusCode": status,
            "headers": ResponseBuilder._cors_headers(),
            "body": json.dumps(body, ensure_ascii=False),
        }


class RequestParser:
    @staticmethod
    def parse(event: Any) -> Tuple[str, str, Optional[str], bool]:
        if isinstance(event, str):
            event = json.loads(event)

        return (
            event.get("httpMethod", "GET"),
            event.get("path", "/"),
            event.get("body"),
            bool(event.get("isBase64Encoded")),
        )

    @staticmethod
    def parse_body(body: Optional[str], is_b64: bool) -> Optional[Dict[str, Any]]:
        if not body:
            return None

        try:
            if is_b64:
                body = base64.b64decode(body).decode()
            return json.loads(body)
        except Exception:
            return None


class MessageValidator:
    @staticmethod
    def validate(data: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str], str]:
        if not data:
            return False, None, None, "invalid json"

        name = (data.get("name") or "anonymous")[:50]
        text = (data.get("text") or "").strip()[:1000]

        if not text:
            return False, None, None, "text required"

        return True, name, text, ""


class MessageHandler:

    def __init__(self, ydb_client: YDBClient):
        self.ydb_client = ydb_client

    def handle_version(self) -> Dict[str, Any]:
        return {
            "backend_version": Config.BACKEND_VERSION,
            "replica": Config.REPLICA_NAME,
        }

    def handle_list_messages(self) -> Dict[str, Any]:
        messages = self.ydb_client.list_messages()
        result = {
            "backend_version": Config.BACKEND_VERSION,
            "replica": Config.REPLICA_NAME,
            "messages": [msg.to_dict() for msg in messages],
        }
        return result

    def handle_add_message(self, body: Optional[str], is_b64: bool) -> Dict[str, Any]:
        data = RequestParser.parse_body(body, is_b64)
        is_valid, name, text, error = MessageValidator.validate(data)

        if not is_valid:
            return {"error": error}

        message = self.ydb_client.add_message(name, text)

        result = {
            "backend_version": Config.BACKEND_VERSION,
            "replica": Config.REPLICA_NAME,
            "saved": message.to_dict(),
        }
        return result


ydb_client = YDBClient()

try:
    ydb_client.connect()
except Exception as e:
    print(f"Failed to connect to YDB: {e}")

message_handler = MessageHandler(ydb_client)


def handler(event, context):
    method, path, body, is_b64 = RequestParser.parse(event)

    if method == "OPTIONS":
        return ResponseBuilder.build(200, {})

    if path.endswith("/api/version"):
        response_data = message_handler.handle_version()
        return ResponseBuilder.build(200, response_data)

    elif path.endswith("/api/messages"):
        if method == "GET":
            response_data = message_handler.handle_list_messages()
            return ResponseBuilder.build(200, response_data)

        elif method == "POST":
            response_data = message_handler.handle_add_message(body, is_b64)
            if "error" in response_data:
                return ResponseBuilder.build(400, response_data)
            return ResponseBuilder.build(200, response_data)

    return ResponseBuilder.build(404, {"error": "not found"})
