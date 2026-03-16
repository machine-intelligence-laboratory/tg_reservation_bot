import sqlite3
from contextlib import contextmanager
from typing import Iterable, List, Tuple, Optional, Dict, Any


@contextmanager
def get_connection(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db(db_path: str) -> None:
    with get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_conversations_chat_id
            ON conversations (chat_id)
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (conversation_id) REFERENCES conversations(id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL,
                date_text TEXT,
                time_text TEXT,
                guests_count_text TEXT,
                name_text TEXT,
                phone_text TEXT,
                floor_text TEXT,
                certificate_needed_text TEXT,
                structured_request_json TEXT,
                completed INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (conversation_id) REFERENCES conversations(id)
            )
            """
        )
        for col in ("name_text", "phone_text"):
            try:
                cur.execute(f"ALTER TABLE bookings ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass
        conn.commit()


def get_or_create_conversation(db_path: str, chat_id: int) -> int:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM conversations WHERE chat_id = ? ORDER BY id DESC LIMIT 1",
            (chat_id,),
        )
        row = cur.fetchone()
        if row:
            return int(row["id"])

        cur.execute(
            "INSERT INTO conversations (chat_id) VALUES (?)",
            (chat_id,),
        )
        conn.commit()
        return int(cur.lastrowid)


def add_message(
    db_path: str, conversation_id: int, role: str, content: str
) -> None:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO messages (conversation_id, role, content)
            VALUES (?, ?, ?)
            """,
            (conversation_id, role, content),
        )
        cur.execute(
            "UPDATE conversations SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (conversation_id,),
        )
        conn.commit()


def get_recent_messages(
    db_path: str, conversation_id: int, limit: int = 30
) -> List[Dict[str, Any]]:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT role, content
            FROM messages
            WHERE conversation_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (conversation_id, limit),
        )
        rows = cur.fetchall()
        messages = [
            {"role": row["role"], "content": row["content"]} for row in rows
        ]
        messages.reverse()
        return messages


def get_or_create_booking(db_path: str, conversation_id: int) -> int:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM bookings
            WHERE conversation_id = ? AND completed = 0
            ORDER BY id DESC
            LIMIT 1
            """,
            (conversation_id,),
        )
        row = cur.fetchone()
        if row:
            return int(row["id"])

        cur.execute(
            "INSERT INTO bookings (conversation_id) VALUES (?)",
            (conversation_id,),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_booking(db_path: str, booking_id: int) -> Optional[sqlite3.Row]:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM bookings WHERE id = ?",
            (booking_id,),
        )
        return cur.fetchone()


def update_booking_field(
    db_path: str, booking_id: int, field: str, value: str
) -> None:
    allowed = {
        "date_text", "time_text", "guests_count_text",
        "name_text", "phone_text",
        "floor_text", "certificate_needed_text",
    }
    if field not in allowed:
        raise ValueError(f"Unknown booking field: {field}")

    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE bookings
            SET {field} = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (value, booking_id),
        )
        conn.commit()


def reset_booking(db_path: str, conversation_id: int) -> None:
    """Завершает текущую незавершённую бронь — следующий запрос создаст новую."""
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bookings
            SET completed = 1, updated_at = CURRENT_TIMESTAMP
            WHERE conversation_id = ? AND completed = 0
            """,
            (conversation_id,),
        )
        conn.commit()


def mark_booking_completed(
    db_path: str, booking_id: int, structured_request_json: str
) -> None:
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bookings
            SET completed = 1,
                structured_request_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (structured_request_json, booking_id),
        )
        conn.commit()


def get_last_completed_booking(db_path: str, chat_id: int):
    """Последнее завершённое бронирование по chat_id (user id)."""
    with get_connection(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM conversations WHERE chat_id = ? ORDER BY id DESC LIMIT 1",
            (chat_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        conversation_id = int(row["id"])
        cur.execute(
            """
            SELECT * FROM bookings
            WHERE conversation_id = ? AND completed = 1
            ORDER BY id DESC LIMIT 1
            """,
            (conversation_id,),
        )
        return cur.fetchone()


def copy_booking_fields(db_path: str, target_booking_id: int, source_row) -> None:
    """Копирует поля брони из source в target (для «изменить бронирование»)."""
    if not source_row:
        return
    source = dict(source_row)
    fields = (
        "date_text", "time_text", "guests_count_text",
        "name_text", "phone_text", "floor_text", "certificate_needed_text",
    )
    for f in fields:
        v = source.get(f)
        if v is not None and str(v).strip():
            update_booking_field(db_path, target_booking_id, f, str(v).strip())

