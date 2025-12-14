import pandas as pd
from pathlib import Path
import sqlite3

from .db import connect_database
from .schema import create_it_tickets_table

DATA_DIR = Path("DATA")


def insert_ticket(conn: sqlite3.Connection, title: str, priority: str,
                  status: str = "open", created_date: str = None, assigned_to: str = None):
    """
    Insert a new IT ticket and return its new id.
    Matches schema:
    it_tickets(id, title, priority, status, created_date, resolved_date?, assigned_to?)
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(it_tickets)")
    cols = [row[1] for row in cursor.fetchall()]
    if "assigned_to" in cols:
        cursor.execute(
            """
            INSERT INTO it_tickets (title, priority, status, created_date, assigned_to)
            VALUES (?, ?, ?, ?, ?)
            """,
            (title, priority, status, created_date, assigned_to)
        )
    else:
        cursor.execute(
            """
            INSERT INTO it_tickets (title, priority, status, created_date)
            VALUES (?, ?, ?, ?)
            """,
            (title, priority, status, created_date)
        )
    conn.commit()
    return cursor.lastrowid


def get_ticket_by_id(conn: sqlite3.Connection, ticket_id: int):
    """Fetch one ticket by id."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM it_tickets WHERE id = ?", (ticket_id,))
    return cursor.fetchone()


def get_all_tickets(conn: sqlite3.Connection):
    """Fetch all tickets."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM it_tickets ORDER BY id DESC")
    return cursor.fetchall()


def update_ticket(conn: sqlite3.Connection, ticket_id: int,
                  title=None, priority=None, status=None, created_date=None, assigned_to=None):
    """
    Update ticket fields if provided.
    If status transitions to 'closed', set `resolved_date` to now.
    """
    current = get_ticket_by_id(conn, ticket_id)
    if not current:
        return False

    current_status = current[3]
    current_resolved = None
    current_assigned = None
    if len(current) >= 6:
        current_resolved = current[5]
    if len(current) >= 7:
        current_assigned = current[6]

    new_title = title if title is not None else current[1]
    new_priority = priority if priority is not None else current[2]
    new_status = status if status is not None else current_status
    new_created_date = created_date if created_date is not None else current[4]
    new_assigned = assigned_to if assigned_to is not None else current_assigned

    new_resolved_date = current_resolved
    if new_status and new_status.lower() == "closed" and (current_status is None or current_status.lower() != "closed"):
        new_resolved_date = pd.Timestamp.now().isoformat()

    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(it_tickets)")
    cols = [row[1] for row in cursor.fetchall()]
    if "resolved_date" in cols and "assigned_to" in cols:
        cursor.execute(
            """
            UPDATE it_tickets
            SET title = ?, priority = ?, status = ?, created_date = ?, resolved_date = ?, assigned_to = ?
            WHERE id = ?
            """,
            (new_title, new_priority, new_status, new_created_date, new_resolved_date, new_assigned, ticket_id)
        )
    elif "resolved_date" in cols:
        cursor.execute(
            """
            UPDATE it_tickets
            SET title = ?, priority = ?, status = ?, created_date = ?, resolved_date = ?
            WHERE id = ?
            """,
            (new_title, new_priority, new_status, new_created_date, new_resolved_date, ticket_id)
        )
    else:
        cursor.execute(
            """
            UPDATE it_tickets
            SET title = ?, priority = ?, status = ?, created_date = ?
            WHERE id = ?
            """,
            (new_title, new_priority, new_status, new_created_date, ticket_id)
        )
    conn.commit()
    return True


def delete_ticket(conn: sqlite3.Connection, ticket_id: int):
    """Delete a ticket by id."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM it_tickets WHERE id = ?", (ticket_id,))
    conn.commit()
    return cursor.rowcount


def load_it_tickets_csv(conn: sqlite3.Connection, csv_filename="it_tickets_1000.csv", force: bool = False):
    """
    Load IT tickets from CSV into it_tickets table.
    CSV columns expected: id, title, priority, status, created_date

    If your CSV has 'id', we drop it so SQLite autoincrements.
    """
    create_it_tickets_table(conn)

    csv_path = DATA_DIR / csv_filename

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return 0

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()

    expected_cols = ["id", "title", "priority", "status", "created_date"]
    if "assigned_to" in df.columns:
        expected_cols.append("assigned_to")
    df = df[expected_cols]

    df = df.drop(columns=["id"])

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(1) FROM it_tickets")
    existing = cursor.fetchone()[0]
    if existing > 0:
        if not force:
            print(f"it_tickets already has {existing} rows; skipping CSV load (use force=True to overwrite)")
            return 0
        else:
            cursor.execute("DELETE FROM it_tickets")
            conn.commit()
            print("Existing it_tickets rows deleted (force=True).")

    df.to_sql("it_tickets", conn, if_exists="append", index=False)

    print(f"Loaded {len(df)} rows into it_tickets")
    return len(df)


if __name__ == "__main__":
    c = connect_database()
    load_it_tickets_csv(c)
    print(get_all_tickets(c)[:3])
    c.close()