import pandas as pd
from pathlib import Path
import sqlite3

from .db import connect_database
from .schema import create_cyber_incidents_table


DATA_DIR = Path("DATA")  # folder where CSVs live
 

def insert_incident(conn: sqlite3.Connection, title, severity, status="open", date=None):
    """Insert a new incident and return its new id."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO cyber_incidents (title, severity, status, date)
        VALUES (?, ?, ?, ?)
        """,
        (title, severity, status, date)
    )
    conn.commit()
    return cursor.lastrowid


def get_incident_by_id(conn: sqlite3.Connection, incident_id: int):
    """Fetch one incident by id."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM cyber_incidents WHERE id = ?",
        (incident_id,)
    )
    return cursor.fetchone()


def get_all_incidents(conn: sqlite3.Connection):
    """Fetch all incidents."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cyber_incidents")
    return cursor.fetchall()


def update_incident(conn, incident_id, title=None, severity=None, status=None, date=None):
    cursor = conn.cursor()

    current = get_incident_by_id(conn, incident_id)
    if not current:
        return False

    new_title = title if title is not None else current[1]
    new_severity = severity if severity is not None else current[2]
    new_status = status if status is not None else current[3]
    new_date = date if date is not None else current[4]

    prev_status = current[3] if len(current) > 3 else None
    prev_resolved = current[5] if len(current) > 5 else None
    resolved_date = prev_resolved
    if prev_status != "Closed" and new_status == "Closed":
        from datetime import datetime
        resolved_date = datetime.utcnow().strftime("%Y-%m-%d")

    cursor.execute("""
        UPDATE cyber_incidents
        SET title = ?, severity = ?, status = ?, date = ?, resolved_date = ?
        WHERE id = ?
    """, (new_title, new_severity, new_status, new_date, resolved_date, incident_id))

    conn.commit()
    return True


def delete_incident(conn: sqlite3.Connection, incident_id: int):
    """Delete an incident by id."""
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM cyber_incidents WHERE id = ?",
        (incident_id,)
    )
    conn.commit()
    return cursor.rowcount




def load_cyber_incidents_csv(conn: sqlite3.Connection, csv_filename="cyber_incidents_1000.csv", force: bool = False):
    """
    Load cyber incidents from CSV into cyber_incidents table.
    CSV columns expected: id,title,severity,status,date
    """
    # ensure table exists
    create_cyber_incidents_table(conn)

    csv_path = DATA_DIR / csv_filename

    if not csv_path.exists():
        print(f" CSV not found: {csv_path}")
        return 0

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip()  # clean headers

    expected_cols = ["id", "title", "severity", "status", "date"]
    df = df[expected_cols]

    df = df.drop(columns=["id"])

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(1) FROM cyber_incidents")
    existing = cursor.fetchone()[0]
    if existing > 0:
        if not force:
            print(f"cyber_incidents already has {existing} rows; skipping CSV load (use force=True to overwrite)")
            return 0
        else:
            cursor.execute("DELETE FROM cyber_incidents")
            conn.commit()
            print("Existing cyber_incidents rows deleted (force=True).")

    df.to_sql("cyber_incidents", conn, if_exists="append", index=False)

    print(f"Loaded {len(df)} rows into cyber_incidents")
    return len(df)
