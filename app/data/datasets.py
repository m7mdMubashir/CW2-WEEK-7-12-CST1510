import pandas as pd
from pathlib import Path
import sqlite3

from .db import connect_database
from .schema import create_datasets_metadata_table

DATA_DIR = Path("DATA")

def insert_dataset(
    conn: sqlite3.Connection,
    dataset_name: str,
    category: str,
    source: str,
    last_updated: str,
    record_count: int,
    file_size_mb: float
):
    """Insert a new dataset row and return its id."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO datasets_metadata
        (dataset_name, category, source, last_updated, record_count, file_size_mb)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (dataset_name, category, source, last_updated, record_count, file_size_mb))
    conn.commit()
    return cursor.lastrowid

def get_all_datasets(conn: sqlite3.Connection = None):
    """Return all datasets as a DataFrame."""
    close_after = False
    if conn is None:
        conn = connect_database()
        close_after = True

    df = pd.read_sql_query(
        "SELECT * FROM datasets_metadata ORDER BY id DESC",
        conn
    )

    if close_after:
        conn.close()

    return df


def delete_dataset(conn: sqlite3.Connection, dataset_id: int):
    """Delete dataset by id. Returns number of deleted rows."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM datasets_metadata WHERE id = ?", (dataset_id,))
    conn.commit()
    return cursor.rowcount

def load_datasets_metadata_csv(conn, csv_filename="datasets_metadata_1000.csv", force: bool = False):
    """
    Load datasets metadata CSV into datasets_metadata table.
    CSV columns (Week 8): id, name, source, category, size
    Mapped to Week 9 columns:
    dataset_name, category, source, last_updated, record_count, file_size_mb
    """

    create_datasets_metadata_table(conn)

    csv_path = DATA_DIR / csv_filename
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return 0

    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.lower()

    df = df.rename(columns={
        "name": "dataset_name",
        "size": "file_size_mb"
    })

    df["last_updated"] = "2024-01-01"        # placeholder
    df["record_count"] = 0                  # placeholder

    final_cols = [
        "dataset_name",
        "category",
        "source",
        "last_updated",
        "record_count",
        "file_size_mb"
    ]

    df = df[final_cols]

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(1) FROM datasets_metadata")
    existing = cursor.fetchone()[0]
    if existing > 0:
        if not force:
            print(f"datasets_metadata already has {existing} rows; skipping CSV load (use force=True to overwrite)")
            return 0
        else:
            cursor.execute("DELETE FROM datasets_metadata")
            conn.commit()
            print("Existing datasets_metadata rows deleted (force=True).")

    df.to_sql("datasets_metadata", conn, if_exists="append", index=False)

    print(f"Loaded {len(df)} dataset rows!")
    return len(df)