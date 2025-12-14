import sqlite3


def create_users_table(conn: sqlite3.Connection):
    """Create users table."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'user'
        );
    """)
    conn.commit()
    print("users table created successfully!")


def create_cyber_incidents_table(conn: sqlite3.Connection):
    """Create cyber_incidents table."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cyber_incidents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            severity TEXT NOT NULL,
            status TEXT DEFAULT 'open',
            date TEXT NOT NULL,
            resolved_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    print("cyber_incidents table created successfully!")

    cursor.execute("PRAGMA table_info(cyber_incidents)")
    existing_cols = [row[1] for row in cursor.fetchall()]
    if "resolved_date" not in existing_cols:
        try:
            cursor.execute("ALTER TABLE cyber_incidents ADD COLUMN resolved_date TEXT")
            conn.commit()
            print("Added 'resolved_date' column to cyber_incidents (migration)")
        except Exception:
            pass


def create_datasets_metadata_table(conn: sqlite3.Connection):
    """Create datasets_metadata table (new Week 9 structure)."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datasets_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_name TEXT NOT NULL,
            category TEXT NOT NULL,
            source TEXT NOT NULL,
            last_updated TEXT,
            record_count INTEGER,
            file_size_mb REAL
        )
    """)
    conn.commit()
    print("datasets_metadata table created successfully!")


def create_it_tickets_table(conn: sqlite3.Connection):
    """Create it_tickets table."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS it_tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            priority TEXT NOT NULL,
            status TEXT DEFAULT 'open',
            created_date TEXT NOT NULL,
            resolved_date TEXT,
            assigned_to TEXT
        );
    """)
    conn.commit()
    print("it_tickets table created successfully!")

    cursor.execute("PRAGMA table_info(it_tickets)")
    existing_cols = [row[1] for row in cursor.fetchall()]
    if "resolved_date" not in existing_cols:
        try:
            cursor.execute("ALTER TABLE it_tickets ADD COLUMN resolved_date TEXT")
            conn.commit()
            print("Added 'resolved_date' column to it_tickets (migration)")
        except Exception:
            pass
    cursor.execute("PRAGMA table_info(it_tickets)")
    existing_cols = [row[1] for row in cursor.fetchall()]
    if "assigned_to" not in existing_cols:
        try:
            cursor.execute("ALTER TABLE it_tickets ADD COLUMN assigned_to TEXT")
            conn.commit()
            print("Added 'assigned_to' column to it_tickets (migration)")
        except Exception:
            pass


def create_all_tables(conn: sqlite3.Connection):
    """Create all tables."""
    create_users_table(conn)
    create_cyber_incidents_table(conn)
    create_datasets_metadata_table(conn)
    create_it_tickets_table(conn)
    print("all tables created successfully!")