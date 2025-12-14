import bcrypt
from pathlib import Path

from ..data.users import get_user_by_username, insert_user
from ..data.schema import create_users_table
from ..data.db import connect_database


def register_user(username: str, password: str, role: str = "user"):
    """
    Register a new user with hashed password.
    Returns: (success: bool, message: str)
    """
    if not username or not password:
        return False, "Username and password are required."

    existing = get_user_by_username(username)
    if existing:
        return False, f"User '{username}' already exists."

    password_hash = bcrypt.hashpw(
        password.encode("utf-8"),
        bcrypt.gensalt()
    ).decode("utf-8")

    insert_user(username, password_hash, role)
    return True, f"User '{username}' registered successfully."


def login_user(username: str, password: str):
    """
    Validate login.
    Returns: (success: bool, message: str)
    """
    if not username or not password:
        return False, "Username and password are required."

    user = get_user_by_username(username)
    if not user:
        return False, "User not found."

    stored_hash = user[2]  # (id, username, password_hash, role)
    if bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8")):
        return True, "Login successful!"
    else:
        return False, "Incorrect password."


def migrate_users_from_file(filepath="DATA/users.txt"):
    """
    Migrate users from Week 7 users.txt into DB.

    Expected format per line:
      username,password_hash,role(optional)

    Returns: number of inserted users
    """
    path = Path(filepath)

    if not path.exists():
        print(f"{filepath} not found. Skipping migration.")
        return 0

    conn = connect_database()
    create_users_table(conn)

    migrated = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = [p.strip() for p in line.split(",")]
            username = parts[0]
            password_hash = parts[1]
            role = parts[2] if len(parts) > 2 else "user"

            try:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO users (username, password_hash, role)
                    VALUES (?, ?, ?)
                    """,
                    (username, password_hash, role)
                )
                migrated += 1
            except Exception:
                pass

    conn.commit()
    conn.close()

    print(f"Migrated {migrated} users from {filepath}")
    return migrated