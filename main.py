from app.data.db import connect_database
from app.data.schema import create_all_tables
from app.services.user_service import register_user, login_user, migrate_users_from_file
from app.data.incidents import insert_incident, get_all_incidents, get_incident_by_id, update_incident, delete_incident, load_cyber_incidents_csv 
from app.data.datasets import get_all_datasets, load_datasets_metadata_csv
from app.data.tickets import get_all_tickets, load_it_tickets_csv

def main():
    print("---Starting System Setup ---")
    
    conn = connect_database()
    create_all_tables(conn)
    print("✅ Database tables created (if not existing).")
    
    try:
        migrated = migrate_users_from_file()
        print(f"✅ Migrated {migrated} users from DATA/users.txt")
    except Exception as e:
        print(f"User migration skipped or failed: {e}")
    
    from app.data.users import get_user_by_username
    if not get_user_by_username("alice"):
        success, msg = register_user("alice", "SecurePass123!", "analyst")
        print(f"User 'alice' created: {msg}")
    else:
        print("User 'alice' already exists.")

    conn = connect_database()
    try:
        incidents_present = len(get_all_incidents(conn))
        if incidents_present == 0:
            loaded_rows = load_cyber_incidents_csv(conn, force=True)
            print(f"✅ Loaded {loaded_rows} cyber incidents from CSV")
        else:
            print(f"ℹcyber_incidents already has {incidents_present} rows; skipping CSV load")
    except Exception as e:
        print(f"Error loading Cyber CSV: {e}")

    try:
        incident_id = insert_incident(
            conn,
            "Test Phishing Incident",   # title
            "High",                     # severity
            "open",                     # status
            "2024-11-05"                # date
        )
        print(f"✅ CRUD Test: Created incident #{incident_id}")
        
        update_incident(conn, incident_id, status="Closed")
        after_update = get_incident_by_id(conn, incident_id)
        if after_update and after_update[3] == "Closed": # Assuming index 3 is status
            print("CRUD Test: Update successful (Status is Closed)")
        else:
            print(f"CRUD Test: Update check result: {after_update}")

        delete_incident(conn, incident_id)
        after_delete = get_incident_by_id(conn, incident_id)
        if after_delete is None:
            print("CRUD Test: Delete successful")
        else:
            print("CRUD Test: Delete failed")
            
    except Exception as e:
        print(f"CRUD Test Error: {e}")

    datasets_present = len(get_all_datasets())
    if datasets_present == 0:
        loaded_datasets = load_datasets_metadata_csv(conn, force=True)
        print(f"Loaded {loaded_datasets} datasets from CSV")
    else:
        print(f"datasets_metadata already has {datasets_present} rows.")

    try:
        tickets_present = len(get_all_tickets(conn))
        if tickets_present == 0:
            loaded_tickets = load_it_tickets_csv(conn, force=True)
            print(f"Loaded {loaded_tickets} IT tickets from CSV")
        else:
            print(f"it_tickets already has {tickets_present} rows.")
    except Exception as e:
        print(f"Error loading IT Tickets: {e}")

    print("---  Data Summary ---")
    incidents = get_all_incidents(conn)
    print(f"Total Cyber Incidents: {len(incidents)}")

    datasets = get_all_datasets()
    print(f"Total Datasets: {len(datasets)}")

    tickets = get_all_tickets(conn)
    print(f"Total IT Tickets: {len(tickets)}")
    
    conn.close()
    print("--- Setup Complete ---")

if __name__ == "__main__":
    main()