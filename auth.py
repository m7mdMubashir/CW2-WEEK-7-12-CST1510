import bcrypt
import os

USER_DATA_FILE = "users.txt"


def hash_password(plain_text_password):
    """
    Hashes a password using bcrypt with automatic salt generation.
    """
    password_bytes = plain_text_password.encode('utf-8')
    
    salt = bcrypt.gensalt()
    
    hashed_bytes = bcrypt.hashpw(password_bytes, salt)
    
    return hashed_bytes.decode('utf-8')

def verify_password(plain_text_password, hashed_password):
    """
    Verifies a plaintext password against a stored bcrypt hash.
    """
    password_bytes = plain_text_password.encode('utf-8')
    hashed_bytes = hashed_password.encode('utf-8')
    
    # Verify using bcrypt
    return bcrypt.checkpw(password_bytes, hashed_bytes)

def user_exists(username):
    """Checks if username exists in the file."""
    if not os.path.exists(USER_DATA_FILE):
        return False
    
    with open(USER_DATA_FILE, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            if len(parts) >= 1 and parts[0] == username:
                return True
    return False

def register_user(username, password):
    """Registers a new user."""
    if user_exists(username):
        print(f"Error: Username '{username}' already exists.")
        return False

    hashed_pw = hash_password(password)
    
    with open(USER_DATA_FILE, 'a') as file:
        file.write(f"{username},{hashed_pw}\n")
        
    print(f"Success: User '{username}' registered successfully!")
    return True

def login_user(username, password):
    """Authenticates a user."""
    if not os.path.exists(USER_DATA_FILE):
        print("Error: No users registered yet.")
        return False
    with open(USER_DATA_FILE, 'r') as file:
        for line in file:
            try:
                stored_user, stored_hash = line.strip().split(',')
                
                if stored_user == username:
                    if verify_password(password, stored_hash):
                        return True
                    else:
                        break
            except ValueError:
                continue 

    return False


def validate_username(username):
    """Basic validation for username."""
    if len(username) < 3:
        return False, "Username too short."
    return True, ""

def validate_password(password):
    """Basic validation for password."""
    if len(password) < 6:
        return False, "Password must be at least 6 characters."
    return True, ""

def display_menu():
    """Displays the main menu[cite: 120]."""
    print("\n" + "="*50)
    print(" MULTI-DOMAIN INTELLIGENCE PLATFORM")
    print(" Secure Authentication System")
    print("="*50)
    print(" [1] Register a new user")
    print(" [2] Login")
    print(" [3] Exit")
    print("-"*50)

def main():
    """Main program loop[cite: 130]."""
    print("\nWelcome to the Week 7 Authentication System!")
    
    while True:
        display_menu()
        choice = input("\nPlease select an option (1-3): ").strip()
        
        if choice == '1':
            print("\n--- USER REGISTRATION ---")
            username = input("Enter a username: ").strip()
            
            is_valid, err = validate_username(username)
            if not is_valid:
                print(f"Error: {err}")
                continue

            password = input("Enter a password: ").strip()
            is_valid, err = validate_password(password)
            if not is_valid:
                print(f"Error: {err}")
                continue
                
            confirm = input("Confirm password: ").strip()
            if password != confirm:
                print("Error: Passwords do not match.")
                continue
                
            register_user(username, password)

        elif choice == '2':
            print("\n--- USER LOGIN ---")
            username = input("Enter your username: ").strip()
            password = input("Enter your password: ").strip()
            
            if login_user(username, password):
                print(f"\nSuccess: Welcome, {username}!")
                print("You are now logged in.")
                input("Press Enter to return to menu...")
            else:
                print("\nError: Invalid username or password.")

        elif choice == '3':
            print("\nThank you for using the authentication system.")
            print("Exiting...")
            break
        else:
            print("\nError: Invalid option.")

if __name__ == "__main__":
    main()