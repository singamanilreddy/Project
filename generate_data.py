import csv
import random
from datetime import datetime, timedelta

num_records = 30000

valid_roles = ["admin", "editor", "viewer"]
invalid_roles = ["invalid_role", "guest", "unknown", None]

# -------------------------
# Generate users.csv
# -------------------------
with open("users.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "user_id", "username", "email",
        "signup_date", "last_login_date", "status"
    ])

    for i in range(1, num_records + 1):

        signup_date = datetime.now() - timedelta(days=random.randint(1, 365))
        last_login_date = datetime.now() - timedelta(days=random.randint(1, 400))

        rand = random.random()

        # Introduce invalid data scenarios
        if rand < 0.8:
            username = f"user_{i}"
            email = f"user_{i}@test.com"
            status = "active"
        elif rand < 0.9:
            username = ""   # missing username
            email = f"user_{i}@test.com"
            status = "inactive"
        elif rand < 0.97:
            username = f"user_{i}"
            email = "invalid_email"   # bad format
            status = "active"
        else:
            username = None
            email = None
            status = "unknown"   # invalid status

        writer.writerow([
            i,
            username,
            email,
            signup_date.strftime("%Y-%m-%d"),
            last_login_date.strftime("%Y-%m-%d"),
            status
        ])


# -------------------------
# Generate roles.csv
# -------------------------
with open("roles.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "role_id", "user_id", "role_name", "assigned_date"
    ])

    for i in range(1, num_records + 1):

        assigned_date = datetime.now() - timedelta(days=random.randint(1, 365))
        rand = random.random()

        # Introduce invalid role scenarios
        if rand < 0.8:
            role = random.choice(valid_roles)
        elif rand < 0.95:
            role = random.choice(invalid_roles)
        else:
            role = ""   # empty role

        writer.writerow([
            i,
            i,
            role,
            assigned_date.strftime("%Y-%m-%d")
        ])