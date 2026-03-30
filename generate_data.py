import csv
import random
from datetime import datetime, timedelta

num_records = 10000

valid_roles = ["admin", "editor", "viewer"]

# Generate users.csv
with open("users.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["user_id", "username", "email", "signup_date", "status"])

    for i in range(1, num_records + 1):
        signup_date = datetime.now() - timedelta(days=random.randint(1, 365))
        status = "active" if random.random() > 0.2 else "inactive"

        writer.writerow([
            i,
            f"user_{i}",
            f"user_{i}@test.com",
            signup_date.strftime("%Y-%m-%d"),
            status
        ])

# Generate roles.csv
with open("roles.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["role_id", "user_id", "role_name", "assigned_date"])

    for i in range(1, num_records + 1):
        assigned_date = datetime.now() - timedelta(days=random.randint(1, 365))

        # Add some invalid roles (10%)
        role = random.choice(valid_roles) if random.random() > 0.1 else "invalid_role"

        writer.writerow([
            i,
            i,
            role,
            assigned_date.strftime("%Y-%m-%d")
        ])