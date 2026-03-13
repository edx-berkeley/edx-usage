import os
import sys
import csv
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

BASE_DIR = Path(__file__).parent.parent

from edx import get_users

TOKEN = os.environ.get("JUPYTERHUB_TOKEN")
if not TOKEN:
    raise EnvironmentError("JUPYTERHUB_TOKEN environment variable is not set")

# ---------------------------------------------------------------------------
# 1. Build a lookup: Course Specific Anonymized User ID -> (User ID, course)
# ---------------------------------------------------------------------------
courses = {
    "88.1": BASE_DIR / "88.1ex" / "ids.csv",
    "88.2": BASE_DIR / "88.2ex" / "ids.csv",
    "88.3": BASE_DIR / "88.3ex" / "ids.csv",
}

course_specific_to_user = {}   # course_specific_anon_id -> (user_id, course)

for course_name, path in courses.items():
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = row["User ID"]
            course_specific_id = row["Course Specific Anonymized User ID"]
            course_specific_to_user[course_specific_id] = (uid, course_name)

print(f"Loaded {len(course_specific_to_user)} course-specific ID mappings")

# ---------------------------------------------------------------------------
# 2. Pull JupyterHub users
# ---------------------------------------------------------------------------
print("Fetching JupyterHub users...")
hub_users = get_users("edx", TOKEN)
print(f"Fetched {len(hub_users)} JupyterHub users")

# ---------------------------------------------------------------------------
# 3. Match each JupyterHub user to a real User ID + course
# ---------------------------------------------------------------------------
records = []
unmatched = 0

for user in hub_users:
    name = user.get("name")
    last_activity = user.get("last_activity")
    if name in course_specific_to_user:
        user_id, course = course_specific_to_user[name]
        records.append({
            "User ID": user_id,
            "course": course,
            "last_activity": last_activity,
        })
    else:
        unmatched += 1

print(f"Matched {len(records)} users, {unmatched} unmatched")

# ---------------------------------------------------------------------------
# 4. Pivot to one row per User ID with last activity per course
# ---------------------------------------------------------------------------
df = pd.DataFrame(records)

pivot = df.pivot_table(
    index="User ID",
    columns="course",
    values="last_activity",
    aggfunc="max",   # take the most recent activity if duplicates exist
).reset_index()

# Strip time, keep date only
for col in pivot.columns:
    if col != "User ID":
        pivot[col] = pivot[col].apply(lambda x: x[:10] if isinstance(x, str) else x)

pivot.columns.name = None
pivot = pivot.rename(columns={
    "88.1": "88.1 last activity",
    "88.2": "88.2 last activity",
    "88.3": "88.3 last activity",
})

# Fill missing courses with "none"
for col in ["88.1 last activity", "88.2 last activity", "88.3 last activity"]:
    if col not in pivot.columns:
        pivot[col] = "none"
    else:
        pivot[col] = pivot[col].fillna("none")

# Reorder columns
pivot = pivot[["User ID", "88.1 last activity", "88.2 last activity", "88.3 last activity"]]

output_path = BASE_DIR / "output" / "user_activity.csv"
output_path.parent.mkdir(exist_ok=True)
pivot.to_csv(output_path, index=False)
print(f"Saved {len(pivot)} rows to {output_path}")
print(pivot.head())
