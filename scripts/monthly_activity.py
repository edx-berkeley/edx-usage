import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

df = pd.read_csv(BASE_DIR / "output" / "user_activity.csv")

courses = {
    "88.1": "88.1 last activity",
    "88.2": "88.2 last activity",
    "88.3": "88.3 last activity",
}

# For each course, extract month-year and count users
monthly = {}

for course, col in courses.items():
    # Drop users with no activity in this course
    active = df[df[col] != "none"][col]
    # Parse to month-year (e.g. "2025-09")
    month_year = pd.to_datetime(active).dt.to_period("M")
    counts = month_year.value_counts().sort_index()
    monthly[course] = counts

summary = pd.DataFrame(monthly).fillna(0).astype(int).reset_index()
summary.columns = ["month-year", "88.1 users", "88.2 users", "88.3 users"]
summary["month-year"] = summary["month-year"].astype(str)

output_path = BASE_DIR / "output" / "monthly_activity.csv"
output_path.parent.mkdir(exist_ok=True)
summary.to_csv(output_path, index=False)
print(f"Saved {len(summary)} rows to {output_path}")
print(summary.to_string(index=False))
