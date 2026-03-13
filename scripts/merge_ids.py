import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

courses = {
    "id-1": BASE_DIR / "88.1ex" / "ids.csv",
    "id-2": BASE_DIR / "88.2ex" / "ids.csv",
    "id-3": BASE_DIR / "88.3ex" / "ids.csv",
}

merged = None

for col_name, path in courses.items():
    df = pd.read_csv(path, usecols=["User ID", "Anonymized User ID"])
    df = df.rename(columns={"Anonymized User ID": col_name})
    if merged is None:
        merged = df
    else:
        merged = merged.merge(df, on="User ID", how="outer")

output_path = BASE_DIR / "output" / "merged_ids.csv"
output_path.parent.mkdir(exist_ok=True)
merged = merged.fillna("none")
merged.to_csv(output_path, index=False)
print(f"Saved {len(merged)} rows to {output_path}")
