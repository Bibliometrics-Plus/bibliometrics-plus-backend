from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
TOR_DIR = BASE_DIR / "data" / "toronto"

print("Toronto folder path:", TOR_DIR)
print("Exists?", TOR_DIR.exists())

print("\nCSV files found:")
for f in TOR_DIR.glob("*.csv"):
    print("-", f.name)