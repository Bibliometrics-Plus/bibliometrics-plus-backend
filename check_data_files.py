from pathlib import Path

# This file lives in ...\bibliometrics-plus\backend
BASE_DIR = Path(__file__).resolve().parent

# Force it to use backend/data (where your screenshot shows the files)
DATA_DIR = BASE_DIR / "data"

print("BASE_DIR:", BASE_DIR)
print("DATA_DIR:", DATA_DIR)

files = [
    "Ottawa_Public_Library_Locations_2024.csv",
    "Library_Loans_Physical_Material_Circulation_2022.csv",
    "Ottawa_Public_Library_1_year_active_cardholders_by_Neighbourhood_(ONS)_2024.csv",
    "Ottawa_Public_Library_Most_Requested_Titles_2024.csv",
]

for name in files:
    path = DATA_DIR / name
    print(f"{name} -> {path} | exists? {path.exists()}")

print("\nFiles that Python sees in DATA_DIR:")
for p in DATA_DIR.glob("*"):
    print("-", p.name)
