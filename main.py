from fastapi import FastAPI
from sqlalchemy import text
from backend.load_circulation_from_loans import CSV_2024, CSV_LOANS_2022, CSV_LOANS_2024, load_loans
from db import engine


app = FastAPI()

@app.get("/libraries")
def list_libraries():
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT library_id, name, address, city FROM library ORDER BY name")
        )
        rows = [dict(r._mapping) for r in result]
    return {"libraries": rows}

def main():
    load_loans(CSV_LOANS_2022, 2022)
    load_loans(CSV_LOANS_2024, 2024)

if __name__ == "__main__":
    main()
